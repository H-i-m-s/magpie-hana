// runtime/proxy.mjs — magpie-hana 托管 + 反代层
// ─────────────────────────────────────────────────────────────────────────────
// 为什么「反代进程同时是 magpie 的父进程」：
//   magpie web 的网页面对每个请求都要求 key（webGuard 不豁免回环），key 只在它
//   启动时打印一次。让本进程直接 spawn 它，就能：①自行解析 key；②自动带 key 转发；
//   ③magpie 落在宿主的 job object 里 —— 本进程一停，agent 树整体回收。
//
// 为什么不走宿主的 service 挂载：
//   magpie 前端用绝对路径调 API（fetch("/api/" + path)）。service 会挂在
//   /api/apps/<id>/routes/_runtime/<rid>/ 之下，绝对路径会绕过前缀打到宿主根
//   （ComfyUI 为此付出过 403 的代价）。这里改成卡片 iframe 直连本进程的随机端口：
//   跨源不影响，因为 key 与主题都由本进程在服务端处理。
//
// 不变量：
//   - 绝不修改上游任何文件
//   - 绝不读写用户的真实 magpie 配置目录（靠便携 data/ 达成）
//   - 绝不抢 3425
// ─────────────────────────────────────────────────────────────────────────────

import { createServer, request as httpRequest } from "node:http";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { userInfo } from "node:os";
import { connect } from "node:net";
import { randomBytes } from "node:crypto";
// 托管进程的环境快照。
//
// 为什么要这个：magpie 靠「用户目录 + 各家 agent 的配置文件」判断本机装了哪些 agent。
// 同一个 exe，在普通 shell 里能认出 9 个，在宿主给的环境里只认出一个（实测），
// 差别只能在环境。这里把相关变量摆出来（白名单，不吐整份 env，里面可能有凭据）。
function envSnapshot() {
  const keys = [
    "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA", "LOCALAPPDATA",
    "HOME", "XDG_CONFIG_HOME", "SystemRoot", "ComSpec",
  ];
  const out = {
    pid: process.pid,
    cwd: process.cwd(),
    user: (() => { try { return userInfo().username; } catch { return ""; } })(),
  };
  for (const k of keys) {
    const v = process.env[k];
    out[k] = v === undefined ? "(未设置)" : v;
  }
  const p = process.env.PATH || process.env.Path || "";
  out.PATH_dirs = p.split(";").filter(Boolean).length;
  out.PATH_head = p.slice(0, 200);
  out.ENV_count = Object.keys(process.env).length;
  return out;
}

const UPSTREAM_HOST = "127.0.0.1";
const ANSI = /\x1b\[[0-9;]*[A-Za-z]/g;
const HOP_BY_HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade",
]);

const DEFAULT_HIDDEN = ["library", "sessions"];

// 上游可能不发 content-type（或我们把它剥了）。宿主会给卡片加 nosniff，
// 而浏览器对「nosniff + 无正确 MIME」的样式表/脚本是直接拒用的——
// 表现就是卡片里完全没有 CSS。所以按扩展名兜底补上。
const MIME = {
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
  ".wasm": "application/wasm",
  ".txt": "text/plain; charset=utf-8",
  ".xml": "application/xml",
  ".html": "text/html; charset=utf-8",
};

function mimeOf(url) {
  const path = String(url || "").split("?")[0].toLowerCase();
  const i = path.lastIndexOf(".");
  if (i < 0) return null;
  return MIME[path.slice(i)] || null;
}

// ── 压缩：代理不自行压缩 ──────────
// v0.3.0 曾给文本资源上 br/gzip，结果卡片里所有 CSS/JS 全加载失败。
// 根因（已用对照实验证实）：宿主的受管服务转发用的是 Node 的 fetch()，
// 它自动解压 body，却原样保留 content-encoding / content-length 头；
// 转到浏览器就成了「明文 body + 声明是 br」，浏览器解压失败，资源整批挂掉。
// 实验（本地回环，原始 674B -> br 87B）：fetch() 拿到的 content-encoding="br"、
// content-length="87"，而 body 已是 698 字节明文。
// 结论：只要响应会经过宿主转发，代理就不能自行压缩。
// 上游 magpie 自己也不压，所以也没有「透传上游压缩」这条路。

const state = {
  listenPort: 0,
  upstreamPort: 0,
  recent: [],          // 最近请求（诊断：看卡片实际发的是什么路径）
  upstreamKey: "",
  upstreamFailures: 0,   // 连续多少次连不上上游（到了阈值就自愈重拉）
  exePath: "",          // 当前被托管的 exe（重拉时要用）
  exeCwd: "",
  respawnCount: 0,
  upstreamPortFixed: 0,  // 钉给 magpie 的 web 端口：自更新重起时会延用同一个
  webKey: "",            // 固定的网页 key：重起后也认得出同一只
  hidden: new Set(DEFAULT_HIDDEN),
  themeAttr: "",
  themeChoice: "auto",   // 主题设置：auto | 具体主题名（青夜/暖纸/…）
  themeCss: "",
  serverTheme: "",       // Hana 当前主题名（服务端从 preferences.json 读，作 auto 的兜底）
  themeApplied: "",      // 实际生效的主题（诊断用）
  view: "",              // 上次停在 magpie 的哪一页（agents/providers/usage/…）
  lastError: null,
  phase: "starting",   // starting | waiting-magpie | ready | error | stopping
  requests: 0,
  rewrites: 0,
  magpiePid: null,
  diag: [],              // 诊断：入站原始请求头 + 卡片内部上报的页面上下文
  cacheStats: { hit: 0, stale: 0, miss: 0, refresh: 0, cleared: 0 },
  cacheCount: 0,
};

// HANA_HOME/user/preferences.json 里的 appearance.theme，以及浅/深调色板。
// 为什么服务端要读：卡片 iframe 里拿主题名有两条路——读父窗口的 data-theme、
// 或宿主在 URL 上带 hana-theme 参数；而 auto 模式下的「浅色用哪套 / 深色用哪套」
// 只有宿主自己知道。进程按需读一次就够（Hana 换主题时会写这个文件）。
//
// 注意：Hana 的 auto 模式实际是把 auto 解析成一个具体主题（默认 light→warm-paper、
// dark→midnight），preferences 里可能存的是 "auto"。这里就按 Hana 的默认规则解析。
const HANA_AUTO_LIGHT = "warm-paper";
const HANA_AUTO_DARK = "midnight";

function readHanaTheme() {
  try {
    const dataDir = process.cwd();                    // …\app-data\magpie-hana
    const hanaHome = dirname(dirname(dataDir));       // …\.hanako
    const f = join(hanaHome, "user", "preferences.json");
    if (!existsSync(f)) return "";
    const j = JSON.parse(readFileSync(f, "utf8"));
    const t = j && j.appearance && typeof j.appearance.theme === "string" ? j.appearance.theme : "";
    return t;
  } catch {
    return "";
  }
}

// 给卡片脚本的「宿主当前主题」：解析掉 auto，并附上浅/深调色板名。
// 宿主在 URL 上给的是挂载当刻的值；用户随后在 Hana 里换主题时，这个端点
// 会读到新的 preferences，卡片脚本每 5 秒问一次就能跟上。
function hostThemeInfo() {
  const raw = readHanaTheme();
  // 系统的明暗这里拿不到（代理是独立进程），但 Hana 的 auto 实际按
  // 「浅色→warm-paper、深色→midnight」解析；卡片侧若能从 magpie 自己的
  // data-theme 判断出明暗，会用它去挑对应的那套。
  const theme = raw && raw !== "auto" ? raw : "";
  return {
    raw: raw || "",
    theme: theme || HANA_AUTO_DARK,
    light: HANA_AUTO_LIGHT,
    dark: HANA_AUTO_DARK,
  };
}

// HANA 的 server-info.json（拿到宿主的 HTTP 端口，用来取主题 CSS）。
function readHanaServerPort() {
  try {
    const dataDir = process.cwd();
    const hanaHome = dirname(dirname(dataDir));
    const f = join(hanaHome, "server-info.json");
    if (!existsSync(f)) return 0;
    const j = JSON.parse(readFileSync(f, "utf8"));
    return Number(j && j.port) || 0;
  } catch {
    return 0;
  }
}

// 取宿主当前主题的 CSS 变量表。
// 为什么由代理来取、而不让卡片里的脚本直接向宿主发请求：
// 服务挂载模式下，页面里的 /api/apps/theme.css 可能被当成 magpie 自己的路径
// （实测直接访问代理时就是 404，卡片里也不保证），由代理从回环向宿主取最稳。
function fetchThemeCss(name) {
  return new Promise((resolve) => {
    const port = readHanaServerPort();
    if (!port) return resolve({ ok: false, error: "读不到 HANA 的 server-info.json" });
    // 不带主题名时：绝不能省掉 ?theme= —— 实测宿主对无参请求返回的是
    // *默认*主题（暖纸 #F8F4ED），不是用户当前主题。这里自己解析。
    let themeName = String(name || "").trim();
    if (!themeName) {
      const raw = readHanaTheme();
      themeName = raw && raw !== "auto" ? raw : HANA_AUTO_DARK;
    } else if (themeName === "auto") {
      themeName = HANA_AUTO_DARK;
    }
    const q = `?theme=${encodeURIComponent(themeName)}`;
    let req;
    try {
      req = httpRequest(
        { host: "127.0.0.1", port, path: `/api/apps/theme.css${q}`, method: "GET", timeout: 6000 },
        (r) => {
          if (r.statusCode !== 200) { r.resume(); return resolve({ ok: false, error: `宿主返回 ${r.statusCode}` }); }
          const chunks = [];
          r.on("data", (c) => chunks.push(c));
          r.on("end", () => resolve({ ok: true, theme: themeName, css: Buffer.concat(chunks).toString("utf8") }));
        });
    } catch (e) {
      // httpRequest 可能在同步阶段就抛（端口/参数不合法等）。不接住就是一个
      // 未处理的 rejection，会把整个代理进程带走。
      return resolve({ ok: false, error: String((e && e.message) || e) });
    }
    req.on("timeout", () => { try { req.destroy(); } catch { /* 忽略 */ } resolve({ ok: false, error: "取主题超时" }); });
    req.on("error", (e) => resolve({ ok: false, error: String(e && e.message || e) }));
    req.end();
  });
}

const log = (m) => process.stderr.write(`[magpie-hana] ${m}\n`);

// ── 「停在哪一页」的持久化 ────────────────────────────────────────────────────
// 为什么这件事要落在代理上、而不是页面自己：
//   magpie 的当前页只活在地址栏的 ?view= 上（app.js 的 show() 改完页面就
//   history.replaceState 写回 URL），它自己不往任何存储里记。而卡片是一个
//   每次打开都新起的 iframe，地址栏那点东西随 iframe 一起没了。
//   代理进程活得更久、又有个稳定的数据目录（cwd = app-data/magpie-hana），
//   所以由它记：页面换页时上报，卡片壳下次打开时把它拼回 iframe 地址。
// 只有真正的「窗口」形态才有页签（mode=panel 是托盘面板），所以只认 window。
const VIEW_FILE = "view.json";
const VIEW_NAMES = ["agents", "providers", "gateway", "routing", "usage", "sessions", "library", "plugins", "settings"];

function validView(v) {
  return typeof v === "string" && VIEW_NAMES.includes(v) ? v : "";
}

function readView() {
  try {
    const f = join(process.cwd(), VIEW_FILE);
    if (!existsSync(f)) return "";
    const j = JSON.parse(readFileSync(f, "utf8"));
    return validView(j && j.view);
  } catch { return ""; }
}

function saveView(v) {
  const ok = validView(v);
  if (!ok) return;
  try {
    writeFileSync(join(process.cwd(), VIEW_FILE),
      JSON.stringify({ view: ok, updatedAt: new Date().toISOString() }, null, 2), "utf8");
  } catch (e) { log("保存 view.json 失败：" + (e && e.message ? e.message : String(e))); }
}

// ── 主题：把「Hana 的配色」注入 magpie，但不夺走它自己的开关 ───────────
//
// v0.3.x 的做法有两个错：
//   ① 用 !important 无条件压住 magpie 的全部颜色变量，还往它的 data-theme
//      属性上写值 —— 于是 magpie 设置页里的「外观（跟随系统/浅色/深色）」
//      成了摆设，按了没反应（15 秒后又被这里改回去）。
//   ② 取「宿主当前主题」时用了不带参数的 /api/apps/theme.css，而它返回的是
//      *默认*主题（暖纸 #F8F4ED），不是用户当前主题（青夜 #3B4A54）。
//      所以「跟随 Hana」从一开始就没跟对过。
//
// 现在的做法：
//   · 主题名与三套调色板（当前 / 浅色 / 深色）从卡片 iframe 的 URL 参数读。
//     宿主确实会带：hana-theme、hana-css、hana-palette-light-theme/-css、
//     hana-palette-dark-theme/-css。这比读 preferences.json 实时，也比读
//     父窗口计算样式可靠（桌面版未必同源）。
//   · 不再写 magpie 的 data-theme，改成**读**它：用户在 magpie 里选浅色/深色，
//     就切到 Hana 对应的浅色/深色主题。开关因此真正可用，配色仍来自 Hana。
//   · 颜色变量仍带 !important（否则压不过 magpie 自己的 :root[data-theme]），
//     但这是一层「皮肤」，不再改动 magpie 的任何行为属性。
//   · 宿主当前版本不会主动推 hana.theme.changed（在 bundle 里搜不到），所以
//     跟随靠轮询：页面每 5 秒问一次代理「宿主现在是什么主题」。
const THEME_CLIENT = `<script id="hana-theme-client">
(function(){
  var STYLE_ID = "hana-theme";

  // ── 宿主在卡片 iframe 的 URL 上给的参数 ──────────────────────────────
  var q = new URLSearchParams(location.search || "");
  var P = {
    theme: q.get("hana-theme") || "",
    appearance: q.get("hana-theme-appearance") || "",
    light: q.get("hana-palette-light-theme") || "",
    lightCss: q.get("hana-palette-light-css") || "",
    dark: q.get("hana-palette-dark-theme") || "",
    darkCss: q.get("hana-palette-dark-css") || "",
    css: q.get("hana-css") || ""
  };

  // 卡片挂载前缀（形如 /api/apps/<id>/routes/_runtime/<rid>/_surface/<token>）。
  // 直接访问代理时 pathname 就是 "/"，此时必须返回空串：
  // 否则 BASE + "/_hana/x" 会拼成 "//_hana/x"（双斜杠），代理认不出这个前缀。
  function mountBase(){
    var p = location.pathname || "/";
    if (p === "/") return "";
    return (p.charAt(p.length - 1) === "/") ? p.slice(0, -1) : p;
  }
  var BASE = mountBase();

  // 卡片壳（route 卡片把代理页放进 iframe）会主动打个招呼。收到之后就认定
  // 「主题选择的持久化要走壳子代转」：壳子与 App 路由同源，我们不同源，
  // 直接 POST 会被同源策略挡掉。壳子也会在每帧主动重复打招呼，避免我们
  // 加载慢一步而错过第一声。
  var hanaShell = false;
  window.addEventListener("message", function (e) {
    var d = e.data;
    if (d && d.type === "magpie-hana:shell") hanaShell = true;
  });

  // ── 变量表 ───────────────────────────────────────────────────────────
  // seedVars：服务端随页面注入的「首屏变量表」（就是宿主当前的配色）。
  // 它是**一张变量表**，不是「主题名 -> 变量表」的映射，所以单独放，
  // 只当首屏兵底用，不往 cache 里塞（早先塞错了，导致按名取时命中一张假表）。
  // cache：主题名 -> 变量表。按需从宿主 / 代理取，取到就留着。
  var cache = {};
  var seedVars = (window.__hanaVars && typeof window.__hanaVars === "object") ? window.__hanaVars : null;
  // seedName：首屏那张变量表到底是哪一套主题的（服务端解析出来时就知道）。
  // 早先只判断了 P.theme，而实测卡片 iframe 与直接访问代理时 P.theme 都可能是
  // 空串 —— 那句 !P.theme 于是对所有主题名前都成立，选任何主题都拿回首屏那张表。
  var seedName = (typeof window.__hanaSeedName === "string") ? window.__hanaSeedName : "";

  function parseVars(css){
    var out = {}, re = /(--[A-Za-z0-9_-]+)\\s*:\\s*([^;}]+)/g, m;
    while ((m = re.exec(css))) out[m[1]] = m[2].replace(/!important/gi, "").trim();
    return out;
  }
  function usable(v){ return !!(v && (v["--bg"] || v["--text"])); }

  // 某个主题名对应的「宿主给的」完整 CSS URL（同源可直接取，最准）
  function hostCssUrl(name){
    if (!name) return "";
    if (name === P.theme) return P.css;
    if (name === P.light) return P.lightCss;
    if (name === P.dark) return P.darkCss;
    return "";
  }

  // 取变量表：内存 -> 首屏种子（仅当前主题） -> 宿主 URL -> 代理端点
  function varsFor(name){
    if (!name) return Promise.resolve(null);
    if (cache[name]) return Promise.resolve(cache[name]);
    // 首屏：如果请求的正是宿主当前主题，而服务端也备好了种子，直接用。
    // （直接访问代理时没有 URL 参数，P.theme 为空，此时也会落到这里。）
    if (seedVars && usable(seedVars) && seedName && name === seedName) {
      cache[name] = seedVars;
      return Promise.resolve(seedVars);
    }
    var host = hostCssUrl(name);
    var url = host || (BASE + "/_hana/theme?theme=" + encodeURIComponent(name));
    return fetch(url, { credentials: "same-origin" })
      .then(function(r){ return r.ok ? r.text() : ""; })
      .then(function(text){
        var v = null;
        try { var j = JSON.parse(text); v = j && j.vars ? j.vars : null; } catch (e) { v = null; }
        if (!usable(v)) v = parseVars(text);
        if (usable(v)) { cache[name] = v; return v; }
        return null;
      })
      .catch(function(){ return null; });
  }

  // ── 配色构造：把 Hana 的变量名映射到 magpie 的变量名 ────────────────
  function luminance(c){
    if (!c) return null;
    c = String(c).trim();
    var r, g, b, m;
    if ((m = /^#([0-9a-f]{3})$/i.exec(c))) {
      var h = m[1];
      r = parseInt(h[0]+h[0],16); g = parseInt(h[1]+h[1],16); b = parseInt(h[2]+h[2],16);
    } else if ((m = /^#([0-9a-f]{6})/i.exec(c))) {
      r = parseInt(m[1].slice(0,2),16); g = parseInt(m[1].slice(2,4),16); b = parseInt(m[1].slice(4,6),16);
    } else if ((m = /^rgba?\\(\\s*([\\d.]+)[,\\s]+([\\d.]+)[,\\s]+([\\d.]+)/i.exec(c))) {
      r = +m[1]; g = +m[2]; b = +m[3];
    } else { return null; }
    return (0.2126*r + 0.7152*g + 0.0722*b) / 255;
  }

  function buildCss(v){
    function g(){
      for (var i = 0; i < arguments.length; i++) {
        var x = v[arguments[i]];
        if (x && x.indexOf("url(") !== 0) return x;
      }
      return "";
    }
    var bg = g("--bg") || "#1b1e24";
    var card = g("--bg-card", "--card") || "#232830";
    var fg = g("--text", "--fg") || "#e6e9ef";
    var fg2 = g("--text-light", "--fg-2") || fg;
    var muted = g("--text-muted", "--muted") || fg2;
    var accent = g("--accent") || "#a5b4fc";
    var green = g("--green") || "#6fd99b";
    var danger = g("--danger") || "#f28b82";
    var coral = g("--coral") || danger;
    var border = g("--border");
    var shadowColor = g("--shadow") || "rgba(0,0,0,.4)";
    var s = [];
    // 带 !important：要压过 magpie 自己的 :root / :root[data-theme]（它没带）。
    function set(n, val){ if (val) s.push(n + ":" + val + " !important"); }
    set("--hana-bg", bg); set("--hana-card", card); set("--hana-fg", fg);
    set("--hana-fg-2", fg2); set("--hana-muted", muted);
    set("--hana-accent", accent); set("--hana-green", green);
    set("--hana-danger", danger); set("--hana-coral", coral);
    set("--bg", "var(--hana-bg)");
    set("--card", "var(--hana-card)");
    set("--card-2", "color-mix(in srgb, var(--hana-fg) 4%, var(--hana-card))");
    set("--pill", "color-mix(in srgb, var(--hana-fg) 7%, var(--hana-card))");
    set("--pill-hover", "color-mix(in srgb, var(--hana-fg) 12%, var(--hana-card))");
    set("--line", border || "color-mix(in srgb, var(--hana-fg) 15%, transparent)");
    set("--line-2", "color-mix(in srgb, var(--hana-fg) 8%, transparent)");
    set("--fg", "var(--hana-fg)");
    set("--fg-2", "var(--hana-fg-2)");
    set("--muted", "var(--hana-muted)");
    set("--faint", "color-mix(in srgb, var(--hana-muted) 62%, transparent)");
    set("--accent", "var(--hana-accent)");
    set("--accent-soft", "color-mix(in srgb, var(--hana-accent) 15%, transparent)");
    set("--accent-fg", "var(--hana-bg)");
    set("--sel", "color-mix(in srgb, var(--hana-accent) 24%, transparent)");
    set("--green", "var(--hana-green)");
    set("--green-soft", "color-mix(in srgb, var(--hana-green) 16%, transparent)");
    set("--red", "var(--hana-danger)");
    set("--red-soft", "color-mix(in srgb, var(--hana-danger) 16%, transparent)");
    set("--amber", "var(--hana-coral)");
    set("--amber-soft", "color-mix(in srgb, var(--hana-coral) 16%, transparent)");
    set("--drift", "var(--hana-danger)");
    set("--drift-soft", "color-mix(in srgb, var(--hana-danger) 16%, transparent)");
    set("--pop-bg", "color-mix(in srgb, var(--hana-fg) 8%, var(--hana-card))");
    set("--seg-track", "color-mix(in srgb, var(--hana-fg) 8%, transparent)");
    set("--seg-thumb", "color-mix(in srgb, var(--hana-fg) 18%, transparent)");
    set("--ctl-fg", "var(--hana-fg-2)");
    set("--shadow", "0 16px 44px " + shadowColor + ", 0 2px 8px " + shadowColor);
    var lum = luminance(bg);
    var scheme = (lum !== null && lum > 0.55) ? "light" : "dark";
    return { css: "html:root{color-scheme:" + scheme + "}html:root{" + s.join(";") + "}", scheme: scheme };
  }

  function applyVars(v){
    if (!v) return;
    var built = buildCss(v);
    var el = document.getElementById(STYLE_ID);
    if (!el) {
      el = document.createElement("style");
      el.id = STYLE_ID;
      (document.head || document.documentElement).appendChild(el);
    }
    if (el.textContent !== built.css) el.textContent = built.css;
    // 告诉同页的其它注入脚本（外观下拉）：配色刚换过，该重新对一下标签。
    // 为什么不能只靠 DOM 观察：上色用的是 <head> 里那个 <style>，不在 body 子树上。
    try { window.dispatchEvent(new Event("hana-theme-applied")); } catch (e) {}
    // 注意：这里【不碰】 document.documentElement 的 data-theme。
    // 那是 magpie 自己的「外观」开关（system/light/dark）的地盘，
    // 抢过来写就等于把它的设置项焊死。我们只读它，见 render() 里的选取逻辑。
  }


  var appliedKey = "";
  // followNow：Hana 那边刚换过主题。为 true 时，这一次渲染无条件跟随
  // Hana 当前主题（忽略 magpie 自己的 light/dark）。
  //
  // 这样 magpie 的「外观」开关成了一次「手动覆盖」：你点它，卡片按
  // 你的明暗偏好走（映射到 Hana 的对应浅/深主题）；等你下次在 Hana 里
  // 换主题，跟随重新接管。两个诉求因此不打架。
  var followNow = true;

  function dataThemeAttr(){
    try { return document.documentElement.getAttribute("data-theme") || ""; } catch (err) { return ""; }
  }

  function render(){
    var choice = window.__hanaThemeChoice || "auto";
    if (choice !== "auto") {
      if (appliedKey === choice + "|" + dataThemeAttr()) return Promise.resolve();
      return varsFor(choice).then(function(v){
        if (!v) return;
        applyVars(v);
        appliedKey = choice + "|" + dataThemeAttr();
      });
    }
    var host = hostName || P.theme || (choice === "auto" ? seedName : "") || "";
    if (!host) return Promise.resolve();
    return varsFor(host).then(function(v){
      if (!v) return;
      var dt = dataThemeAttr();
      var target = host;
      if (!followNow && (dt === "light" || dt === "dark") && schemeOf(v) && schemeOf(v) !== dt) {
        target = dt === "light" ? (P.light || "warm-paper") : (P.dark || "midnight");
      }
      followNow = false;
      if (target === host) {
        if (appliedKey !== host + "|" + dt) { applyVars(v); appliedKey = host + "|" + dt; }
        return;
      }
      if (appliedKey === target + "|" + dt) return;
      return varsFor(target).then(function(v2){
        if (!v2) return;
        applyVars(v2);
        appliedKey = target + "|" + dt;
      });
    });
  }

  // 给同一页里的其它注入脚本（卡片里的「外观」下拉）一个入口：
  // 换完主题选择就地重画，不用等下面那个 5 秒轮询。
  window.__hanaThemeApply = function(next){
    if (typeof next === "string" && next) window.__hanaThemeChoice = next;
    appliedKey = "";
    followNow = true;
    return render();
  };

  // 判一套配色变量的明暗（用来和 magpie 自己的 data-theme 比对）。
  // 放在 render 之前定义；早先这次清理误删过它，导致 render 里抛
  // ReferenceError 被 .catch 吞掉，样式永远注入不进去（现象：--bg 一直是
  // magpie 默认值，而且日志里什么都不报）。
  function schemeOf(v){
    if (!v) return "";
    var lum = luminance(v["--bg"] || v["--background"] || "");
    if (lum === null) return "";
    return lum > 0.55 ? "light" : "dark";
  }

  // 诊断出口：把内部状态挂到 window 上，便于从外部查「为什么没跟上」。
  // 只读，不影响任何行为。（早先没有这个，定位轮询问题时只能猜。）
  window.__hanaThemeDiag = function(){
    return {
      choice: window.__hanaThemeChoice || "auto",
      hostName: hostName,
      urlTheme: P.theme,
      light: P.light,
      dark: P.dark,
      appliedKey: appliedKey,
      dataTheme: dataThemeAttr(),
      cacheKeys: Object.keys(cache),
      hasSeed: !!(seedVars && usable(seedVars)),
      dataTheme: document.documentElement.getAttribute("data-theme") || "",
      styleLen: (document.getElementById(STYLE_ID) || {}).textContent ? document.getElementById(STYLE_ID).textContent.length : 0,
      base: BASE,
      href: location.href
    };
  };

  render();

  // magpie 自己改 data-theme（用户点它的外观开关）时立刻跟上。
  // 这里把 appliedKey 清掉再 render：用户显式改了明暗，这是一次「手动覆盖」，
  // 必须让它当场生效（followNow 在首次渲染后已是 false，所以会走明暗映射）。
  try {
    if (window.MutationObserver) {
      new MutationObserver(function(){ appliedKey = ""; render(); })
        .observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    }
  } catch (e) {}

  // ── 跟随 Hana 换主题 ─────────────────────────────────────────────────
  // 宿主当前版本不主动推 hana.theme.changed（bundle 里搜不到），所以轮询。
  // hostName 是「当前实际跟着的主题」，也是下一次轮询重算的依据。
  var hostName = "";
  function pollHostTheme(){
    return fetch(BASE + "/_hana/host-theme", { credentials: "same-origin" })
      .then(function(r){ return r.ok ? r.json() : null; })
      .then(function(j){
        if (!j || !j.theme) return;
        if (j.light) P.light = j.light;
        if (j.dark) P.dark = j.dark;
        // 主题选择（设置页的主题下拉 / 卡片里的「外观」下拉）变了：
        // 把它当一次显式选择，把跟随拿回来并强制重算。
        // 没这一段的话，已打开的页面会一直用挂载当刻的旧选择。
        var choiceChanged = false;
        if (typeof j.choice === "string" && j.choice && j.choice !== window.__hanaThemeChoice) {
          window.__hanaThemeChoice = j.choice;
          appliedKey = "";
          followNow = true;
          choiceChanged = true;
        }
        // 只跟 hostName 比。不能把 P.theme 也当“当前值”：P.theme 是挂载当刻
        // 的 URL 快照，拿它比会让首次轮询就把 hostName 定死，之后 A→B→A
        // 这种来回切换永远回不去。
        if (j.theme === hostName && !choiceChanged) return;
        hostName = j.theme;
        // Hana 那边换了主题：重新把「跟随」拿回来，并强制重算。
        // （用户在 magpie 里点过外观的话，followNow 已被置 false；
        //  这里把它改回 true，于是 Hana 的这一次变更能无条件生效。）
        followNow = true;
        appliedKey = "";
        return render();
      })
      .catch(function(){});
  }
  pollHostTheme();
  setInterval(pollHostTheme, 5000);

  // ── 避让宿主卡片右上角悬浮的按钮簇 ───────────────────────────────
  // 宿主在卡片右上角悬浮着它的按钮簇（设置/关闭）。magpie 的头部已改成两行，
  // ↻ ⚙ 挪到了第二行；这里再量出那一簇到底往下占了多少，写成 --hana-head-safe，
  // 让第一行（品牌那一行）的高度刚好把它躲开，第二行整行就都在它下面。
  //
  // 不猜宿主的类名：用 elementsFromPoint 在「本视口右上角」几个点上问
  // 「谁压在最上面」，一直数到遇上本 iframe 为止——之前那些就是压在卡片上的
  // 悬浮层。取它们相对本视口顶部的最大下探深度，就是需要让出的高度。
  (function adapt(){
    var safe = 48;      // 量不到时的兜底
    var gap = 112;
    function measure(){
      if (!window.parent || window.parent === window) return null;   // 直接访问代理：没有宿主层
      var frame = null;
      try { frame = window.frameElement; } catch (err) {}
      if (!frame) return null;
      var par, d, vw, fTop = 0;
      try { par = window.parent; d = par.document; vw = par.innerWidth || 0; } catch (err) { return null; }
      if (!d || !d.elementsFromPoint) return null;
      try { fTop = frame.getBoundingClientRect().top; } catch (err) {}
      var deepest = 0, widest = 0;
      var pts = [[vw - 18, fTop + 16], [vw - 54, fTop + 16], [vw - 18, fTop + 38], [vw - 92, fTop + 20]];
      for (var i = 0; i < pts.length; i++) {
        var list;
        try { list = d.elementsFromPoint(pts[i][0], pts[i][1]); } catch (err) { continue; }
        for (var k = 0; k < list.length; k++) {
          var el = list[k];
          if (el === frame || el.contains(frame)) break;   // 到本 iframe 就停
          var r;
          try { r = el.getBoundingClientRect(); } catch (err) { continue; }
          if (r.width <= 0 || r.height <= 0) continue;
          if (r.bottom < fTop) continue;
          var below = r.bottom - fTop;
          if (below > deepest) deepest = below;
          var right = vw - r.left;
          if (right > widest) widest = right;
        }
      }
      if (deepest <= 0) return null;
      return { safe: Math.ceil(deepest) + 6, gap: Math.ceil(widest) + 8 };
    }
    function run(){
      var m = null;
      try { m = measure(); } catch (err) {}
      if (m) { safe = m.safe; gap = m.gap; }
      try {
        var rs = document.documentElement.style;
        rs.setProperty("--hana-head-safe", safe + "px");
        rs.setProperty("--hana-chrome-gap", gap + "px");
      } catch (err) {}
    }
    run();
    setTimeout(run, 700);
    setTimeout(run, 2200);
  })();
})();
</script>
`;

// ── 外观下拉：把 magpie 设置页的分段控件换成一枚自绘下拉 ──────────────────
//
// 目标 DOM（magpie 自己的 index.html，设置 → 常规）：
//   <div class="row pref">
//     <div class="who">…外观 / 浅色、深色，或跟随系统…</div>
//     <div id="themeSegs" class="om-hide"></div>
//   </div>
// app.js 会把 #themeSegs 填成一个 .segs 盒子（dataset.kind = "system|light|dark"），
// 里面三个 button.opt，点谁谁带 .on。
//
// 做法：先把 #themeSegs 收起来（它此刻还是空的，所以没有闪动），在旁边插入自己
// 的下拉；选项就直接用注入的 THEME_OPTIONS（与设置页同一份清单）。
// 选中一项 = 写「主题选择」（App 的 ui.json 是唯一写者），本页立刻重画；
// 原生控件仍然留着（只藏不删），magpie 自己的存取路径不受影响。
// 万一本段脚本没跑通，3.6 秒后把原生控件放回来。
const SELECT_CLIENT = `<script id="hana-select-client">
(function(){
  var ROOT = document.documentElement;
  var NATIVE_ID = "themeSegs";
  var HOST_ID = "hanaThemeSelect";
  // 值就是「主题选择」本身：auto 或某个 Hana 主题名。
  // 服务端拿不到时的兜底（与 proxy.mjs 里的 THEME_OPTIONS 同一份）
  var FALLBACK = [
    ["auto", "自动（跟随 Hana）"],
    ["midnight", "青夜"], ["midnight-contrast", "青夜 · 高对比"],
    ["warm-paper", "暖纸"], ["new-warm-paper", "新暖纸"], ["high-contrast", "素白"],
    ["grass-aroma", "草香"], ["contemplation", "沉思"], ["absolutely", "Absolutely"],
    ["delve", "Delve"], ["deep-think", "Deep Think"], ["coral", "珊瑚"]
  ];
  // 与 BASE_SHIM / THEME_CLIENT 同一套算法：卡片挂载前缀，直接访问代理时为空串。
  var BASE = (function(){
    var p = location.pathname || "/";
    if (p === "/") return "";
    return p.charAt(p.length - 1) === "/" ? p.slice(0, -1) : p;
  })();

  // 立刻收起原生分段控件。此刻它还没被 app.js 填内容，所以不闪；
  // 构建失败时（见 ensure 的兜底）会把类摘掉，原生控件原样回来。
  try { ROOT.classList.add("hana-theme-select"); } catch (e) {}

  var state = { opts: [], ids: [], optEls: [], label: null, panel: null, open: false, built: false };

  function report(kind, extra) {
    try { if (typeof window.__hanaDiag === "function") window.__hanaDiag(kind, extra || {}); } catch (e) {}
  }
  function nativeBox() { return document.getElementById(NATIVE_ID); }
  function trim(s) { return String(s == null ? "" : s).replace(/^\\s+|\\s+$/g, ""); }

  // 选项表：来自服务端注入的 THEME_OPTIONS（window.__hanaThemeList）；
  // 拿不到时用内置的那份。分隔线放在第 1 项之后（自动 / 具体主题 之间），
  // 与设置页里的位置一致。
  function themeOptions() {
    var list = (window.__hanaThemeList && window.__hanaThemeList.length) ? window.__hanaThemeList : FALLBACK;
    var out = [];
    for (var i = 0; i < list.length; i++) {
      var id = String((list[i] && list[i][0]) || "");
      if (!id) continue;
      out.push({ id: id, label: String((list[i] && list[i][1]) || id), sepBefore: out.length === 1 });
    }
    return out;
  }

  // 当前值就是「主题选择」本身（跟着代理走，5 秒轮询保持最新）。
  // 早先这里还去读 magpie 原生控件的明暗，于是下拉里多出「浅色 / 深色」——
  // 它们只是把明暗叠在「跟随 Hana」上，点了看不出变化，也与设置页清单对不上。
  // 现在选择只有一个来源。
  function currentId() {
    var choice = "";
    try { choice = String(window.__hanaThemeChoice || ""); } catch (e) {}
    return choice || "auto";
  }

  // 那一行的说明文字是 magpie 自己的 i18n 串（「浅色、深色，或跟随系统」），
  // 它描述的是原生的三档；下拉换成 Hana 主题清单后这句就对不上了，改成同义的一句。
  //
  // 不能记「已改过」标记就完事：magpie 换语言/重绘时会按自己的 data-t/data-en
  // 把文字写回去，而标记还留着 —— 实测就撞上了（它自更新到 0.1.973 之后那片描述又变回去了）。
  // 现在改成比文字：不一样就再改一遍，一样就不动（不会自激）。同时看着那一格的子树，
  // 它被改回去就能当场纠回来。元素叫 .sub（不是 .desc），一份普通、一份 om-only（Omarchy 下才显示）。
  var ROW_DESC = "跟随 Hana 当前主题，或钉住某一套 Hana 主题（与插件设置里的「主题」同一份）";
  function fixRowDesc() {
    var box = nativeBox();
    if (!box) return;
    try {
      var row = box.closest ? box.closest(".row") : null;
      if (!row) return;
      var subs = row.querySelectorAll(".sub");
      for (var i = 0; i < subs.length; i++) {
        if (subs[i].textContent === ROW_DESC) continue;
        subs[i].textContent = ROW_DESC;
      }
      watchRowDesc(row);
    } catch (e) {}
  }
  function watchRowDesc(row) {
    var who = row.querySelector(".who");
    if (!who || who.__hanaDescWatched || !window.MutationObserver) return;
    who.__hanaDescWatched = true;
    new MutationObserver(function(){ fixRowDesc(); })
      .observe(who, { childList: true, characterData: true, subtree: true });
  }

  function refresh() {
    var v = currentId();
    for (var i = 0; i < state.optEls.length; i++) {
      var el = state.optEls[i];
      var on = el.dataset.value === v;
      if (on) el.classList.add("selected"); else el.classList.remove("selected");
      el.setAttribute("aria-selected", on ? "true" : "false");
    }
    if (state.label) {
      var text = "";
      for (var k = 0; k < state.opts.length; k++) if (state.opts[k].id === v) text = state.opts[k].label;
      state.label.textContent = text || (state.opts[0] ? state.opts[0].label : "");
    }
  }

  function openPanel() {
    var p = state.panel, host = document.getElementById(HOST_ID);
    if (!p || !host) return;
    var tr = host.getBoundingClientRect();
    var vh = window.innerHeight || document.documentElement.clientHeight || 480;
    var vw = window.innerWidth || document.documentElement.clientWidth || 320;
    var gap = 4, pad = 8;
    p.hidden = false;
    p.style.minWidth = Math.max(140, Math.round(tr.width)) + "px";
    p.style.left = Math.round(tr.left) + "px";
    p.style.top = Math.round(tr.bottom + gap) + "px";
    p.style.maxHeight = "240px";
    var ph = p.offsetHeight;
    // 下方空间不够就翻到触发器上面；连上面也放不下就贴着下沿、滚着看
    if (tr.bottom + gap + ph > vh - pad) {
      var above = tr.top - gap - ph;
      if (above >= pad) p.style.top = Math.round(above) + "px";
      else {
        p.style.maxHeight = Math.max(96, Math.round(vh - tr.bottom - pad - gap)) + "px";
        p.style.top = Math.round(tr.bottom + gap) + "px";
      }
    }
    var pw = p.offsetWidth;
    // 贴著右缘时改成右对齐：否则比触发器宽的面板会鼓到卡片外面去
    if (tr.right > vw * 0.55) p.style.left = Math.max(pad, Math.round(tr.right - pw)) + "px";
    var left = parseFloat(p.style.left) || 0;
    if (left + pw > vw - pad) p.style.left = Math.max(pad, Math.round(vw - pw - pad)) + "px";
    state.open = true;
    // 面板的「打开」体现在 .open 上：先定位、下一帧再上类，过渡才从正确位置展开。
    // rAF 在隐藏/后台的窗口里不触发（实测 visibilityState=hidden 时不跑），
    // 补一个定时器兜底，否则面板只是 hidden=false 却永远不显形。两边都是幂等的。
    var reveal = function(){
      if (!state.open) return;
      p.classList.add("open");
      host.classList.add("open");
      host.setAttribute("aria-expanded", "true");
    };
    requestAnimationFrame(reveal);
    setTimeout(reveal, 60);
  }

  function closePanel() {
    if (!state.open) return;
    state.open = false;
    var p = state.panel, host = document.getElementById(HOST_ID);
    if (p) p.classList.remove("open");
    if (host) { host.classList.remove("open"); host.setAttribute("aria-expanded", "false"); }
    setTimeout(function(){ if (!state.open && p) p.hidden = true; }, 140);
  }

  // 选择落地：写进 App 的 ui.json（它才是唯一写者，写完再推给代理进程），
  // 然后让本页立刻按新选择重画，不等那 5 秒轮询。
  //
  // 为什么要绕 App 的 routes 而不是直接 POST 代理的 /_hana/theme：
  // 代理那份只是内存，重启就丢；设置页读写的也是 App 的那份。两边必须同源。
  // 凭据靠卡片 iframe URL 上的 appSurfaceSession（与设置页 SDK 用的是同一个）。
  // 直接打开代理页（没有会话）时退回代理自己的端点，功能照旧，只是不落盘。
  function choiceUrl() { return location.origin + "/api/apps/magpie-hana/routes/magpie-hana/theme"; }
  // 凭据：宿主给卡片的 appSurfaceSession。设置页的 SDK 是从 iframe URL 的查询串里
  // 取它的；卡片自己那个 URL 里票据也可能在路径上（…/_surface/<ticket>/…）。
  // 两条都试，拿不到就用代理端点兜底（直接打开代理页就是这种情况）。
  function surfaceSession() {
    var s = "";
    try { s = new URLSearchParams(location.search || "").get("appSurfaceSession") || ""; } catch (e) {}
    if (s) return s;
    // 卡片自己那个 URL 里票据在路径上（…/_surface/<ticket>/…）。不用正则：
    // 这层模板串会把 \/ 这类转义吃掉一层，写 indexOf 最稳。
    try {
      var p = String(location.pathname || "");
      var at = p.indexOf("/_surface/");
      if (at >= 0) {
        var rest = p.slice(at + 10);
        var cut = rest.indexOf("/");
        s = decodeURIComponent(cut >= 0 ? rest.slice(0, cut) : rest);
      }
    } catch (e) {}
    return s;
  }
  function postChoice(id) {
    // 卡片壳模式：交给壳子代转（壳子 POST App 路由，且能带同源凭据）。
    if (hanaShell) {
      try {
        window.parent.postMessage({ type: "magpie-hana:choice", theme: id }, "*");
        report("theme-choice-post", { via: "card-shell", theme: id });
        return;
      } catch (e) { /* 掉下去走老路径 */ }
    }
    var session = surfaceSession();
    if (!session) {
      report("theme-choice-post", { via: "proxy", theme: id, reason: "no-session" });
      proxyPostChoice(id);
      return;
    }
    try {
      fetch(choiceUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Hana-App-Surface-Session": session },
        body: JSON.stringify({ theme: id })
      }).then(function(r){
        if (r && r.ok) { report("theme-choice-post", { via: "surface", theme: id, status: r.status }); return; }
        report("theme-choice-post", { via: "proxy", theme: id, status: r ? r.status : 0 });
        proxyPostChoice(id);
      }).catch(function(e){
        report("theme-choice-post", { via: "proxy", theme: id, reason: String((e && e.message) || e) });
        proxyPostChoice(id);
      });
    } catch (e) { proxyPostChoice(id); }
  }
  function proxyPostChoice(id) {
    try {
      fetch(BASE + "/_hana/theme", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ theme: id })
      }).catch(function(){});
    } catch (e) {}
  }
  function applyChoice(id) {
    postChoice(id);
    try {
      if (typeof window.__hanaThemeApply === "function") window.__hanaThemeApply(id);
      else window.__hanaThemeChoice = id;
    } catch (e) {}
  }

  function pick(id) {
    // 每一项都直接落到「主题选择」上（App 的 ui.json 是唯一写者），
    // 本页自己立刻重画，不等那 5 秒轮询。
    applyChoice(id);
    closePanel();
    refresh();
  }

  function buildPanel(opts) {
    var p = state.panel;
    if (!p) {
      p = document.createElement("div");
      p.className = "hana-select-panel";
      p.setAttribute("role", "listbox");
      p.hidden = true;
      document.body.appendChild(p);
      state.panel = p;
      document.addEventListener("click", function(){ closePanel(); });
      document.addEventListener("keydown", function(e){ if (e.key === "Escape") closePanel(); });
      // 面板自己滚（选项多、卡片矮，这里必然要滚）不算「页面动了」。
      // 早先这条不带判断，滚轮一进面板就把它收掉，现象就是「一滚动菜单就消失」。
      // scroll 不冒泡，但捕获阶段照样经过 window，所以必须在捕获里放过面板内部。
      window.addEventListener("scroll", function(e){
        var t = e && e.target;
        if (t && state.panel && (t === state.panel || (t.nodeType === 1 && state.panel.contains(t)))) return;
        closePanel();
      }, true);
      window.addEventListener("resize", function(){ closePanel(); });
    }
    p.innerHTML = "";
    state.optEls = [];
    for (var i = 0; i < opts.length; i++) {
      if (opts[i].sepBefore) {
        var sep = document.createElement("div");
        sep.className = "hana-select-sep";
        p.appendChild(sep);
      }
      var el = document.createElement("div");
      el.className = "hana-select-option";
      el.setAttribute("role", "option");
      el.dataset.value = opts[i].id;
      el.textContent = opts[i].label;
      (function(opt){
        el.addEventListener("click", function(e){ e.stopPropagation(); pick(opt.id); });
      })(opts[i]);
      p.appendChild(el);
      state.optEls.push(el);
    }
  }

  function build() {
    var opts = themeOptions();
    if (!opts || !opts.length) return false;
    var box = nativeBox();
    if (!box || !box.parentNode) return false;
    state.opts = opts;
    state.ids = [];
    for (var i = 0; i < opts.length; i++) state.ids.push(opts[i].id);

    var host = document.getElementById(HOST_ID);
    if (!host) {
      host = document.createElement("div");
      host.id = HOST_ID;
      host.className = "hana-select";
      host.setAttribute("role", "button");
      host.setAttribute("tabindex", "0");
      host.setAttribute("aria-haspopup", "listbox");
      host.setAttribute("aria-expanded", "false");
      var span = document.createElement("span");
      span.className = "hana-select-label";
      var chev = document.createElement("span");
      chev.className = "hana-select-chevron";
      chev.setAttribute("aria-hidden", "true");
      host.appendChild(span);
      host.appendChild(chev);
      state.label = span;
      // 无障碍名：借同一行「外观 / Appearance」那行标题，别让读屏器念一个空按钮
      var rowName = "";
      try {
        var icon = box.closest ? box.closest(".row") : null;
        var nm = icon ? icon.querySelector(".name") : null;
        rowName = nm ? trim(nm.textContent) : "";
      } catch (e) {}
      host.setAttribute("aria-label", rowName || "外观");
      host.addEventListener("click", function(e){
        e.stopPropagation();
        if (state.open) closePanel(); else openPanel();
      });
      host.addEventListener("keydown", function(e){
        var k = e.key;
        if (k === "Enter" || k === " " || k === "Spacebar") {
          e.preventDefault();
          if (state.open) closePanel(); else openPanel();
        } else if (k === "Escape") { closePanel(); }
      });
    }
    // 紧跟在原生控件后面（同一行的右侧那一格）
    if (host.parentNode !== box.parentNode) box.parentNode.insertBefore(host, box.nextSibling);
    // 确认建出来了才收起原生控件（先前若因建不出来而放回过，这里再收起来）
    try { ROOT.classList.add("hana-theme-select"); } catch (e) {}

    buildPanel(opts);
    fixRowDesc();
    state.built = true;
    refresh();
    watchNative();
    report("theme-select-ready", {
      options: state.ids.join("|"),
      labels: state.opts.map(function(o){ return o.label; }).join("|")
    });
    return true;
  }

  // 原生控件被重绘（app.js 每次重画都 replaceChildren）时同步选中态
  function watchNative() {
    var box = nativeBox();
    if (!box || box.__hanaSelectWatched || !window.MutationObserver) return;
    box.__hanaSelectWatched = true;
    new MutationObserver(function(){ refresh(); fixRowDesc(); })
      .observe(box, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });
  }

  var boot = 0, lastTry = 0, warned = false, retryTimer = null;
  function ensure() {
    if (state.built && document.getElementById(HOST_ID)) return true;
    // 防抖：DOM 一抖就重试会把主线程拖住，而建不出来的原因往往同一刻也不会变。
    // 被防抖挡掉的那次排一个尾随重试，免得错过刚出现的窗口。
    var now = Date.now();
    if (now - lastTry < 300) {
      if (!retryTimer) {
        retryTimer = setTimeout(function(){ retryTimer = null; ensure(); }, 320 - (now - lastTry));
      }
      return false;
    }
    retryTimer = null;
    lastTry = now;
    if (build()) return true;
    boot += 1;
    // 试了很多次都没成，先把原生控件放回来（别把设置弄丢），但不就此死心：
    // 设置页可能在很久之后才被打开，那时才轮到控件出现。
    if (boot >= 25 && !warned) {
      warned = true;
      try { ROOT.classList.remove("hana-theme-select"); } catch (e) {}
      report("theme-select-waiting", { tries: boot });
    }
    return false;
  }

  // magpie 自己改 data-theme 时同步（原生按钮也走这条）
  try {
    if (window.MutationObserver) {
      new MutationObserver(function(){ refresh(); })
        .observe(ROOT, { attributes: true, attributeFilter: ["data-theme"] });
    }
  } catch (e) {}

  // 页面配色换过（自己选的、设置页改的、Hana 换主题都算）就重新对标签
  try { window.addEventListener("hana-theme-applied", function(){ refresh(); }); } catch (e) {}

  // 设置页若被重建，把下拉补回去
  try {
    if (window.MutationObserver) {
      var pending = false;
      new MutationObserver(function(){
        if (pending) return;
        pending = true;
        setTimeout(function(){
          pending = false;
          if (!state.built || !document.getElementById(HOST_ID)) { state.built = false; ensure(); }
          else { refresh(); watchNative(); }
        }, 60);
      }).observe(document.body, { childList: true, subtree: true });
    }
  } catch (e) {}

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", ensure);
  ensure();
  setTimeout(ensure, 1200);
  setTimeout(ensure, 3000);
  // 安全网：即便没等到 DOM 变更（magpie 在某些路径上是直接换子节点，理论上有，
  // 但不想把“能不能出来”压在一个观察器上），前两分钟每 2.5 秒也看一眼
  var tick = setInterval(function(){
    if (state.built && document.getElementById(HOST_ID)) { clearInterval(tick); return; }
    ensure();
  }, 2500);
  setTimeout(function(){ clearInterval(tick); }, 120000);
})();
</script>
`;

// 主题变量表：给页面首屏注入用（客户端脚本拿到后立即上色，不等任何请求）。
//
// choice 为具体主题名时就用它；auto 时按宿主当前主题解析（fetchThemeCss 内部
// 会读 preferences.json；拿不到就回退到 HANA_AUTO_DARK）。
// TTL 给短一点：用户的 Hana 主题可能随时换，服务端这份要能跟上。
let themeCache = { at: 0, key: null, vars: null };

async function themeVars() {
  const choice = state.themeChoice || "auto";
  let key = choice;
  if (choice === "auto") {
    const raw = readHanaTheme();
    key = "auto:" + (raw && raw !== "auto" ? raw : HANA_AUTO_DARK);
  }
  const fresh = themeCache.vars && themeCache.key === key && (Date.now() - themeCache.at) < 5000;
  if (fresh) return themeCache.vars;

  const r = await fetchThemeCss(choice === "auto" ? "" : choice);
  if (!r.ok) return themeCache.vars || null;   // 取不到就用旧的，不把页面搞硬
  const vars = parseThemeVars(r.css);
  if (!vars || !(vars["--bg"] || vars["--text"])) return themeCache.vars || null;
  themeCache = { at: Date.now(), key, vars };
  state.themeApplied = r.theme || choice;
  return vars;
}

function parseThemeVars(css) {
  const out = {};
  const re = /(--[A-Za-z0-9_-]+)\s*:\s*([^;}]+)/g;
  let m;
  while ((m = re.exec(css))) out[m[1]] = m[2].replace(/!important/gi, "").trim();
  return out;
}

// 兜底：宿主侧推来的 css（现在不再由 index.js 推送，保留端点以防将来需要）
function themeBlock() {
  if (!state.themeCss) return "";
  return `<style id="hana-theme-pushed">\n${state.themeCss}\n</style>\n`;
}

// 卡片里的头部布局改造。
//
// 背景：宿主在卡片右上角悬浮它的按钮簇（设置/关闭）。magpie 原生把 ↻ 刷新与
// ⚙ 设置放在头部右端，正好叠在那一块：既不好看，也点不到。
//
// magpie 自己会在宽度 >=800px 时隐藏品牌名、把头塌成单行 44px（实测：header 的
// class 变成 `top tight cramped inrow crowded packed`，`.brand{display:none}`，
// .actions 回到 y=9）——所以只调 order/padding 在宽卡片下会失效，必须接管布局。
//
// 这里固定两行：
//   第 1 行：品牌（只占左边一小块，右侧整块留给宿主按钮簇）
//   第 2 行：标签栏 + ↻ ⚙（图标跟在标签栏右端）
// 第 1 行高度走 `--hana-head-safe`（客户端量出宿主按钮簇实际占用后写入，默认 48px），
// 保证第 2 行整行都在宿主按钮簇下方，两者不重叠。
//
// 为什么用 grid：flex-wrap 下 #nav 是 flex:0 0 100%，改 order 会把它自己占满一行、
// .actions 被挤到第三行；grid 的模板区能把两者确定性地锁在同一行。
const ADAPT_CSS = `html:root header.top{
  display: grid !important;
  grid-template-columns: minmax(0, 1fr) auto !important;
  grid-template-areas: "brand ." "nav actions" !important;
  align-items: center !important;
  column-gap: 10px !important;
  row-gap: 2px !important;
  height: auto !important;
  padding: 0 12px 8px 12px !important;
}
html:root header.top .brand{
  grid-area: brand !important;
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  min-height: var(--hana-head-safe, 48px) !important;
  height: auto !important;
}
html:root header.top .brand > span:not(.logo){
  display: inline !important;
}
html:root header.top #nav{
  grid-area: nav !important;
  position: relative !important;
  left: auto !important;
  top: auto !important;
  transform: none !important;
  margin: 0 !important;
  min-width: 0 !important;
  overflow-x: auto !important;
  overflow-y: hidden !important;
  scrollbar-width: none !important;
}
html:root header.top #nav::-webkit-scrollbar{
  display: none !important;
}
html:root header.top .actions{
  grid-area: actions !important;
  margin-left: 0 !important;
  flex: none !important;
  align-self: center !important;
}
`;

function adaptBlock() {
  return `<style id="hana-adapt">\n${ADAPT_CSS}</style>\n`;
}

// 把 magpie 设置页「常规 → 外观」那只分段控件（跟随系统 / 浅色 / 深色）换成
// 一枚 HanaSelect 风格的下拉。
//
// 为什么是「盖住」而不是改 magpie：内置的 exe 是上游原件，本 App 一行都不动它
// （README 里写着「未修改 magpie 的任何代码」）。原生 #themeSegs 仍是状态源，
// 下拉只是它的皮肤与开关，做法与 git-save-load 的 HanaSelect、本 App 设置页的
// 主题下拉完全一致 —— 原生 <select>/分段控件负责存值，自绘控件负责视觉与交互。
//
// 尺寸对齐 magpie 自己的 .segs：高 26px（2 + 22 + 2）、圆角 7px、字号 12px。
// 颜色全部取注入进页面的 Hana 变量（--card/--line/--fg/--accent/…），换主题时
// 跟着一起变。
// 卡片「外观」下拉的选项表。值域两档，语义互相排斥：
//   auto       跟随 Hana 当前主题（Hana 里换主题，卡片 5 秒内跟上）
//   Hana 主题名 钉住那一套（不论 Hana 当前用什么）
//
// 这份清单与 App 设置页「主题」下拉（ui/settings.html 里的 THEMES）逐项一致：
// 同一个存储、同一批名字、同一个顺序，两边都要改就一起改。
// 曾经这里多出「浅色 / 深色」两项，但它们只是把明暗叠在「跟随 Hana」上，
// 点了视觉上不会变，而且设置页里没有这两项 —— 属于多余，已删。
// index.js 的 actTheme 里另有一份 VALID 白名单，它只决定参数收不收；
// 清单本身以这里与 settings.html 为准。
const THEME_OPTIONS = [
  ["auto", "自动（跟随 Hana）"],
  ["midnight", "青夜"],
  ["midnight-contrast", "青夜 · 高对比"],
  ["warm-paper", "暖纸"],
  ["new-warm-paper", "新暖纸"],
  ["high-contrast", "素白"],
  ["grass-aroma", "草香"],
  ["contemplation", "沉思"],
  ["absolutely", "Absolutely"],
  ["delve", "Delve"],
  ["deep-think", "Deep Think"],
  ["coral", "珊瑚"],
];

const SELECT_CSS = `html:root.hana-theme-select #themeSegs{
  display: none !important;
}
html:root #hanaThemeSelect.hana-select{
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  box-sizing: border-box;
  min-width: 132px;
  height: 26px;
  padding: 0 8px 0 10px;
  background: var(--card, rgba(127, 127, 127, .08));
  border: 1px solid var(--line, rgba(127, 127, 127, .28));
  border-radius: 7px;
  color: var(--fg, #e8e9ed);
  font: inherit;
  font-size: 12px;
  line-height: 1;
  cursor: default;
  user-select: none;
  -webkit-user-select: none;
  outline: none;
  transition: border-color .15s ease, box-shadow .15s ease;
}
html:root #hanaThemeSelect.hana-select:hover{
  border-color: var(--fg-2, var(--muted, #8b8d98));
}
html:root #hanaThemeSelect.hana-select.open{
  border-color: var(--accent, #6b7dff);
  box-shadow: 0 0 0 3px var(--sel, rgba(107, 125, 255, .22));
}
html:root #hanaThemeSelect .hana-select-label{
  flex: 1 1 auto;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
html:root #hanaThemeSelect .hana-select-chevron{
  flex: none;
  width: 10px;
  height: 10px;
  opacity: .55;
  background-color: currentColor;
  -webkit-mask-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 8' fill='none' stroke='%23fff' stroke-width='1.6' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M1 1l5 5 5-5'/%3E%3C/svg%3E");
  -webkit-mask-repeat: no-repeat;
  -webkit-mask-position: center;
  -webkit-mask-size: contain;
  mask-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 8' fill='none' stroke='%23fff' stroke-width='1.6' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M1 1l5 5 5-5'/%3E%3C/svg%3E");
  mask-repeat: no-repeat;
  mask-position: center;
  mask-size: contain;
  transition: transform .15s ease;
}
html:root #hanaThemeSelect.open .hana-select-chevron{
  transform: rotate(180deg);
}
html:root .hana-select-panel{
  position: fixed;
  z-index: 4000;
  box-sizing: border-box;
  min-width: 140px;
  max-height: 240px;
  padding: 4px;
  background: var(--pop-bg, var(--card, #232830));
  border: 1px solid var(--line, rgba(127, 127, 127, .28));
  border-radius: 8px;
  box-shadow: var(--shadow, 0 16px 44px rgba(0, 0, 0, .35));
  overflow-y: auto;
  opacity: 0;
  transform: scale(.97) translateY(-4px);
  transform-origin: top left;
  transition: opacity .12s ease, transform .12s ease;
  pointer-events: none;
}
html:root .hana-select-panel.open{
  opacity: 1;
  transform: none;
  pointer-events: auto;
}
html:root .hana-select-sep{
  height: 1px;
  margin: 4px 6px;
  background: var(--line, rgba(127, 127, 127, .28));
}
html:root .hana-select-option{
  position: relative;
  padding: 6px 12px 6px 26px;
  border-radius: 5px;
  font-size: 12px;
  color: var(--fg, #e8e9ed);
  cursor: default;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  transition: background .12s ease;
}
html:root .hana-select-option:hover{
  background: var(--pill-hover, rgba(127, 127, 127, .14));
}
html:root .hana-select-option.selected{
  color: var(--accent, #6b7dff);
  font-weight: 500;
}
html:root .hana-select-option.selected::before{
  content: "\\2713";
  position: absolute;
  left: 9px;
  color: var(--accent, #6b7dff);
  font-size: 11px;
}
`;

function selectBlock() {
  return `<style id="hana-select">\n${SELECT_CSS}</style>\n`;
}

// 焦点环：点一下某个面板，它就一直带着一圈外框，看着像"选中框"。
// 来源是 magpie 自己的样式（app.css 里 `button:focus-visible, input:focus-visible
// { outline: 2px solid var(--accent) }`，以及部分组件用 box-shadow 画的环）。
// 这里只掐「按钮/可聚焦容器」那一类：input/textarea/select 的焦点环保留，
// 键盘可用性不受影响。html:root 是为了在特异性上压过 magpie 自己的规则。
const FOCUS_CSS = `html:root button:focus,
html:root button:focus-visible,
html:root a:focus,
html:root a:focus-visible,
html:root summary:focus,
html:root summary:focus-visible,
html:root [tabindex]:focus,
html:root [tabindex]:focus-visible,
html:root canvas:focus,
html:root canvas:focus-visible {
  outline: none !important;
}
html:root [tabindex]:focus-visible,
html:root canvas:focus-visible {
  box-shadow: none !important;
}
`;

function focusBlock() {
  return `<style id="hana-focus">\n${FOCUS_CSS}</style>\n`;
}

function hiddenBlock() {
  // 选择器必须对真实的 DOM。magpie 的顶部导航是：
  //   <nav class="seg" id="nav"><button data-view="library">…</button></nav>
  // （早先写的 #ptabs [data-ptab=…] 打的是另一个 hidden 的窄屏导航，所以没效果）
  const byFeature = {
    library: ['#nav button[data-view="library"]'],
    sessions: ['#nav button[data-view="sessions"]'],
    routing: ['#nav button[data-view="routing"]', '#view-routing'],
    usage: ['#nav button[data-view="usage"]', '#view-usage'],
    gateway: ['#nav button[data-view="gateway"]', '#view-gateway'],
    providers: ['#nav button[data-view="providers"]', '#view-providers'],
    plugins: ['#nav button[data-view="plugins"]', '#view-plugins'],
    "settings-otel": ['#setTab-otel', '#setPage-otel'],
    "settings-sync": ['#setTab-sync', '#setPage-sync'],
    "settings-privacy": ['#setTab-privacy', '#setPage-privacy'],
  };
  const rules = [];
  for (const f of state.hidden) if (byFeature[f]) rules.push(...byFeature[f]);
  if (!rules.length) return "";
  return `<style id="hana-hide">\n${rules.join(",\n")} { display: none !important; }\n</style>\n`;
}

// 宿主把本服务挂在 /api/apps/<id>/routes/_runtime/<rid>/ 下。magpie 的前端
// 用绝对路径调 API（fetch("/api/" + path)），不加修正就会绕过前缀打到宿主根。
// 这里注入一段垫片：把绝对路径补上挂载前缀。相对路径（app.js、app.css 等）
// 浏览器自己会带前缀，不用管。
const BASE_SHIM = `<script id="hana-base">
(function(){
  var p = location.pathname;
  // 未经改写的原语
  window.__hanaOrigFetch = window.fetch;
  window.__hanaXhrOpen = window.XMLHttpRequest && XMLHttpRequest.prototype.open;
  var base = (p.length > 1 && p.charAt(p.length - 1) === '/') ? p.slice(0, -1) : p;
  window.__hanaMount = (base && base !== '/') ? base : '';

  // ── 诊断上报（必须在 return 之前，否则 base 为空时什么都上报不了）──
  function report(kind, extra){
    try {
      var url = (window.__hanaMount || '') + '/_hana/diag';
      var payload = JSON.stringify(Object.assign({
        kind: kind, href: String(location.href), pathname: String(location.pathname),
        base: base, readyState: document.readyState, t: Date.now()
      }, extra || {}));
      var f = window.__hanaOrigFetch;
      if (f) { f.call(window, url, { method: 'POST', body: payload, headers: {'Content-Type':'application/json'}, keepalive: true }).catch(function(){}); return; }
      if (navigator.sendBeacon) navigator.sendBeacon(url, payload);
    } catch (e) {}
  }
  window.__hanaDiag = report;
  report('boot');
  // 渲染侧的时钟：光看 DOM-ready 不够，得知道「首绘、最大内容绘、有没有长任务」
  // ——那几秒到底是在等网，还是在等渲染，就靠这三个观察器分开。
  // 只用 PerformanceObserver，不改任何页面行为；不支持就算了。
  try {
    if (window.PerformanceObserver) {
      new PerformanceObserver(function(list){
        list.getEntries().forEach(function(en){ report('paint', { name: String(en.name), ms: Math.round(en.startTime) }); });
      }).observe({ entryTypes: ['paint'] });
      new PerformanceObserver(function(list){
        var es = list.getEntries(); var last = es[es.length - 1];
        if (last) report('lcp', { ms: Math.round(last.startTime), tag: String((last.element && last.element.tagName) || '') });
      }).observe({ entryTypes: ['largest-contentful-paint'] });
      new PerformanceObserver(function(list){
        list.getEntries().forEach(function(en){ report('longtask', { ms: Math.round(en.duration) }); });
      }).observe({ entryTypes: ['longtask'] });
    }
  } catch (e) {}
  window.addEventListener('DOMContentLoaded', function(){ report('dom-ready'); });
  window.addEventListener('load', function(){
    var sheets = 0; try { sheets = document.styleSheets.length; } catch (e) {}
    report('load', { sheets: sheets });
  });
  window.addEventListener('error', function(e){
    var t = e.target;
    if (t && t.tagName && (t.tagName === 'LINK' || t.tagName === 'SCRIPT' || t.tagName === 'IMG')) {
      report('resource-error', { tag: t.tagName, url: String(t.src || t.href || ''), rel: String(t.rel || '') });
    }
  }, true);
  setTimeout(function(){
    var sheets = 0; try { sheets = document.styleSheets.length; } catch (e) {}
    var links = [];
    try { links = [].slice.call(document.querySelectorAll('link[rel=stylesheet]'), 0, 6).map(function(l){ return l.href; }); } catch (e) {}
    report('snapshot', { sheets: sheets, linkCount: links.length, links: links });
  }, 3000);

  // 一次性探针：把"谁带着外框"说清楚，万一上面那条 CSS 没掐干净，
  // 下次不用再让用户复现一遍。只在页面里观察，不改任何行为。
  function ringOf(el) {
    try {
      var cs = getComputedStyle(el);
      return {
        tag: el.tagName,
        cls: String(el.className || '').slice(0, 80),
        id: String(el.id || ''),
        tabindex: String(el.getAttribute('tabindex')),
        outline: cs.outlineStyle + ' ' + cs.outlineWidth + ' ' + cs.outlineColor,
        shadow: String(cs.boxShadow || '').slice(0, 90),
        peers: (function () {
          try { return [].slice.call(document.querySelectorAll('input,textarea,select')).length; } catch (e) { return -1; }
        })()
      };
    } catch (e) { return { err: String(e && e.message) }; }
  }
  try {
    window.addEventListener('focusin', function (e) {
      var el = e.target;
      if (!el || el === document.body) return;
      report('probe', Object.assign({ probe: 'ring', via: 'focusin' }, ringOf(el)));
    }, true);
    setTimeout(function () {
      var el = document.activeElement;
      if (el && el !== document.body) report('probe', Object.assign({ probe: 'ring', via: 'active' }, ringOf(el)));
    }, 1500);
  } catch (e) {}

  if (!base || base === '/') return;
  function fix(u){
    if (typeof u !== 'string' || !u) return u;
    if (u.charAt(0) !== '/') return u;
    if (u.indexOf('//') === 0) return u;
    if (u.indexOf(base + '/') === 0) return u;
    return base + u;
  }
  var of = window.fetch;
  if (of) {
    window.fetch = function(input, init){
      try {
        if (typeof input === 'string') input = fix(input);
        else if (input && input.url) input = new Request(fix(input.url), input);
      } catch (e) {}
      return of.call(this, input, init);
    };
  }
  var XO = window.XMLHttpRequest && XMLHttpRequest.prototype.open;
  if (XO) {
    XMLHttpRequest.prototype.open = function(m, u){
      try { arguments[1] = fix(u); } catch (e) {}
      return XO.apply(this, arguments);
    };
  }
  document.addEventListener('click', function(e){
    var t = e.target;
    var a = t && t.closest ? t.closest('a[href^="/"]') : null;
    if (!a) return;
    var h = a.getAttribute('href');
    if (h && h.indexOf('//') !== 0) a.setAttribute('href', fix(h));
  }, true);
})();
</script>
`;

// ── 「从本机导入用量信息」（magepie 设置页 → 同步与备份，最下面一行）──────────────
// magepie 支持便携目录，所以本 App 带的是第二份 magepie，它的账本从零开始。
// 这一行就是把它本机那份的第一份 magepie 的历史并过来，任何人装上都能用。
const IMPORT_CSS = `html:root .hana-import-btn{
  text-align: left;
}
html:root .hana-import-btn.armed{
  color: var(--accent, #6b7dff);
}
html:root .hana-import-btn:disabled{
  opacity: .6;
}
html:root .hana-import-note{
  padding: 2px 14px 10px 14px;
  font-size: 12px;
  line-height: 1.6;
  color: var(--fg-2, var(--muted, #8b8d98));
  white-space: normal;
}
html:root .hana-import-note.warn{
  color: var(--warn, #d8a24a);
}
html:root .hana-import-note.bad{
  color: var(--bad, #d9615e);
}
`;

function importBlock() {
  return `<style id="hana-import">\n${IMPORT_CSS}</style>\n`;
}

const IMPORT_CLIENT = `<script id="hana-import-client">
(function(){
  var BASE = (function(){
    var p = location.pathname || "/";
    if (p === "/") return "";
    return p.charAt(p.length - 1) === "/" ? p.slice(0, -1) : p;
  })();
  var ROW_ID = "hanaImportRow";
  var LIST_ID = "syncList";
  var busy = false;
  var armed = false;

  function report(kind, extra) {
    try { if (typeof window.__hanaDiag === "function") window.__hanaDiag(kind, extra || {}); } catch (e) {}
  }
  function el(id) { return document.getElementById(id); }
  function note(text, tone) {
    var n = el("hanaImportNote");
    if (!n) return;
    if (!text) { n.hidden = true; n.textContent = ""; return; }
    n.hidden = false;
    n.textContent = text;
    n.className = "hana-import-note" + (tone ? " " + tone : "");
  }
  function reset() {
    armed = false;
    var b = el("hanaImportBtn");
    if (!b) return;
    b.disabled = false;
    b.textContent = "导入";
    b.classList.remove("armed");
  }

  // 跟上面那一排按钮对齐。
  // magpie 自己的动作列是右对齐的，所以按钮宽度 = 文字宽度（两个字 36px、三个字 45px），
  // 左边缘自然参差不齐。用户要的是「和上面的按钮左对齐」，所以把宽度撑到与上面那个
  // 按钮一样（不写死像素：直接量旁边那个真实按钮，这样换字号/缩放/语言都不会跑掉），
  // 文字改成左对齐，于是字和上面的字从同一个 x 开始。
  function alignToNeighbour() {
    var list = el(LIST_ID), row = el(ROW_ID), b = el("hanaImportBtn");
    if (!list || !row || !b) return;
    var prev = null;
    for (var i = 0; i < list.children.length; i++) {
      var c = list.children[i];
      if (c === row) break;
      if (c.querySelector && c.querySelector("button")) prev = c;
    }
    var ref = prev ? prev.querySelector("button") : null;
    if (!ref) return;
    var w = Math.round(ref.getBoundingClientRect().width);
    if (w > 0) b.style.minWidth = w + "px";
  }

  function build() {
    var list = el(LIST_ID);
    if (!list) return;
    var row = el(ROW_ID);
    var n = el("hanaImportNote");
    if (row && n) {
      // magpie 自己的那几行是异步渲染的，可能落在我们后面（实测就是这样）。
      // 用户要的是「最下面一行」，所以发现自己不是队尾就把这两件东西按顺序挪到末尾。
      // 挪完最后一次就不再变化，不会自激。
      if (list.lastElementChild !== n) { list.appendChild(row); list.appendChild(n); }
      alignToNeighbour();
      return;
    }
    if (!list || el(ROW_ID)) return;
    var row = document.createElement("div");
    row.className = "row pref";
    row.id = ROW_ID;
    var who = document.createElement("div");
    who.className = "who";
    var name = document.createElement("div");
    name.className = "name";
    name.textContent = "从本机导入用量信息";
    var sub = document.createElement("div");
    sub.className = "sub";
    sub.textContent = "把本机上另一份 magpie（如 ~/.config/magpie）的用量、配额与路由记录并进这里；可反复导入，重复的不算两遍。导入时 magpie 会重启一次。";
    who.appendChild(name);
    who.appendChild(sub);
    var ctl = document.createElement("div");
    // 用 magpie 自己的 .val：它自己的同步页就是 who + val，动作靠 .who 的 flex:1 推到右边。
    // 按钮也用它那个 text 类（纯文字，无框），这样跟「设置」「导出…」那几行排在一起是一个样子。
    ctl.className = "val";
    var b = document.createElement("button");
    b.type = "button";
    b.id = "hanaImportBtn";
    b.className = "text hana-import-btn";
    b.textContent = "导入";
    b.onclick = onClick;
    ctl.appendChild(b);
    row.appendChild(who);
    row.appendChild(ctl);
    var n = document.createElement("div");
    n.id = "hanaImportNote";
    n.className = "hana-import-note";
    n.hidden = true;
    list.appendChild(row);
    list.appendChild(n);
    alignToNeighbour();
  }

  function describe(rep) {
    if (!rep) return "导入完成。";
    var parts = [];
    var files = rep.files || [];
    for (var i = 0; i < files.length; i++) {
      var f = files[i];
      if (f.added > 0) parts.push(f.name.replace("routing/", "路由 ") + " +" + f.added);
    }
    if (!parts.length) return "本机那份没有这里还缺的记录，没有变化。";
    return "已并入：" + parts.join("、") + "。备份留在数据目录的 import-backup 里。";
  }

  async function onClick() {
    if (busy) return;
    if (!armed) {
      armed = true;
      var b0 = el("hanaImportBtn");
      if (b0) { b0.textContent = "确认导入（会重启 magpie）"; b0.classList.add("armed"); }
      note("再点一次就开始。导入会先停 magpie，并完再起，中间几秒它在休息。", "warn");
      return;
    }
    busy = true;
    var b1 = el("hanaImportBtn");
    if (b1) { b1.disabled = true; b1.textContent = "导入中…"; }
    note("正在导入，magpie 会短暂重启…", "warn");
    report("import-usage-start", {});
    try {
      var res = await fetch(BASE + "/_hana/import-usage", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apply: true })
      });
      var j = await res.json();
      if (!res.ok || !j || j.ok === false) throw new Error((j && j.error) || ("HTTP " + res.status));
      note(describe(j.report) + " 回到「额度」页刷新一下就能看到。", null);
    } catch (e) {
      note("导入失败：" + String((e && e.message) || e), "bad");
    } finally {
      busy = false;
      reset();
      report("import-usage-done", {});
    }
  }

  build();
  if (window.MutationObserver) {
    new MutationObserver(function(){ build(); }).observe(document.documentElement, { childList: true, subtree: true });
  }
})();
</script>
`;

// 页面里跑的「换页上报」。装在 </head> 之前，早于 app.js，所以能先包住
// history.replaceState —— app.js 换页最后一步就是它。
const VIEW_CLIENT = `<script id="hana-view-client">
(function(){
  var VALID = {agents:1,providers:1,gateway:1,routing:1,usage:1,sessions:1,library:1,plugins:1,settings:1};
  var q = new URLSearchParams(location.search || "");
  if ((q.get("mode") || "window") !== "window") return;   // 托盘面板没有页签
  var BASE = (function(){
    var p = location.pathname || "/";
    if (p === "/") return "";
    return (p.charAt(p.length - 1) === "/") ? p.slice(0, -1) : p;
  })();
  var AT = BASE + "/_hana/view";
  var last = "";
  var sending = false, pending = null;

  function report(v){
    if (!v || v === last) return;
    last = v;
    if (sending) { pending = v; return; }        // 上一次还在路上：记下最新的一次，等它回来再发
    sending = true;
    try {
      fetch(AT + "?view=" + encodeURIComponent(v), { method: "POST", keepalive: true })
        .catch(function(){})
        .then(function(){ sending = false; if (pending) { var n = pending; pending = null; report(n); } });
    } catch (e) { sending = false; }
  }

  function current(){
    var v = "";
    try { v = new URLSearchParams(location.search || "").get("view") || ""; } catch (e) {}
    if (VALID[v]) return v;
    // Agent 是 magpie 的默认页，它自己不写进地址（syncURL 里 delete("view")）。
    // 所以地址里没有 view 就是「在 Agent 页」——这里必须把它认成 agents 而
    // 不是空值，否则用户从「用量」点回「Agent」时，记下的还会是旧的「用量」。
    return "agents";
  }

  // 主路：包住 history.replaceState。app.js 每次 show() 都会调它把页写回地址栏，
  // 在这里取新值零延迟，也不用轮询。
  try {
    var rs = history.replaceState;
    if (typeof rs === "function") {
      history.replaceState = function(){
        var r = rs.apply(this, arguments);
        try { report(current()); } catch (e) {}
        return r;
      };
    }
  } catch (e) {}

  // 兜底：万一 magpie 换了写法去动地址栏（或经链接跳转），一秒比对一次全量 URL。
  // 只有真变了才发，代价可以忽略。
  var prevHref = String(location.href);
  setInterval(function(){
    var href = String(location.href);
    if (href === prevHref) return;
    prevHref = href;
    report(current());
  }, 1000);

  // 首屏若地址上就带着页（壳子拼进来的那一份），把它认成「这一趟到的页」，
  // 免得后面第一次换页时把 last 判成空而多发一条。
  // 注意：首屏不发上报。壳子没把上次那一页拼上时（比如读不到 /status），
  // 页面就是默认的 Agent；那会儿若照agent上报，会把用户真正的记忆冲掉。
  last = current();

  // ── 替上游补一处首屏（2026-10-10）────────────────────────────────────────
  // magpie 的每个页模块都在自己文件结尾补了一句「要是地址直接打开的是我这一页，
  // 就自己加载一次」，因为 app.js 的启动段跑得比它们早：
  //   library.js:  // opened on ?view=library: app.js showed the page before this was here
  //                if (!page.hidden) load();
  //   plugins.js:  // opened on the Plugins tab (?view=plugins): app.js showed it before
  //                if (view === "plugins") load();
  //   sessions.js: （没有这一句）
  // 所以被地址直接打开到「会话」时，app.js 那一刻 window.loadSessionsPage 还是
  // undefined（sessions.js 排在 app.js 后面），show() 里的 ?.() 默默跳过，页面
  // 只剩下导航高亮、内容区空着，得手动切走再切回。这里替它补上。
  // 门槛定成「这一页确实一个元素都没有」：已经自己画出来的页（library、plugins
  // 现在都画得出来）不会被多叫一次，也就不会多读一遍数据。
  var LATE_LOADERS = { sessions: "loadSessionsPage", library: "loadLibrary", plugins: "loadPlugins" };
  function healFirstPaint(){
    var v = "";
    try { v = new URLSearchParams(location.search || "").get("view") || ""; } catch (e) {}
    var name = LATE_LOADERS[v];
    if (!name) return;
    var box = document.getElementById("view-" + v);
    if (!box || box.querySelector("*")) return;   // 已经有东西了，不是这一种
    var fn = window[name];
    if (typeof fn !== "function") return;
    try { fn(); } catch (e) {}
  }
  // DOMContentLoaded 在各脚本执行完以后才来，那会儿这些 loader 都已就位
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", healFirstPaint);
  else healFirstPaint();
})();
</script>
`;

function inject(html, vars) {
  let out = html;
  // 主题选择的初始值 + 服务端备好的变量表，直接写进页面。
  // （不依赖卡片里的 fetch：骨架里实测宿主并未给卡片 iframe 带上 hana-theme 参数，
  //  而注入常量这条路径不靠任何网络请求，最稳。）
  const seed =
    `<script id="hana-theme-seed">window.__hanaThemeChoice=${JSON.stringify(state.themeChoice || "auto")};` +
    `window.__hanaThemeList=${JSON.stringify(THEME_OPTIONS)};` +
    // 首屏变量表对应的主题名（auto 时服务端已解析成具体主题名）
    `window.__hanaSeedName=${JSON.stringify(state.themeApplied || "")};` +
    `window.__hanaVars=${JSON.stringify(vars || null)};</script>\n`;
  // ① 垫片必须在 magpie 自己的脚本之前
  if (/<head>/i.test(out)) out = out.replace(/<head>/i, "<head>" + BASE_SHIM + seed);
  else {
    if (/<html([^>]*)>/i.test(out)) out = out.replace(/<html([^>]*)>/i, (m) => m + BASE_SHIM + seed);
    else out = seed + out;
  }
  // ② 主题与隐藏规则放到 head 末尾（app.css 之后）
  const tail = themeBlock() + adaptBlock() + hiddenBlock() + selectBlock() + focusBlock() + importBlock() + THEME_CLIENT + SELECT_CLIENT + IMPORT_CLIENT + VIEW_CLIENT;
  if (/<\/head>/i.test(out)) out = out.replace(/<\/head>/i, tail + "</head>");
  else out += tail;
  state.rewrites += 1;
  return out;
}

function waitingPage(reason) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8">
${THEME_CLIENT}
<style>
  body{margin:0;height:100vh;display:flex;align-items:center;justify-content:center;
       background:var(--bg,#16171b);color:var(--fg,#e8e9ed);
       font:14px/1.6 -apple-system,"Segoe UI",system-ui,sans-serif}
  .w{text-align:center;max-width:440px;padding:24px}
  .s{width:26px;height:26px;margin:0 auto 14px;border-radius:50%;
     border:2px solid var(--line,rgba(255,255,255,.14));border-top-color:var(--accent,#6b7dff);
     animation:sp 900ms linear infinite}
  @keyframes sp{to{transform:rotate(360deg)}}
  .m{color:var(--muted,#8b8d98);font-size:12.5px;margin-top:6px;word-break:break-all}
</style></head><body><div class="w">
  <div class="s"></div><div>magpie 正在启动…</div>
  <div class="m">${reason || "首次启动需要一点时间"}</div>
</div></body></html>`;
}

function sendHtml(res, text) {
  const buf = Buffer.from(text, "utf8");
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Content-Length": String(buf.length) });
  res.end(buf);
}

// 上游不可用时该怎么回。
//
// 这里是「把那片红字生出来」的地方：magpie 的前端拿 /api/* 的响应体直接当正文用，
// 一旦收到整页 HTML，它会把那堆标签原样打印到界面上（用户看到的就是一整屏 HTML）。
// 所以接口路径一律 503 + JSON，前端安安静静当成一次失败；
// 只有真的文档导航才给等待页（那张「magpie 正在启动…」是要给人看的）。
function apiLikePath(path) {
  return /^\/(api|v1|v1beta|gateway)(\/|$)/.test(path);
}
// 接口路径、以及一切不要文档的请求（accept 不含 text/html）→ JSON；
// 只有要文档的（accept 带 text/html，即真正的导航/文档加载）才给等待页。
//
// 为什么不看 sec-fetch-mode：它是浏览器的禁止头，宿主加载卡片文档那一下实测带的是
// `sec-fetch-mode: cors`（不是 navigate），拿它判断会把文档也判成接口，
// 于是等待页变成一坨 JSON。accept 才是稳的那一个。
function wantsJson(req) {
  const p = servicePath((req && req.url) || "/");
  if (apiLikePath(p)) return true;
  const h = (req && req.headers) || {};
  const accept = String(h.accept || "");
  if (accept.indexOf("text/html") >= 0) return false;
  return true;
}
function sendUnavailable(req, res, reason) {
  const why = reason || "magpie 还没就绪";
  const asJson = wantsJson(req);
  // 诊断：降级时到底判成了什么、入站头长什么样。
  // 这张卡出问题时最常被问的就是「为什么这里吐的是 JSON / 为什么吐的是整页 HTML」。
  try {
    const h = (req && req.headers) || {};
    state.diag.push({
      at: new Date().toISOString(), kind: "degraded",
      url: req && req.url, path: servicePath((req && req.url) || "/"),
      accept: h.accept || "", fetchMode: h["sec-fetch-mode"] || "",
      dest: h["sec-fetch-dest"] || "", asJson: asJson, reason: why,
    });
    if (state.diag.length > 80) state.diag.shift();
  } catch { /* 忽略 */ }
  if (asJson) {
    state.lastError = state.lastError || why;
    sendJson(res, { ok: false, error: why, retryAfterMs: 1500 }, 503);
    return;
  }
  sendHtml(res, waitingPage(why === "magpie 还没就绪" ? "" : why));
}

// 宿主把本服务挂在 /api/apps/<id>/routes/_runtime/<rid>[/_surface/<ticket>]/ 下。
// 浏览器对相对资源（app.css / boot.js）会带上这个前缀；magpie 自己只认根路径，
// 原样转发它只会回 404。这里把宿主前缀剥掉，还原成服务内路径再转发。
// 若宿主已经剥过，则正则不匹配，原样返回（no-op），两种情况都对。
function servicePath(url) {
  const m = /^\/api\/apps\/[^/]+\/routes\/_runtime\/[^/]+(?:\/_surface\/[^/]+)?(\/.*)?$/.exec(url);
  return m ? (m[1] || "/") : url;
}

function note(url, status, ct, extra) {
  try {
    state.recent.push({ t: Date.now(), url, path: servicePath(url), status, ct: ct || "", ...(extra || {}) });
    // 300 条：一次卡片加载就有 30~40 个请求，早先的 40 条一圈就被挤掉了，
    // 结果想回看「导航请求到第一个资源之间隔了多久」时已经没数据了。
    if (state.recent.length > 300) state.recent.shift();
  } catch { /* 忽略 */ }
}

// ── 只读接口的短 TTL 缓存 ────────────────────────────────────────────────────
// 为什么要有这一层：magpie 有几个 GET 每次都要重读一遍设置，实测 300~500ms
// （/api/state 407ms、/api/plugins 512ms、/api/providers 393ms、/api/settings 365ms、
//  /api/library 1387ms）。卡片首屏要连着打好几个，加起来 1~2 秒，用户看到的就是
// 「卡片里先转一会儿圈」。最亏的是 /boot.js：只有 81 字节，却写在 <head> 里
// 阻塞首绘，整页都在等它，而它自己还要 400ms 上下。
//
// 做法就是 stale-while-revalidate：命中直接回；过期了也先回旧的，同时在后台刷新。
// 任何非 GET 请求（改设置、开关供应商、登录、装插件）一到，整张表立刻作废，
// 而且写之前、写之后各清一次，免得并发中的回源把旧值又填回去。所以自己动手改过的
// 东西立刻能看到，不会读到旧值。
//
// 不确定的部分说清楚：这些接口的内容多久变一次我们并不知道，TTL 是按「变了也
// 不该超过这几秒才被看到」定的；真正兜住正确性的是「写入即作废」，不是 TTL。
const CACHED_GETS = new Map([
  ["/boot.js", 60 * 1000],        // 只在语言/主题/字号变化时变，而那些都走 POST
  ["/api/settings", 30 * 1000],
  ["/api/state", 3 * 1000],
  ["/api/plugins", 3 * 1000],
  ["/api/providers", 3 * 1000],
  ["/api/library", 3 * 1000],
  // 这两条是「探测本机装了哪些 agent CLI」，每次开页都会打，实测 314~330ms。
  // 内容只在装了/卸了 agent 时变，而那些动作都走 POST，所以 TTL 可以给长一点。
  ["/api/agents/cli", 60 * 1000],
  ["/api/agents/install", 60 * 1000],
]);
const CACHE_MAX_BYTES = 8 * 1024 * 1024;
// 带 ?v=<内容哈希> 的 js/css 改发一年长缓存。想关掉就改成 false。
const USE_IMMUTABLE_ASSETS = true;
const getCache = new Map();     // path -> { at, status, headers, body }
const cacheBusy = new Set();    // 正在后台刷新的 path（同一个只刷一次）

function cachePlanFor(req) {
  if ((req.method || "GET") !== "GET") return null;
  const full = servicePath(req.url || "/");
  const ttl = CACHED_GETS.get(full.split("?")[0]);
  return ttl ? { key: full, ttl } : null;
}

function clearGetCache(why) {
  if (getCache.size === 0) return;
  getCache.clear();
  state.cacheCount = 0;
  state.cacheStats.cleared += 1;
  if (why) log(`只读缓存已作废（${why}）`);
}

function serveCached(res, entry, mark) {
  const headers = { ...entry.headers, "x-hana-cache": mark };
  headers["content-length"] = String(entry.body.length);
  delete headers["transfer-encoding"];
  res.writeHead(entry.status, headers);
  res.end(entry.body);
}

// 自己去上游要一份完整的、未压缩的响应。后台刷新与预热都走这条路。
// 拿不到（非 200、有压缩、太大、断了）就返回 null，缓存保持原样。
function fetchUpstreamOnce(path) {
  return new Promise((resolve) => {
    if (!state.upstreamPort) return resolve(null);
    const req = httpRequest({
      host: UPSTREAM_HOST,
      port: state.upstreamPort,
      method: "GET",
      path,
      headers: {
        host: `${UPSTREAM_HOST}:${state.upstreamPort}`,
        cookie: state.upstreamKey ? `magpie_web_${state.upstreamPort}=${state.upstreamKey}` : "",
        "accept-encoding": "identity",   // 有压缩就不存，见下
      },
    }, (res) => {
      const chunks = [];
      let size = 0;
      res.on("data", (c) => {
        chunks.push(c);
        size += c.length;
        if (size > CACHE_MAX_BYTES) { try { res.destroy(); } catch { /* 忽略 */ } }
      });
      res.on("error", () => resolve(null));
      res.on("end", () => {
        if (res.statusCode !== 200) return resolve(null);
        // 压缩过的存下来没法安全地发给「没要压缩」的下一个请求，索性不存
        if (res.headers["content-encoding"]) return resolve(null);
        if (size > CACHE_MAX_BYTES) return resolve(null);
        const headers = {};
        for (const [k, v] of Object.entries(res.headers)) {
          if (HOP_BY_HOP.has(k.toLowerCase())) continue;
          headers[k.toLowerCase()] = v;
        }
        resolve({ status: 200, headers, body: Buffer.concat(chunks) });
      });
    });
    req.setTimeout(60 * 1000, () => { try { req.destroy(); } catch { /* 忽略 */ } });
    req.on("error", () => resolve(null));
    req.end();
  });
}

function refreshCached(key) {
  if (cacheBusy.has(key)) return;
  cacheBusy.add(key);
  fetchUpstreamOnce(key).then((fresh) => {
    cacheBusy.delete(key);
    if (!fresh) return;
    getCache.set(key, { at: Date.now(), ...fresh });
    state.cacheCount = getCache.size;
    state.cacheStats.refresh += 1;
  }).catch(() => { cacheBusy.delete(key); });
}

// 预热：上游刚就绪时先把这几条拉一遍，卡片第一次打开就是热的。
// 一件一件来（别和首屏抢），失败不影响任何事：请求自己会照常回源。
async function warmGetCache() {
  let n = 0;
  for (const path of CACHED_GETS.keys()) {
    if (state.phase !== "ready" || state.upstreamPort === 0) return;
    if (getCache.has(path)) continue;
    const fresh = await fetchUpstreamOnce(path);
    if (fresh) {
      getCache.set(path, { at: Date.now(), ...fresh });
      state.cacheCount = getCache.size;
      n += 1;
    }
  }
  if (n > 0) log(`只读缓存预热完成：${n} 条（${[...getCache.keys()].join(", ")}）`);
}

// 返回 true 表示这个请求已经被缓存层处理掉了，不用再回源
function handleCachedGet(req, res) {
  const plan = cachePlanFor(req);
  if (!plan) return false;
  const entry = getCache.get(plan.key);
  if (!entry) {
    state.cacheStats.miss += 1;
    refreshCached(plan.key);   // 这次照常回源（慢这一次），顺手把缓存填上
    return false;
  }
  if (Date.now() - entry.at <= plan.ttl) {
    state.cacheStats.hit += 1;
    serveCached(res, entry, "hit");
    note(req.url, entry.status, entry.headers["content-type"] || "", { cache: "hit" });
  } else {
    state.cacheStats.stale += 1;
    serveCached(res, entry, "stale");
    note(req.url, entry.status, entry.headers["content-type"] || "", { cache: "stale" });
    refreshCached(plan.key);
  }
  return true;
}

// ── 反代 ─────────────────────────────────────────────────────────────────────
function proxyRequest(clientReq, clientRes) {
  if (!state.upstreamPort) {
    clientReq.resume();
    sendUnavailable(clientReq, clientRes, state.lastError || "magpie 还没就绪");
    return;
  }
  state.requests += 1;
  // 头五个请求记下全部入站头：诊断「宿主到底把什么转给我」
  if (state.requests <= 5) {
    try {
      state.diag.push({
        at: new Date().toISOString(), kind: "inbound",
        url: clientReq.url, method: clientReq.method,
        headers: clientReq.headers,
      });
      if (state.diag.length > 80) state.diag.shift();
    } catch { /* 忽略 */ }
  }

  const headers = {};
  // 文档请求不能带条件头。同一个页面每次都要重新注入（主题变量表、隐藏规则、
  // 上次停在的页都在里面），而上游的 ETag 会让浏览器拿 If-None-Match 来问，
  // 上游回 304 —— 304 不带正文，浏览器就继续用它自己缓存的那一份旧注入。
  // 实测：卡片里 F5 一下，页面跑的还是上一个版本的上报脚本。
  // 所以只要这一趟要的是文档（accept 里有 text/html），就不把条件头转上去。
  const wantDoc = String((clientReq.headers && clientReq.headers.accept) || "").includes("text/html");
  for (const [k, v] of Object.entries(clientReq.headers)) {
    const lk = k.toLowerCase();
    if (HOP_BY_HOP.has(lk) || lk === "host" || lk === "content-length") continue;
    if (lk === "referer" || lk === "origin") continue;  // 别把跨源上下文传给上游
    if (wantDoc && (lk === "if-none-match" || lk === "if-modified-since")) continue;
    headers[k] = v;
  }
  headers["host"] = `${UPSTREAM_HOST}:${state.upstreamPort}`;
  // webGuard 对所有请求都要 key（不豁免回环），用 cookie 带上
  if (state.upstreamKey) headers.cookie = `magpie_web_${state.upstreamPort}=${state.upstreamKey}`;

  const upReq = httpRequest({
    host: UPSTREAM_HOST,
    port: state.upstreamPort,
    method: clientReq.method,
    path: servicePath(clientReq.url),
    headers,
  }, (upRes) => {
    const ct = String(upRes.headers["content-type"] || "");
    // 上游不给 HTML 页发 content-type 时，靠路径兜底（无扩展名的当页面），
    // 否则会漏掉注入（主题、隐藏、base 垫片全失效）。
    const urlPath = String(clientReq.url || "").split("?")[0].toLowerCase();
    const hasExt = /\.[a-z0-9]{1,8}$/.test(urlPath);
    const isHtml = /text\/html/i.test(ct) || (ct === "" && (!hasExt || urlPath.endsWith(".html")));
    note(clientReq.url, upRes.statusCode || 0, ct, {
      ae: String(clientReq.headers["accept-encoding"] || ""),
      ce: String(upRes.headers["content-encoding"] || ""),
    });

    // 非 HTML 原样透传（连 content-type / content-length / content-encoding 一起），
    // 只对要注入重算的 HTML 剥掉这几个。
    const outHeaders = {};
    for (const [k, v] of Object.entries(upRes.headers)) {
      const lk = k.toLowerCase();
      if (HOP_BY_HOP.has(lk)) continue;
      // 去掉会妨碍我们注入/内嵌的头
      if (lk === "x-frame-options" || lk === "content-security-policy") continue;
      if (isHtml && (lk === "content-encoding" || lk === "content-length" || lk === "content-type")) continue;
      // 文档的验证器一并拿掉：留着它，浏览器就会拿旧 ETag 来问、拿到 304、
      // 然后继续用那份旧注入（上面已经拦了入站条件头，这里是不让它再生一个）。
      if (isHtml && (lk === "etag" || lk === "last-modified" || lk === "expires" || lk === "age")) continue;
      outHeaders[lk] = v;
    }

    if (!isHtml) {
      // 非 HTML：原样透传（含上游的 content-encoding / content-length）。
      // MIME 必须可靠——见上方 MIME 注释；压缩一律不做，见上方「代理不自行压缩」。
      if (!outHeaders["content-type"]) {
        const guess = mimeOf(clientReq.url);
        if (guess) outHeaders["content-type"] = guess;
      }
      // 带内容哈希的静态资源（app.css?v=696bc2…）：magpie 只发 no-cache，
      // 于是浏览器每次打开都要把这十几个文件重新条件请求一遍、校验近 1MB。
      // 哈希即内容（内容一变哈希就变），所以这里改成一年长缓存是安全的。
      if (USE_IMMUTABLE_ASSETS && /[?&]v=[0-9a-f]{6,}/i.test(String(clientReq.url || "")) && /\.(?:js|css)$/i.test(String(clientReq.url || "").split("?")[0])) {
        outHeaders["cache-control"] = "public, max-age=31536000, immutable";
      }
      clientRes.writeHead(upRes.statusCode || 502, outHeaders);
      upRes.pipe(clientRes);
      return;
    }

    // HTML：收完 -> 注入 -> 发出
    const chunks = [];
    let size = 0;
    upRes.on("data", (c) => {
      chunks.push(c);
      size += c.length;
      if (size > 32 * 1024 * 1024) { try { upRes.destroy(); } catch { /* 忽略 */ } }
    });
    upRes.on("end", async () => {
      let text = Buffer.concat(chunks).toString("utf8");
      try {
        // 页面要注入主题变量。变量表从宿主取（取不到就用上一次的缓存）。
        const vars = await themeVars();
        text = inject(text, vars);
      } catch (e) { state.lastError = "inject: " + String(e); }
      let buf = Buffer.from(text, "utf8");
      outHeaders["content-type"] = ct || "text/html; charset=utf-8";
      // 不压缩 HTML。
      // 教训：v0.3.0 为了提速给 HTML 也上了 br/gzip，随后卡片就停在 loading、
      // 一个脚本都不执行（部署在宿主卡片 iframe 里时）。而同样的响应在
      // 「用 node 直接请求代理」时完全正常——因为两者路径不同。
      // 页面 HTML 本来只有 45KB，压不压差别很小，不值得赌。
      delete outHeaders["content-encoding"];
      delete outHeaders["vary"];
      // 注过的 HTML 不进浏览器缓存。
      // 两个理由：① 这份 HTML 里嵌着服务端当前的状态（主题变量表、隐藏规则、
      //    上次停在的页…），缓存住就会拿旧注入去撞新的状态（实测：卡片里 F5
      //    一下，页面跑的还是上一个版本的上报脚本，新的压根没上来）；
      //  ② 上游不给缓存头，浏览器会自己猜一个启发式新鲜度，猜错了不报错、
      //    只静默拿旧的——最难查的那一类。
      // 只影响文档；静态资源不受影响（带内容哈希的那批反而在下面给了长缓存）。
      outHeaders["cache-control"] = "no-store";
      outHeaders["content-length"] = String(buf.length);
      try {
        clientRes.writeHead(upRes.statusCode || 200, outHeaders);
        clientRes.end(buf);
      } catch { /* 忽略 */ }
    });
    upRes.on("error", (e) => {
      state.lastError = "上游读失败：" + (e && e.message);
      try {
        if (!clientRes.headersSent) sendUnavailable(clientReq, clientRes, state.lastError);
        else clientRes.end();
      } catch { /* 忽略 */ }
    });
  });

  upReq.on("error", (e) => {
    const msg = String(e && e.message ? e.message : e);
    // 我们自己掐的超时不算「连不上上游」：慢不等于坏，更不能拿它去触发自愈
    // 重拉（那会在安装中途把 magpie 杀掉）。真正连不上才计数。
    const ours = /上游超时/.test(msg);
    state.lastError = msg;
    log("上游连接失败：" + msg);
    note(clientReq.url, 0, ours ? "upstream-timeout" : "upstream-error");
    if (!ours) noteUpstreamFailure();
    try {
      if (!clientRes.headersSent) sendUnavailable(clientReq, clientRes, "上游连接失败：" + msg);
      else clientRes.end();
    } catch { /* 忽略 */ }
  });
  upReq.setTimeout(upstreamTimeoutFor(clientReq.url), () => {
    try { upReq.destroy(new Error(`上游超时（${Math.round(upstreamTimeoutFor(clientReq.url) / 1000)} 秒无动静）`)); } catch { /* 忽略 */ }
  });

  clientReq.pipe(upReq);
  clientReq.on("error", () => { try { upReq.destroy(); } catch { /* 忽略 */ } });
}

// ── 内部端点 ─────────────────────────────────────────────────────────────────
function readJson(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); } catch { resolve({}); } });
    req.on("error", () => resolve({}));
  });
}

// 上游要等多久才算「没救了」。30 秒对日常够用，但对几件慢事远远不够：
// 第一次装插件要先下 86MB 的 bun（magpie plugin add），检查更新要挨个问 npm，
// 装更新要落盘再重起。这些操作进行时 magpie 是一片安静的，socket 空闲超时
// 会把它误判成断线（实测：30 秒报「上游超时」，装插件就是这么装不上的）。
// 所以：慢活儿给足，其余也从宽。
const SLOW_UPSTREAM_PATHS = [
  "/api/plugins/add", "/api/plugins/remove", "/api/plugins/update", "/api/plugins/upgrade",
  "/api/plugins/check", "/api/plugins/mirror", "/api/plugins/search", "/api/plugin-signin/",
  "/api/update",
];
function upstreamTimeoutFor(path) {
  const p = String(path || "");
  if (SLOW_UPSTREAM_PATHS.some((s) => p.startsWith(s))) return 15 * 60 * 1000;
  return 3 * 60 * 1000;
}

function sendJson(res, obj, status = 200) {
  const buf = Buffer.from(JSON.stringify(obj), "utf8");
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Content-Length": String(buf.length) });
  res.end(buf);
  // 返回 true：内部端点用 `return sendJson(...)` 表示「已处理」，
  // 早先它隐式返回 undefined，会被上层当成未处理，再补一个 404，
  // 结果是「Cannot write headers after they are sent」。
  return true;
}

// ── 生图（够给宿主那边的媒体适配器用）─────────────────────────────────────
// 为什么放在这里而不是适配器里：
//   App 的 ctx.network.fetch 只放行清单 network.allowedHosts 里列过的主机，而清单里
//   只有 127.0.0.1。magpie 的网关（只监听回环）已经能出图，但它把厂商的图以 **URL**
//   交回来（WorkBuddy 给的是腾讯云 CDN 上一条带签名的链接），适配器自己去下就撞白名单：
//     Plugin network.fetch host "…cos.ap-beijing.myqcloud.com" is not declared in manifest network.allowedHosts
//   把 CDN 写进清单等于白名单跟着厂商变，而且每加一个主机都要用户重新审一次。
//   所以改成：本进程（App 申请来的 runtime，申请时就是 network: external，也是 magpie
//   的父进程，本来就整棵树连着外网）跑完整个来回，适配器只跟自己家的 127.0.0.1 说话。
//   下载完把文件直接落到成品目录，只回文件名：几 MB 的图不必再经 JSON 过一遍宿主的
//   门（那条路还受清单 8MiB 响应上限约束）。
const GATEWAY_BASE = "http://127.0.0.1:3425";
const DRAW_TIMEOUT_MS = 5 * 60 * 1000 + 30 * 1000;   // magpie 那边上限 5 分钟
const DRAW_MAX_BYTES = 64 * 1024 * 1024;

function extOfMimeType(mime, fallback) {
  const m = String(mime || "").toLowerCase();
  if (m.includes("jpeg") || m.includes("jpg")) return ".jpg";
  if (m.includes("webp")) return ".webp";
  if (m.includes("gif")) return ".gif";
  if (m.includes("avif")) return ".avif";
  return fallback || ".png";
}

function extOfImageUrl(url) {
  const raw = String(url || "").split("?")[0].split("#")[0];
  const m = /\.[a-z0-9]{2,5}$/i.exec(raw);
  const ext = m ? m[0].toLowerCase() : "";
  return [".png", ".jpg", ".jpeg", ".webp", ".gif", ".avif"].includes(ext) ? ext : ".png";
}

async function fetchTimed(url, init, ms) {
  if (typeof fetch !== "function") {
    throw new Error(`这个 Node 运行时（${process.version}）没有全局 fetch，下不了厂商返回的图片`);
  }
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    return await fetch(url, { ...(init || {}), signal: ac.signal });
  } finally { clearTimeout(t); }
}

async function runDraw(b) {
  const model = String(b.model || "").trim();
  const prompt = String(b.prompt || "").trim();
  if (!model || !prompt) return { ok: false, error: "生图需要 model 与 prompt" };
  const outDir = String(b.outDir || "").trim() || join(process.cwd(), "generated");
  const tmpDir = String(b.tmpDir || "").trim() || join(process.cwd(), "tmp", "draw-" + Date.now().toString(36));

  // 只传 magpie 真认的字段（它读 JSON 时只挑这几个键）。
  const body = { model, prompt };
  for (const k of ["n", "size", "quality", "background", "output_format", "images"]) {
    const v = b[k];
    if (v !== undefined && v !== null && v !== "") body[k] = v;
  }

  const t0 = Date.now();
  let res;
  try {
    res = await fetchTimed(GATEWAY_BASE + "/v1/images/generations", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        // 网关只监听回环、接受任何 token，这个头只决定这次生成记在谁名下。
        authorization: "Bearer magpie-hanako",
        "user-agent": "magpie-hana/1",
        accept: "application/json",
      },
      body: JSON.stringify(body),
    }, DRAW_TIMEOUT_MS);
  } catch (e) {
    const msg = String((e && e.message) || e);
    return { ok: false, error: /abort/i.test(msg) ? "生图超时（magpie 那边上限 5 分钟）" : "连不上 magpie 网关：" + msg };
  }

  const text = await res.text();
  if (!res.ok) {
    let said = text;
    try {
      const ej = JSON.parse(text);
      said = (ej && ej.error && ej.error.message) || (ej && ej.message) || text;
    } catch { /* 原样 */ }
    return { ok: false, error: `magpie 生图失败（HTTP ${res.status}）：${String(said).slice(0, 400)}` };
  }

  let j = null;
  try { j = JSON.parse(text); } catch { return { ok: false, error: `magpie 的回包不是 JSON：${text.slice(0, 200)}` }; }
  const items = Array.isArray(j && j.data) ? j.data : [];
  if (!items.length) {
    return { ok: false, error: `magpie 说成功了但没给图片${j && j.text ? "：" + String(j.text).slice(0, 200) : ""}` };
  }

  mkdirSync(tmpDir, { recursive: true });
  mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const files = [];
  let i = 0;
  for (const it of items) {
    i += 1;
    let buf = null;
    let ext = ".png";
    const b64 = it && typeof it.b64_json === "string" ? it.b64_json : "";
    const url = it && typeof it.url === "string" ? it.url : "";
    if (b64) {
      buf = Buffer.from(b64, "base64");
      ext = extOfMimeType(it.mime_type, ".png");
    } else if (url) {
      let r;
      try {
        r = await fetchTimed(url, { headers: { "user-agent": "magpie-hana/1" } }, 120000);
      } catch (e) {
        return { ok: false, error: `下载厂商返回的图片失败：${String((e && e.message) || e)}` };
      }
      if (!r.ok) return { ok: false, error: `下载厂商返回的图片失败：HTTP ${r.status}` };
      buf = Buffer.from(await r.arrayBuffer());
      ext = extOfMimeType(r.headers && r.headers.get ? r.headers.get("content-type") : "", extOfImageUrl(url));
    }
    if (!buf || !buf.length) continue;
    if (buf.length > DRAW_MAX_BYTES) {
      return { ok: false, error: `图片太大（${Math.round(buf.length / 1048576)}MB），不落盘` };
    }
    const name = `magpie-${stamp}-${i}${ext}`;
    // 先落本次任务自己的暂存目录，下完了再发布到成品目录：
    // 宿主在成品目录里扫新文件，半张图不该被它看见（同卷 rename，原子）。
    writeFileSync(join(tmpDir, name), buf);
    renameSync(join(tmpDir, name), join(outDir, name));
    files.push(name);
  }
  if (!files.length) return { ok: false, error: "图片拿到了但没能落盘" };

  const ms = Date.now() - t0;
  log(`生图完成：${model} → ${files.join(", ")}（${(ms / 1000).toFixed(1)}s）`);
  return { ok: true, model: j.model || model, files, usage: j.usage || null, ms };
}

async function handleInternal(req, res) {
  const path = (req.url || "/").split("?")[0];

  if (path === "/_hana/diag") {
    if (req.method === "POST") {
      const b = await readJson(req);
      try {
        state.diag.push({ at: new Date().toISOString(), kind: "page", ...b });
        if (state.diag.length > 80) state.diag.shift();
      } catch { /* 忽略 */ }
      sendJson(res, { ok: true });
      return true;
    }
    sendJson(res, { ok: true, count: state.diag.length, diag: state.diag.slice(-30) });
    return true;
  }

  // 生图：适配器把参数交给这里，整段来回（网关 + 下载 + 落盘）都在本进程完成。
  if (path === "/_hana/draw" && req.method === "POST") {
    const b = await readJson(req);
    try {
      const out = await runDraw(b);
      return sendJson(res, out, out.ok ? 200 : 502);
    } catch (e) {
      return sendJson(res, { ok: false, error: String((e && e.message) || e) }, 502);
    }
  }

  // 「从本机导入用量信息」（magepie 设置页 → 同步与备份最下面那一行调的）。
  // 先停 magpie：这些表它在内存里也存着一份，不停就白改；并完再起回来。
  if (path === "/_hana/import-usage" && req.method === "POST") {
    if (state.importing) return sendJson(res, { ok: false, error: "已经在导入了，等这次结束再点" }, 409);
    const b = await readJson(req);
    const apply = b.apply === true;
    state.importing = true;
    try {
      const home = join(state.exeCwd || process.cwd(), "data");
      const mod = await import("./import-usage.mjs");
      const found = mod.findSource(home, realUserEnv() || {});
      if (!found.length) {
        return sendJson(res, {
          ok: false,
          error: "本机没找到另一份 magpie 的家目录（找过 ~/.config/magpie、%APPDATA%\\magpie、%LOCALAPPDATA%\\magpie、~/.magpie）。"
            + "本 App 用的是便携目录，不在那几个位置；如果你确实还有一份装在别处，把它告诉我。",
        }, 404);
      }
      const src = found[0].dir;
      if (!apply) {
        const rep = mod.importUsage({ srcDir: src, homeDir: home, apply: false, log: (m) => log("（演练）" + m) });
        return sendJson(res, { ok: true, dry: true, report: rep });
      }
      const wasRunning = !!(state.magpiePid && pidAlive(state.magpiePid));
      log(`导入用量：${src} → ${home}（先停 magpie）`);
      stopMagpie();
      await new Promise((r) => setTimeout(r, 1200));
      let rep = null;
      let bad = null;
      try { rep = mod.importUsage({ srcDir: src, homeDir: home, apply: true, log }); }
      catch (e) { bad = e; }
      clearGetCache("导入用量后");
      if (wasRunning) await startMagpie({ exe: state.exePath, cwd: state.exeCwd });
      if (bad) return sendJson(res, { ok: false, error: String((bad && bad.message) || bad) }, 500);
      log(`导入完成：新增 ${rep.added} 条，备份 ${rep.backup}`);
      return sendJson(res, { ok: true, report: rep });
    } catch (e) {
      return sendJson(res, { ok: false, error: String((e && e.message) || e) }, 500);
    } finally {
      state.importing = false;
    }
  }

  if (path === "/_hana/status") {
    sendJson(res, {
      ok: true, phase: state.phase, port: state.listenPort,
      upstreamPort: state.upstreamPort, magpiePid: state.magpiePid,
      error: state.lastError, hidden: [...state.hidden],
      requests: state.requests, rewrites: state.rewrites,
      recent: state.recent.slice(-30),
      theme: state.themeAttr,
      themeChoice: state.themeChoice,
      themeApplied: state.themeApplied,
      themeVarsCached: !!(themeCache && themeCache.vars),
      view: state.view || readView(),
      diagCount: state.diag.length,
      cache: { ...state.cacheStats, entries: state.cacheCount, paths: [...CACHED_GETS.keys()] },
      env: envSnapshot(),
    });
    return true;
  }

  // 主题选择（auto / 具体主题名）。GET 给注入的客户端脚本读，POST 由设置页改。
  if (path === "/_hana/theme" && req.method === "GET") {
    const want = new URL(req.url || "/", "http://x").searchParams.get("theme");
    if (want !== null) {
      // 带 theme（即使是空串/ auto）都当「要变量表」处理。
      // 空串/auto -> 按宿主当前主题解析（fetchThemeCss 内部会做）。
      const r = await fetchThemeCss(want);
      if (!r.ok) return sendJson(res, { ok: false, error: r.error || "取主题失败" }, 502);
      const vars = parseThemeVars(r.css);
      // 宿主对某些名字（例如旧版里的 light/dark）会返回一份不含变量的空壳，
      // 这时明确报错，别让调用方拿到一个看似成功的空表。
      if (!vars["--bg"] && !vars["--text"]) {
        return sendJson(res, { ok: false, error: `主题 ${r.theme} 没有可用变量（是不是不是 Hana 主题？）`, theme: r.theme }, 502);
      }
      return sendJson(res, { ok: true, theme: r.theme, vars });
    }
    sendJson(res, { ok: true, theme: state.themeChoice, applied: state.themeApplied, appearance: state.themeAttr });
    return true;
  }

  if (path === "/_hana/theme" && req.method === "POST") {
    const b = await readJson(req);
    if (typeof b.css === "string") state.themeCss = b.css;   // 保留兼容
    if (typeof b.appearance === "string") state.themeAttr = b.appearance;
    if (typeof b.theme === "string") {
      // 允许把 choice 设回 auto（早先 `&& b.theme` 会把 auto 当假值漏掉？不会——
      // "auto" 是真值。这里只改成不强求非空，以便未来万一要清空。
      state.themeChoice = b.theme || "auto";
      themeCache = { at: 0, key: null, vars: null };   // 换了主题：缓存作废
    }
    sendJson(res, { ok: true, theme: state.themeChoice, appearance: state.themeAttr });
    return true;
  }

  // 卡片脚本轮询用：宿主当前的配色三件套（当前 / 浅色 / 深色）。
  // 为什么需要它：宿主（当前版本）不会主动向卡片推 hana.theme.changed，
  // 而卡片 iframe 的 URL 参数只是挂载当刻的快照。用户随后在 Hana 里换主题，
  // 只有这个端点能反映出来。
  if (path === "/_hana/host-theme") {
    // choice：卡片里的「外观」下拉与设置页的主题下拉共用同一个选择，
    // 所以把这个值一并给页面，它才能把标签对到当前那一项。
    sendJson(res, { ok: true, choice: state.themeChoice || "auto", ...hostThemeInfo() });
    return true;
  }

  // 上次停在哪一页：GET 给卡片壳读（它拼进 iframe 地址），POST 由页面换页时上报。
  // 值走查询串也走 body —— 上报用的是 fetch(url?view=…, {method:"POST"})，
  // 不写 body 更省事，也避开预检。
  if (path === "/_hana/view") {
    if (req.method === "POST") {
      const want = new URL(req.url || "/", "http://x").searchParams.get("view");
      const b = want === null ? await readJson(req) : {};
      const v = validView(want || (b && b.view));
      if (v) { state.view = v; saveView(v); }
      sendJson(res, { ok: !!v, view: state.view });
      return true;
    }
    sendJson(res, { ok: true, view: state.view || readView() });
    return true;
  }

  if (path === "/_hana/hide" && req.method === "POST") {
    const b = await readJson(req);
    if (Array.isArray(b.hidden)) state.hidden = new Set(b.hidden.map(String));
    sendJson(res, { ok: true, hidden: [...state.hidden] });
    return true;
  }

  if (path === "/_hana/quit" && req.method === "POST") {
    sendJson(res, { ok: true });
    setTimeout(shutdown, 50);
    return true;
  }

  return false;
}

// ── 启停 magpie ──────────────────────────────────────────────────────────────
// magpie 会自更新：它把自己换掉、在新端口重新起来（我们用 --addr 127.0.0.1:0，
// 端口本来就是随机的）。这一整块的职责就是「别把上游指丢」。
// 三道路径，从便宜到狠：
//   ① 它自己打出来的 `magpie web on …` 行：每一次都认，端口变了就换；
//   ② 子进程退出：过一会儿重拉（有次数上限）；
//   ③ 连着连不上上游且子进程还活着：先请它退场再重拉。
// 宿主给 App 运行时的环境是沙箱的：USERPROFILE/HOME/APPDATA/LOCALAPPDATA 全指向
// app-data/<id>/.runtime-tmp（实测），ENV 只有 20 个。
//
// 对 magpie 这很致命：它判断「本机装了哪些 agent」，靠的就是拿用户目录去拼各家配置文件
// 的路径（~/.codex/config.toml、~/.claude/…、%APPDATA%/Code/…）。同一个 exe，
// 在真环境里认 9 个，在沙箱里只认 1 个 —— 用户在卡片里看到的永远是「没识别到」。
//
// 这里把真实用户目录还原出来（HOMEDRIVE+HOMEPATH 未被沙箱改过，是真的），
// 只覆盖这几个目录变量，其余环境照旧继承。认不出来就不动，宁可不全也不乱改。
function realUserEnv() {
  try {
    const cur = String(process.env.USERPROFILE || "");
    const looksSandbox = cur.includes("\\.runtime-tmp") || cur.includes("/.runtime-tmp");
    const fromParts = (process.env.HOMEDRIVE || "") + (process.env.HOMEPATH || "");
    let home = "";
    if (fromParts && existsSync(fromParts)) home = fromParts;
    if (!home && !looksSandbox && cur && existsSync(cur)) home = cur;
    if (!home && looksSandbox) {
      // 兜底：从沙箱路径里反切出真实家目录（…\Users\SSS\.hanako\… -> …\Users\SSS）
      const cut = cur.indexOf("\\.hanako");
      if (cut > 0) {
        const guess = cur.slice(0, cut);
        if (existsSync(guess)) home = guess;
      }
    }
    if (!home) return null;
    const env = { ...process.env };
    env.USERPROFILE = home;
    env.HOME = home;
    const roaming = join(home, "AppData", "Roaming");
    const local = join(home, "AppData", "Local");
    if (existsSync(roaming)) env.APPDATA = roaming;
    if (existsSync(local)) env.LOCALAPPDATA = local;
    delete env.XDG_CONFIG_HOME;   // 别让它把配置写到沙箱之外的地方
    return env;
  } catch {
    return null;
  }
}

function pidAlive(pid) {
  if (!pid) return false;
  try { process.kill(pid, 0); return true; } catch { return false; }
}

// 端口是否有人应答（TCP 连接就够，不需要读内容）。
function portAnswers(port, timeoutMs = 900) {
  return new Promise((resolve) => {
    if (!port) return resolve(false);
    let settled = false;
    const done = (v) => { if (settled) return; settled = true; try { sock.destroy(); } catch { /* 忽略 */ } resolve(v); };
    const sock = connect({ host: "127.0.0.1", port });
    sock.setTimeout(timeoutMs, () => done(false));
    sock.once("connect", () => done(true));
    sock.once("error", () => done(false));
  });
}

// 我们那只子进程退出后、准备重拉之前，先确认「是不是它自己重起了一只」。
// magpie 装完更新会把自己重起，参数跟原来一样 —— 所以钉住的端口一会儿就有人应答。
// 早先没这一步，就会出现两只同源 magpie 抢同一个 data/（实测：更新完真的变成两只）。
async function adoptIfServing(port) {
  for (let i = 0; i < 6; i++) {
    if (await portAnswers(port)) {
      state.upstreamPort = port;
      state.phase = "ready";
      state.lastError = null;
      state.upstreamFailures = 0;
      state.respawnCount = 0;
      clearGetCache("接回重起后的实例");   // 换了进程，旧响应不能再用
      log(`端口 ${port} 已有人在服务（自更新后的重起），接回来，不再重拉`);
      process.stdout.write(`MAGPIE_UPSTREAM_READY port=${port}\n`);
      warmGetCache();
      return true;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

// 找一个空闲端口钉住。为什么不用 0：随机端口在自更新重起后会变，
// 而那次重起不是我们的子进程，我们看不到它新报的端口。钉住就不会丢。
function pickFreePort() {
  return new Promise((resolve) => {
    const s = createServer();
    s.once("error", () => resolve(0));
    s.listen(0, "127.0.0.1", () => {
      const port = s.address().port;
      s.close(() => resolve(port));
    });
  });
}

let respawnTimer = null;
function scheduleRespawn(delayMs) {
  if (respawnTimer || state.phase === "stopping") return;
  if (state.respawnCount >= 6) {
    state.phase = "error";
    state.lastError = "magpie 反复退出，已停止自动重拉（可在设置页里手动启动）";
    log(state.lastError);
    return;
  }
  const delay = typeof delayMs === "number" ? delayMs : Math.min(8000, 1500 * (state.respawnCount + 1));
  respawnTimer = setTimeout(() => {
    respawnTimer = null;
    if (state.phase === "stopping") return;
    if (pidAlive(state.magpiePid)) return;   // 已经有一只活着了，别拉第二只
    state.respawnCount += 1;
    log(`重新拉起 magpie（第 ${state.respawnCount} 次）`);
    startMagpie({ exe: state.exePath, cwd: state.exeCwd });
  }, delay);
  try { respawnTimer.unref?.(); } catch { /* 忽略 */ }
}

// 上游连着连不上：我们守着的那个端口上已经没人了。
// 阈值放宽一点，免得 magpie 自更新时那几秒的拒绝就把一只健康的实例请下场。
function noteUpstreamFailure() {
  state.upstreamFailures = (state.upstreamFailures || 0) + 1;
  if (state.upstreamFailures < 6) return;
  state.upstreamFailures = 0;
  if (state.phase === "stopping") return;
  log("上游连续拒绝连接，判定为失联，重新拉起 magpie");
  state.upstreamPort = 0;
  state.upstreamKey = "";
  const pid = state.magpiePid;
  state.magpiePid = null;                    // 先置空：它退场时就不再重复排重拉
  if (pid && pidAlive(pid)) {
    try { process.kill(pid, "SIGTERM"); } catch { /* 忽略 */ }
    const t = setTimeout(() => { try { process.kill(pid, "SIGKILL"); } catch { /* 忽略 */ } }, 2500);
    t.unref?.();
  }
  state.phase = "waiting-magpie";
  scheduleRespawn(600);
}

function stopMagpie() {
  const pid = state.magpiePid;
  state.magpiePid = null;
  if (!pid) return;
  try { process.kill(pid, "SIGTERM"); } catch { /* 已退出 */ }
  const t = setTimeout(() => { try { process.kill(pid, "SIGKILL"); } catch { /* 已退出 */ } }, 3000);
  t.unref?.();
}

async function startMagpie({ exe, cwd }) {
  if (!exe || !existsSync(exe)) {
    state.phase = "error";
    state.lastError = `magpie 可执行文件不存在：${exe}`;
    log(state.lastError);
    return;
  }
  state.exePath = exe;   // 重拉时要用（自更新后 exe 还是这个路径，内容已经是新的）
  state.exeCwd = cwd;
  state.exePath = exe;   // 重拉时要用（自更新后 exe 还是这个路径，内容已经是新的）
  state.exeCwd = cwd;
  try { mkdirSync(join(cwd, "data"), { recursive: true }); } catch { /* 忽略 */ }

  if (!state.upstreamPortFixed) state.upstreamPortFixed = await pickFreePort();
  if (!state.webKey) state.webKey = randomBytes(24).toString("base64url");
  const addr = state.upstreamPortFixed ? `127.0.0.1:${state.upstreamPortFixed}` : "127.0.0.1:0";
  const args = ["web", "--addr", addr, "--no-open"];
  const childEnv = realUserEnv();
  const env = { ...(childEnv || process.env), MAGPIE_WEB_KEY: state.webKey };
  log(`spawn: ${exe} ${args.join(" ")}  (cwd=${cwd})`);
  if (childEnv) log(`传给 magpie 的用户目录：${childEnv.USERPROFILE}（宿主给的是 ${process.env.USERPROFILE || ""}）`);
  else log("没认出真实的用户目录，按宿主给的环境启动（agent 探测可能不全）");
  state.upstreamKey = state.webKey;   // key 固定，重起后照样能转发

  const child = spawn(exe, args, {
    cwd,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    env,
  });
  const spawnedAt = Date.now();
  state.magpiePid = child.pid;
  state.phase = "waiting-magpie";

  let buffer = "";
  const onChunk = (chunk) => {
    buffer += chunk.toString("utf8");
    const clean = buffer.replace(ANSI, "");
    const m = clean.match(/magpie web on\s+https?:\/\/[\d.]+:(\d+)\/\?k=([^\s]+)/);
    // 每一条公告都认：它自更新后会换个端口重新起来，只认第一次的话，
    // 代理就永远指着那个已经没人听的旧端口（卡片上就是「上游连接失败」）。
    if (m && (parseInt(m[1], 10) !== state.upstreamPort || m[2] !== state.upstreamKey)) {
      const moved = state.upstreamPort !== 0;
      state.upstreamPort = parseInt(m[1], 10);
      state.upstreamKey = m[2];
      if (moved) clearGetCache("上游换了一只");   // 自更新重起后是另一个进程
      state.phase = "ready";
      state.lastError = null;
      state.upstreamFailures = 0;
      state.respawnCount = 0;      // 跑起来一只稳定的，计数归零
      log(`${moved ? "上游端口变更" : "上游就绪"}：127.0.0.1:${state.upstreamPort}（key 已取得）`);
      process.stdout.write(`MAGPIE_UPSTREAM_READY port=${state.upstreamPort}\n`);
      warmGetCache();   // 先把首屏要用的几条填上，卡片第一次打开就是热的
    }
    if (clean.length > 40000) buffer = clean.slice(-8000);
  };
  child.stdout.on("data", onChunk);
  child.stderr.on("data", onChunk);

  child.on("exit", async (code, sig) => {
    if (state.magpiePid !== child.pid) return;
    state.magpiePid = null;
    if (state.phase === "stopping") { state.upstreamPort = 0; return; }
    state.lastError = `magpie 退出（code=${code} signal=${sig}）`;
    log(state.lastError);
    // 跑得太短就退出：多半是钉住的端口被占了（或 exe 有问题）。
    // 把端口和 key 的钉子放松，给下一次一个重新选的机会，别在一个坏端口上死循环。
    if (Date.now() - spawnedAt < 4000) {
      log("它起来后很快就退出，下次重新选端口");
      state.upstreamPortFixed = 0;
      state.respawnCount = 0;
    }
    const port = state.upstreamPortFixed || state.upstreamPort;
    state.upstreamPort = 0;
    clearGetCache("上游退出了");
    if (await adoptIfServing(port)) return;   // 是它自己重起的，接回来
    state.phase = "error";
    scheduleRespawn();
  });
  child.on("error", (e) => {
    state.phase = "error";
    state.lastError = `spawn 失败：${e && e.message}`;
    log(state.lastError);
  });
}

// ── 主流程 ───────────────────────────────────────────────────────────────────
let server = null;

function shutdown() {
  state.phase = "stopping";
  stopMagpie();
  try { server && server.close(); } catch { /* 忽略 */ }
  const t = setTimeout(() => process.exit(0), 300);
  t.unref?.();
}

function main() {
  const opts = { exe: "", cwd: "", port: 0, marker: "", hidden: null, theme: null, view: "" };
  for (const a of process.argv.slice(2)) {
    const i = a.indexOf("=");
    if (i < 0) continue;
    const k = a.slice(0, i), v = a.slice(i + 1);
    if (k === "--exe") opts.exe = v;
    else if (k === "--cwd") opts.cwd = v;
    else if (k === "--port") opts.port = parseInt(v, 10) || 0;
    else if (k === "--marker") opts.marker = v;
    else if (k === "--hidden") opts.hidden = v;
    else if (k === "--theme") opts.theme = v;
    else if (k === "--view") opts.view = v;
  }
  if (opts.hidden !== null) {
    state.hidden = new Set(opts.hidden.split(",").map((s) => s.trim()).filter(Boolean));
  }
  if (opts.theme) state.themeChoice = opts.theme;
  // 上次停在的页：App 启动时把它随参数带进来（它读的是同一份 view.json）；
  // 没带就自己读一遍。记在内存里，GET /_hana/view 与 /_hana/status 都拿它答。
  state.view = validView(opts.view) || readView();
  if (opts.view && validView(opts.view)) saveView(state.view);
  log(`上次停在的页：${state.view || "（还没记过）"}`);
  state.serverTheme = readHanaTheme();
  log(`Hana 主题（服务端读）：${state.serverTheme || "（未读到）"}`);

  server = createServer((req, res) => {
    // 宿主把本服务挂在带前缀的路径下，而客户端脚本（我们的探针、主题变量请求）
    // 也会把前缀拼上，于是 /_hana/xxx 会变成 /api/apps/…/_runtime/…/_hana/xxx。
    // 早先只认「原样以 /_hana/ 开头」，这些请求就都被当成普通请求转给了上游（404），
    // 结果是：页面里的诊断探针全是空的，客户端取主题变量的请求也落空。
    // 所以先按服务内路径归一化再判断。
    const mountedPath = servicePath(req.url || "/");
    if (mountedPath.startsWith("/_hana/")) {
      req.url = mountedPath;
      // 必须自己 .catch：Node 15+ 里未处理的 Promise rejection 默认会让进程
      // 直接退出。内部端点里只要有一处抛出（比如取主题时的 httpRequest 同步抛），
      // 整个代理就会死——而外表看上去只是“某个请求断了”，很难定位。
      handleInternal(req, res).then((handled) => {
        if (!handled) sendJson(res, { ok: false, error: "unknown internal endpoint" }, 404);
      }).catch((e) => {
        state.lastError = "内部端点异常：" + (e && e.message ? e.message : String(e));
        log(state.lastError);
        try {
          if (!res.headersSent) sendJson(res, { ok: false, error: state.lastError }, 500);
          else res.end();
        } catch { /* 忽略 */ }
      });
      return;
    }
    try {
      // 写请求：缓存立刻作废。清两次——写之前一次（免得并发中的读把旧值填回来），
      // 写之后再一次（这次是真正的失效点）。
      if (req.method !== "GET" && req.method !== "HEAD") {
        clearGetCache(req.method + " " + servicePath(req.url || "/"));
        res.on("close", () => clearGetCache("写入完成"));
      }
      if (!handleCachedGet(req, res)) proxyRequest(req, res);
    } catch (e) {
      state.lastError = "转发异常：" + (e && e.message ? e.message : String(e));
      log(state.lastError);
      try {
        if (!res.headersSent) sendUnavailable(req, res, state.lastError);
        else res.end();
      } catch { /* 忽略 */ }
    }
  });

  // 整个进程的兵底：任何漏网的 rejection / 异常都不要让代理静默死掉。
  // 宁可留一条日志与 lastError，也别再出现「请求突然 ECONNRESET」这种现场。
  process.on("unhandledRejection", (e) => {
    state.lastError = "unhandledRejection：" + (e && e.message ? e.message : String(e));
    log(state.lastError);
  });
  process.on("uncaughtException", (e) => {
    state.lastError = "uncaughtException：" + (e && e.message ? e.message : String(e));
    log(state.lastError);
  });
  server.on("error", (e) => {
    state.phase = "error";
    state.lastError = `代理监听失败（端口 ${opts.port}）：${e && e.message}`;
    log(state.lastError);
    process.stdout.write(`MAGPIE_PROXY_ERROR ${state.lastError}\n`);
    // 退出码 7 = 「端口被占」（本仓 comfyui-hana 的同类约定）。
    // App 会据此换一个端口重来（见 index.js 的 ensureStarted）。
    process.exit(e && e.code === "EADDRINUSE" ? 7 : 1);
  });

  server.listen(opts.port, "127.0.0.1", () => {
    state.listenPort = server.address().port;
    process.stdout.write(`MAGPIE_PROXY_READY port=${state.listenPort}\n`);
    if (opts.marker) process.stdout.write(`${opts.marker}\n`);
    log(`代理监听 127.0.0.1:${state.listenPort}`);
    startMagpie({ exe: opts.exe, cwd: opts.cwd });
  });

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
  process.on("disconnect", shutdown);
  process.on("exit", () => { try { stopMagpie(); } catch { /* 忽略 */ } });
}

main();
