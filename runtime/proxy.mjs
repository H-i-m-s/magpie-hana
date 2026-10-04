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
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
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
  hidden: new Set(DEFAULT_HIDDEN),
  themeAttr: "",
  themeChoice: "auto",   // 主题设置：auto | 具体主题名（青夜/暖纸/…）
  themeCss: "",
  serverTheme: "",       // Hana 当前主题名（服务端从 preferences.json 读，作 auto 的兜底）
  themeApplied: "",      // 实际生效的主题（诊断用）
  lastError: null,
  phase: "starting",   // starting | waiting-magpie | ready | error | stopping
  requests: 0,
  rewrites: 0,
  magpiePid: null,
  diag: [],              // 诊断：入站原始请求头 + 卡片内部上报的页面上下文
};

// HANA_HOME/user/preferences.json 里的 appearance.theme。
// 为什么服务端要读：卡片 iframe 里拿主题名有两条路——读父窗口的 data-theme、
// 或宿主在 URL 上带 hana-theme 参数。实测本 App 的服务挂载模式下两者都不保证。
// 所以再配一条完全不依赖网络与父窗口的：进程启动时自己读一次。
// 代价是用户换了 Hana 主题后，服务端这份要等下一次 reload 才更新
// （卡片侧的 MutationObserver 会先跟上，两者互为兜底）。
function readHanaTheme() {
  try {
    const dataDir = process.cwd();                    // …\app-data\magpie-hana
    const hanaHome = dirname(dirname(dataDir));       // …\.hanako
    const f = join(hanaHome, "user", "preferences.json");
    if (!existsSync(f)) return "";
    const j = JSON.parse(readFileSync(f, "utf8"));
    const t = j && j.appearance && typeof j.appearance.theme === "string" ? j.appearance.theme : "";
    return t && t !== "auto" ? t : "";
  } catch {
    return "";
  }
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
    const themeName = name || readHanaTheme();
    const q = themeName ? `?theme=${encodeURIComponent(themeName)}` : "";
    const req = httpRequest(
      { host: "127.0.0.1", port, path: `/api/apps/theme.css${q}`, method: "GET", timeout: 6000 },
      (r) => {
        if (r.statusCode !== 200) { r.resume(); return resolve({ ok: false, error: `宿主返回 ${r.statusCode}` }); }
        const chunks = [];
        r.on("data", (c) => chunks.push(c));
        r.on("end", () => resolve({ ok: true, theme: themeName || "(宿主默认)", css: Buffer.concat(chunks).toString("utf8") }));
      });
    req.on("timeout", () => { try { req.destroy(); } catch { /* 忽略 */ } resolve({ ok: false, error: "取主题超时" }); });
    req.on("error", (e) => resolve({ ok: false, error: String(e && e.message || e) }));
    req.end();
  });
}

const log = (m) => process.stderr.write(`[magpie-hana] ${m}\n`);

// ── 主题：在客户端按宿主当前主题取色 ────────────────────────────────────────
// 为什么不在这里硬编码色号：Hana 的主题是用户可换的命名主题（青夜 / 暖纸 /
// 高对比 …），色号写死在代理里，一换主题就错。改成注入一段脚本，向宿主
// /api/apps/theme.css 取「当前主题」的真实变量，再映射到 magpie 的变量名。
// 用 !important 提升特异性，压过 magpie 自己的 :root / :root[data-theme]。
const THEME_CLIENT = `<script id="hana-theme-client">
(function(){
  var STYLE_ID = "hana-theme";
  // 需要从宿主取的变量（Hana 的主题用这套命名）
  var WANT = ["--bg","--bg-card","--bg-glass","--sidebar-bg","--accent","--accent-hover",
              "--text","--text-light","--text-muted","--border","--shadow",
              "--green","--coral","--danger","--pop-bg"];

  // 只从「祖先窗口」读，不读自己。
  // 卡片里本页是挂在宿主源下的 iframe，parent 就是 Hana（同源可读）；
  // 直接访问代理时 parent 就是自己，此时必须跳过——否则会把 magpie 自己的默认色
  // 当作「宿主主题」再套回去（自我引用）。
  function hostVars(){
    var w = window;
    try { if (!w.parent || w.parent === w) return null; } catch (e) { return null; }
    // 从 parent 起逐层向上找第一个同源、能读到变量的窗口
    for (var k = 0; k < 6; k++) {
      try {
        if (!w.parent || w.parent === w) break;
        w = w.parent;
      } catch (e) { break; }
      try {
        var d = w.document;
        if (!d || !d.documentElement) continue;
        var comp = w.getComputedStyle(d.documentElement), vars = {};
        for (var j = 0; j < WANT.length; j++) {
          var v = comp.getPropertyValue(WANT[j]);
          if (v && v.trim()) vars[WANT[j]] = v.trim();
        }
        if (vars["--bg"] || vars["--text"]) return vars;
      } catch (e) { /* 跨源：继续向上，或放弃 */ }
    }
    return null;
  }

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
    // 宿主变量名优先，自己那套（--fg / --card）兼容处理
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
    // 自定义属性带 !important：压过 magpie 自己的 :root / :root[data-theme]（实测它没有 !important）
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
    try {
      if (document.documentElement.getAttribute("data-theme") !== built.scheme)
        document.documentElement.setAttribute("data-theme", built.scheme);
    } catch (e) {}
  }

  // 主题变量表由服务端备好（它是从宿主取的，且能看真实配置），
  // 卡片脚本只负责套用。这样两种环境（卡片内 / 直接访问）行为一致。
  if (window.__hanaVars && typeof window.__hanaVars === "object") applyVars(window.__hanaVars);

  // auto 时优先读宿主已生效的计算值（同一文档树，最准，换主题能当场上跟着变）；
  // 读不到就用服务端给的那份（直接访问代理、跨源时就是这样）。
  // 固定主题：服务端给的已经是准的，不折腾。
  function refresh(){
    if ((window.__hanaThemeChoice || "auto") !== "auto") return;
    var v = hostVars();
    if (v) applyVars(v);
  }

  refresh();
  try {
    var top = window;
    while (top && top.parent && top.parent !== top) top = top.parent;
    if (top && top.document && top.document.documentElement && window.MutationObserver) {
      new MutationObserver(function(){ refresh(); })
        .observe(top.document.documentElement, { attributes: true, attributeFilter: ["data-theme", "class", "style"] });
    }
  } catch (e) {}

  // ── 避让宿主卡片右上角悬浮的按钮簇 ───────────────────────────
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
          if (r.bottom < fTop) continue;                   // 整块在本视口上方：无关
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
    // 宿主的悬浮层可能比本页面晚一点布局好，补两次复核
    setTimeout(run, 700);
    setTimeout(run, 2200);
  })();

  // auto 且服务端给的是兜底值时，定期复核（宿主换主题时卡里的观察器先跟上）
  setInterval(refresh, 15000);
})();
</script>
`;

// 主题变量表：从宿主取一次，缓存起来。
// auto 时主题名可能被用户改，缓存给个短 TTL；固定主题则永远不变。
let themeCache = { at: 0, choice: null, vars: null };

async function themeVars() {
  const choice = state.themeChoice || "auto";
  const ttl = choice === "auto" ? 10_000 : 60 * 60 * 1000;
  const fresh = themeCache.vars && themeCache.choice === choice && (Date.now() - themeCache.at) < ttl;
  if (fresh) return themeCache.vars;

  const r = await fetchThemeCss(choice === "auto" ? "" : choice);
  if (!r.ok) return themeCache.vars || null;   // 取不到就用旧的，不把页面搞硬
  const vars = parseThemeVars(r.css);
  if (!vars || !(vars["--bg"] || vars["--text"])) return themeCache.vars || null;
  themeCache = { at: Date.now(), choice, vars };
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

function inject(html, vars) {
  let out = html;
  // 主题选择的初始值 + 服务端备好的变量表，直接写进页面。
  // （不依赖卡片里的 fetch：骨架里实测宿主并未给卡片 iframe 带上 hana-theme 参数，
  //  而注入常量这条路径不靠任何网络请求，最稳。）
  const seed =
    `<script id="hana-theme-seed">window.__hanaThemeChoice=${JSON.stringify(state.themeChoice || "auto")};` +
    `window.__hanaVars=${JSON.stringify(vars || null)};</script>\n`;
  // ① 垫片必须在 magpie 自己的脚本之前
  if (/<head>/i.test(out)) out = out.replace(/<head>/i, "<head>" + BASE_SHIM + seed);
  else {
    if (/<html([^>]*)>/i.test(out)) out = out.replace(/<html([^>]*)>/i, (m) => m + BASE_SHIM + seed);
    else out = seed + out;
  }
  // ② 主题与隐藏规则放到 head 末尾（app.css 之后）
  const tail = themeBlock() + adaptBlock() + hiddenBlock() + THEME_CLIENT;
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
    if (state.recent.length > 40) state.recent.shift();
  } catch { /* 忽略 */ }
}

// ── 反代 ─────────────────────────────────────────────────────────────────────
function proxyRequest(clientReq, clientRes) {
  if (!state.upstreamPort) {
    clientReq.resume();
    sendHtml(clientRes, waitingPage(state.lastError || ""));
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
  for (const [k, v] of Object.entries(clientReq.headers)) {
    const lk = k.toLowerCase();
    if (HOP_BY_HOP.has(lk) || lk === "host" || lk === "content-length") continue;
    if (lk === "referer" || lk === "origin") continue;  // 别把跨源上下文传给上游
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
    // 只对要注入重算的 HTML 剥掉这三个。
    const outHeaders = {};
    for (const [k, v] of Object.entries(upRes.headers)) {
      const lk = k.toLowerCase();
      if (HOP_BY_HOP.has(lk)) continue;
      // 去掉会妨碍我们注入/内嵌的头
      if (lk === "x-frame-options" || lk === "content-security-policy") continue;
      if (isHtml && (lk === "content-encoding" || lk === "content-length" || lk === "content-type")) continue;
      outHeaders[lk] = v;
    }

    if (!isHtml) {
      // 非 HTML：原样透传（含上游的 content-encoding / content-length）。
      // MIME 必须可靠——见上方 MIME 注释；压缩一律不做，见上方「代理不自行压缩」。
      if (!outHeaders["content-type"]) {
        const guess = mimeOf(clientReq.url);
        if (guess) outHeaders["content-type"] = guess;
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
      outHeaders["content-length"] = String(buf.length);
      try {
        clientRes.writeHead(upRes.statusCode || 200, outHeaders);
        clientRes.end(buf);
      } catch { /* 忽略 */ }
    });
    upRes.on("error", (e) => {
      state.lastError = "上游读失败：" + (e && e.message);
      try {
        if (!clientRes.headersSent) sendHtml(clientRes, waitingPage(state.lastError));
        else clientRes.end();
      } catch { /* 忽略 */ }
    });
  });

  upReq.on("error", (e) => {
    state.lastError = String(e && e.message ? e.message : e);
    log("上游连接失败：" + state.lastError);
    note(clientReq.url, 0, "upstream-error");
    try {
      if (!clientRes.headersSent) sendHtml(clientRes, waitingPage("上游连接失败：" + state.lastError));
      else clientRes.end();
    } catch { /* 忽略 */ }
  });
  upReq.setTimeout(30000, () => { try { upReq.destroy(new Error("上游超时")); } catch { /* 忽略 */ } });

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

function sendJson(res, obj, status = 200) {
  const buf = Buffer.from(JSON.stringify(obj), "utf8");
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Content-Length": String(buf.length) });
  res.end(buf);
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
      diagCount: state.diag.length,
    });
    return true;
  }

  // 主题选择（auto / 具体主题名）。GET 给注入的客户端脚本读，POST 由设置页改。
  if (path === "/_hana/theme" && req.method === "GET") {
    const want = new URL(req.url || "/", "http://x").searchParams.get("theme");
    if (want) {
      // 卡片脚本可以按名要一套；auto 时用宿主的当前主题
      const r = await fetchThemeCss(want === "auto" ? "" : want);
      if (!r.ok) return sendJson(res, { ok: false, error: r.error || "取主题失败" }, 502);
      return sendJson(res, { ok: true, theme: r.theme, vars: parseThemeVars(r.css) });
    }
    sendJson(res, { ok: true, theme: state.themeChoice, applied: state.themeApplied, appearance: state.themeAttr });
    return true;
  }

  if (path === "/_hana/theme" && req.method === "POST") {
    const b = await readJson(req);
    if (typeof b.css === "string") state.themeCss = b.css;   // 保留兼容
    if (typeof b.appearance === "string") state.themeAttr = b.appearance;
    if (typeof b.theme === "string" && b.theme) {
      state.themeChoice = b.theme;
      themeCache = { at: 0, choice: null, vars: null };   // 换了主题：缓存作废
    }
    sendJson(res, { ok: true, theme: state.themeChoice, appearance: state.themeAttr });
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
function stopMagpie() {
  const pid = state.magpiePid;
  state.magpiePid = null;
  if (!pid) return;
  try { process.kill(pid, "SIGTERM"); } catch { /* 已退出 */ }
  const t = setTimeout(() => { try { process.kill(pid, "SIGKILL"); } catch { /* 已退出 */ } }, 3000);
  t.unref?.();
}

function startMagpie({ exe, cwd }) {
  if (!exe || !existsSync(exe)) {
    state.phase = "error";
    state.lastError = `magpie 可执行文件不存在：${exe}`;
    log(state.lastError);
    return;
  }
  try { mkdirSync(join(cwd, "data"), { recursive: true }); } catch { /* 忽略 */ }

  const args = ["web", "--addr", "127.0.0.1:0", "--no-open"];
  log(`spawn: ${exe} ${args.join(" ")}  (cwd=${cwd})`);

  const child = spawn(exe, args, { cwd, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  state.magpiePid = child.pid;
  state.phase = "waiting-magpie";

  let buffer = "";
  const onChunk = (chunk) => {
    buffer += chunk.toString("utf8");
    const clean = buffer.replace(ANSI, "");
    const m = clean.match(/magpie web on\s+https?:\/\/[\d.]+:(\d+)\/\?k=([^\s]+)/);
    if (m && !state.upstreamPort) {
      state.upstreamPort = parseInt(m[1], 10);
      state.upstreamKey = m[2];
      state.phase = "ready";
      state.lastError = null;
      log(`上游就绪：127.0.0.1:${state.upstreamPort}（key 已取得）`);
      process.stdout.write(`MAGPIE_UPSTREAM_READY port=${state.upstreamPort}\n`);
    }
    if (clean.length > 40000) buffer = clean.slice(-8000);
  };
  child.stdout.on("data", onChunk);
  child.stderr.on("data", onChunk);

  child.on("exit", (code, sig) => {
    if (state.magpiePid !== child.pid) return;
    state.magpiePid = null;
    state.upstreamPort = 0;
    if (state.phase !== "stopping") {
      state.phase = "error";
      state.lastError = `magpie 退出（code=${code} signal=${sig}）`;
      log(state.lastError);
    }
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
  const opts = { exe: "", cwd: "", port: 0, marker: "", hidden: null, theme: null };
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
  }
  if (opts.hidden !== null) {
    state.hidden = new Set(opts.hidden.split(",").map((s) => s.trim()).filter(Boolean));
  }
  if (opts.theme) state.themeChoice = opts.theme;
  state.serverTheme = readHanaTheme();
  log(`Hana 主题（服务端读）：${state.serverTheme || "（未读到）"}`);

  server = createServer((req, res) => {
    if ((req.url || "").startsWith("/_hana/")) {
      handleInternal(req, res).then((handled) => {
        if (!handled) sendJson(res, { ok: false, error: "unknown internal endpoint" }, 404);
      });
      return;
    }
    proxyRequest(req, res);
  });
  server.on("error", (e) => {
    state.phase = "error";
    state.lastError = `代理监听失败（端口 ${opts.port}）：${e && e.message}`;
    log(state.lastError);
    process.stdout.write(`MAGPIE_PROXY_ERROR ${state.lastError}\n`);
    process.exit(1);
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
