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
import { gzipSync, brotliCompressSync, constants as zlibConstants } from "node:zlib";
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

// ── 压缩 ─────────────────────────────────────────────────────────────────────
// 本地回环上不做压缩，首屏要裸传近 1.9MB（实测 app.js 839KB、i18n.js 287KB、
// app.css 229KB）。文本类资源压一下能省七成。
// 只压文本类；图片/字体本身就是压缩格式，再压反而更大。
const COMPRESSIBLE = /^(text\/|application\/(json|javascript|xml|x-javascript)|image\/svg)/i;

function pickEncoding(req, contentType, upEncoding) {
  if (upEncoding) return null;                    // 上游已编码，别二次压
  if (!COMPRESSIBLE.test(String(contentType || ""))) return null;
  const ae = String((req.headers && req.headers["accept-encoding"]) || "").toLowerCase();
  if (/\bbr\b/.test(ae)) return "br";
  if (/\bgzip\b/.test(ae)) return "gzip";
  return null;
}

function compress(buf, enc) {
  try {
    if (enc === "br") {
      // 质量 5：比默认 11 快一个量级，体积只多几个百分点
      return brotliCompressSync(buf, { params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 5 } });
    }
    return gzipSync(buf, { level: 6 });
  } catch {
    return null;
  }
}

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

  // ── 避让宿主卡片右上角的按钮簇 ─────────────────────────────────
  // 卡片里右上角悬浮着宿主的卡片按钮（设置/关闭），magpie 也把「设置」齿轮
  // 贴在右上角，两者重叠。这里量宿主那些按钮的实际宽度（同源可读），
  // 设成 CSS 变量；量不到就用默认值。
  (function adapt(){
    var gap = 112;
    try {
      var w = window;
      while (w && w.parent && w.parent !== w) w = w.parent;
      var d = w.document;
      if (d) {
        // 找卡片上贴右上角的悬浮按钮区（宿主命名不保证，多试几个选择器）
        var cands = d.querySelectorAll('[class*="chrome"],[class*="titlebar"],[class*="card-actions"],[class*="_tabLifted"]');
        var best = 0;
        for (var i = 0; i < cands.length; i++) {
          var el = cands[i], r = el.getBoundingClientRect();
          // 只看真正贴在右上角、且在视口内的
          if (r.width > 0 && r.height > 0 && r.top < 60 && r.right > w.innerWidth - 60) {
            var need = (w.innerWidth - r.left) + 10;
            if (need > best) best = need;
          }
        }
        if (best > 0 && best < 400) gap = Math.ceil(best);
      }
    } catch (e) { /* 跨源或读不到：用默认值 */ }
    try { document.documentElement.style.setProperty("--hana-chrome-gap", gap + "px"); } catch (e) {}
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

// 卡片内的适配：宿主在卡片右上角悬浮着它的卡片按钮簇（gecx/关闭），
// magpie 自己的 header 也把「设置」齿轮贴在右上角，两者正好叠在一起 ——
// 实测 magpie 齿轮在 y=10..36、x=最右，宿主 Chrome 就占那一块，齿轮被压住点不到。
// 这里给 magpie 的 header 留出右侧安全区，把 .actions 整体推离右上角。
// 只改摆放，不动 magpie 自身配色。
const ADAPT_CSS = `html:root header.top{
  padding-right: var(--hana-chrome-gap, 112px) !important;
}
html:root header.top #nav{
  min-width: 0 !important;
}
html:root header.top .actions{
  flex: none !important;
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
  // 未经改写的原语：本页脚本要访问宿主自己的资源（/api/apps/theme.css）时用它，
  // 免得被下面这层前缀垫片二次拼接。
  window.__hanaOrigFetch = window.fetch;
  window.__hanaXhrOpen = window.XMLHttpRequest && XMLHttpRequest.prototype.open;
  var base = (p.length > 1 && p.charAt(p.length - 1) === '/') ? p.slice(0, -1) : p;
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

function note(url, status, ct) {
  try {
    state.recent.push({ t: Date.now(), url, path: servicePath(url), status, ct: ct || "" });
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
    note(clientReq.url, upRes.statusCode || 0, ct);

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
      // 非 HTML：原样透传，但 MIME 必须可靠（见上方 MIME 注释），
      // 文本类则顺手压缩（本地回环不做压缩，裸传体积大得离谱）。
      if (!outHeaders["content-type"]) {
        const guess = mimeOf(clientReq.url);
        if (guess) outHeaders["content-type"] = guess;
      }
      const enc = pickEncoding(clientReq, outHeaders["content-type"], upRes.headers["content-encoding"]);
      if (!enc) {
        clientRes.writeHead(upRes.statusCode || 502, outHeaders);
        upRes.pipe(clientRes);
        return;
      }
      const chunks = [];
      let size = 0;
      upRes.on("data", (c) => {
        chunks.push(c); size += c.length;
        if (size > 64 * 1024 * 1024) { try { upRes.destroy(); } catch { /* 忽略 */ } }
      });
      upRes.on("end", () => {
        const raw = Buffer.concat(chunks);
        const packed = compress(raw, enc);
        if (!packed) {   // 压缩失败：退回裸传，不能什么都不发
          const h = { ...outHeaders, "content-length": String(raw.length) };
          delete h["content-encoding"];
          try {
            if (!clientRes.headersSent) { clientRes.writeHead(upRes.statusCode || 200, h); clientRes.end(raw); }
            else clientRes.end();
          } catch { /* 忽略 */ }
          return;
        }
        outHeaders["content-encoding"] = enc;
        outHeaders["content-length"] = String(packed.length);
        outHeaders.vary = outHeaders.vary ? outHeaders.vary + ", Accept-Encoding" : "Accept-Encoding";
        try {
          clientRes.writeHead(upRes.statusCode || 200, outHeaders);
          clientRes.end(packed);
        } catch { /* 忽略 */ }
      });
      upRes.on("error", () => { try { clientRes.end(); } catch { /* 忽略 */ } });
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
      const enc = pickEncoding(clientReq, outHeaders["content-type"], "");
      const packed = enc ? compress(buf, enc) : null;
      if (packed) {
        outHeaders["content-encoding"] = enc;
        outHeaders.vary = "Accept-Encoding";
        buf = packed;
      }
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
