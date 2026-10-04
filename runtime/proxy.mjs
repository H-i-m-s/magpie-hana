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
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const UPSTREAM_HOST = "127.0.0.1";
const ANSI = /\x1b\[[0-9;]*[A-Za-z]/g;
const HOP_BY_HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade",
]);

const DEFAULT_HIDDEN = ["library", "sessions"];

const state = {
  listenPort: 0,
  upstreamPort: 0,
  upstreamKey: "",
  hidden: new Set(DEFAULT_HIDDEN),
  themeAttr: "",
  lastError: null,
  phase: "starting",   // starting | waiting-magpie | ready | error | stopping
  requests: 0,
  rewrites: 0,
  magpiePid: null,
};

const log = (m) => process.stderr.write(`[magpie-hana] ${m}\n`);

// ── 主题：在客户端按宿主当前主题取色 ────────────────────────────────────────
// 为什么不在这里硬编码色号：Hana 的主题是用户可换的命名主题（青夜 / 暖纸 /
// 高对比 …），色号写死在代理里，一换主题就错。改成注入一段脚本，向宿主
// /api/apps/theme.css 取「当前主题」的真实变量，再映射到 magpie 的变量名。
// 用 !important 提升特异性，压过 magpie 自己的 :root / :root[data-theme]。
const THEME_CLIENT = `<script id="hana-theme-client">
(function(){
  var THEME_URL = "/api/apps/theme.css";
  var STYLE_ID = "hana-theme";
  // 需要从宿主取的变量（Hana 的主题用这套命名）
  var WANT = ["--bg","--bg-card","--bg-glass","--sidebar-bg","--accent","--accent-hover",
              "--text","--text-light","--text-muted","--border","--shadow",
              "--green","--coral","--danger","--pop-bg"];

  function parseVars(text){
    var out = {}, re = /(--[A-Za-z0-9_-]+)\\s*:\\s*([^;}]+)/g, m;
    while ((m = re.exec(text))) { out[m[1]] = m[2].trim(); }
    return out;
  }

  // 优先：直接读宿主已生效的计算值。卡片服务挂在宿主同源下，parent 可达；
  // 这样不靠主题名猜测，用户换任何主题都自动跟随。跨源时会抛异常，转下一条路。
  function hostVars(){
    var w = window, i = 0;
    while (w && i < 6) {
      try {
        var d = w.document;
        if (d && d.documentElement) {
          var cs = w.getComputedStyle(d.documentElement), out = {};
          for (var k = 0; k < WANT.length; k++) {
            var v = cs.getPropertyValue(WANT[k]);
            if (v && v.trim()) out[WANT[k]] = v.trim();
          }
          if (out["--bg"] || out["--text"]) return out;
        }
      } catch (e) { /* 跨源，下一个候选 */ }
      try { if (w === w.parent) break; w = w.parent; } catch (e) { break; }
      i++;
    }
    return null;
  }

  function hostThemeName(){
    var w = window, i = 0;
    while (w && i < 6) {
      try {
        var d = w.document;
        if (d && d.documentElement) {
          var t = d.documentElement.getAttribute("data-theme");
          if (t && t !== "auto" && t !== "inherit") return t;
        }
      } catch (e) {}
      try { if (w === w.parent) break; w = w.parent; } catch (e) { break; }
      i++;
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

  var lastUrl = null;
  function fetchByUrl(name){
    var url = THEME_URL + (name ? "?theme=" + encodeURIComponent(name) : "");
    if (url === lastUrl) return;
    lastUrl = url;
    var f = window.__hanaOrigFetch || window.fetch;
    f.call(window, url, { credentials: "same-origin" })
      .then(function(r){ return (r && r.ok) ? r.text() : ""; })
      .then(function(t){ if (t) applyVars(parseVars(t)); })
      .catch(function(){});
  }

  // 双层：先读宿主计算值（同一文档树，最准）；读不到再按主题名取 CSS
  function refresh(){
    var v = hostVars();
    if (v) { applyVars(v); lastUrl = null; return; }
    fetchByUrl(hostThemeName());
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
  setInterval(refresh, 15000);
})();
</script>
`;

// 兜底：宿主侧推来的 css（现在不再由 index.js 推送，保留端点以防将来需要）
function themeBlock() {
  if (!state.themeCss) return "";
  return `<style id="hana-theme-pushed">\n${state.themeCss}\n</style>\n`;
}

function hiddenBlock() {
  const byFeature = {
    library: ['#view-library', '#ptabs [data-ptab="library"]'],
    sessions: ['#view-sessions', '#ptabs [data-ptab="sessions"]'],
    routing: ['#view-routing', '#ptabs [data-ptab="routing"]'],
    stats: ['#ptabs [data-ptab="stats"]'],
    gateway: ['#view-gateway'],
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

function inject(html) {
  let out = html;
  // ① 垫片必须在 magpie 自己的脚本之前
  if (/<head>/i.test(out)) out = out.replace(/<head>/i, "<head>" + BASE_SHIM);
  else if (/<html([^>]*)>/i.test(out)) out = out.replace(/<html([^>]*)>/i, (m) => m + BASE_SHIM);
  // ② 主题与隐藏规则放到 head 末尾（app.css 之后）
  const tail = themeBlock() + hiddenBlock() + THEME_CLIENT;
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
    path: clientReq.url,
    headers,
  }, (upRes) => {
    const ct = String(upRes.headers["content-type"] || "");
    const isHtml = /text\/html/i.test(ct);

    const outHeaders = {};
    for (const [k, v] of Object.entries(upRes.headers)) {
      const lk = k.toLowerCase();
      if (HOP_BY_HOP.has(lk)) continue;
      // 去掉会妨碍我们注入/内嵌的头
      if (lk === "x-frame-options" || lk === "content-security-policy") continue;
      if (lk === "content-encoding" || lk === "content-length" || lk === "content-type") continue;
      outHeaders[lk] = v;
    }

    if (!isHtml) {
      // 非 HTML：原样流式透传
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
    upRes.on("end", () => {
      let text = Buffer.concat(chunks).toString("utf8");
      try { text = inject(text); } catch (e) { state.lastError = "inject: " + String(e); }
      const buf = Buffer.from(text, "utf8");
      outHeaders["content-type"] = ct || "text/html; charset=utf-8";
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
      theme: state.themeAttr,
    });
    return true;
  }

  if (path === "/_hana/theme" && req.method === "POST") {
    const b = await readJson(req);
    if (typeof b.css === "string") state.themeCss = b.css;   // 保留兼容
    if (typeof b.appearance === "string") state.themeAttr = b.appearance;
    sendJson(res, { ok: true, theme: state.themeAttr });
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
  const opts = { exe: "", cwd: "", port: 0, marker: "", hidden: null };
  for (const a of process.argv.slice(2)) {
    const i = a.indexOf("=");
    if (i < 0) continue;
    const k = a.slice(0, i), v = a.slice(i + 1);
    if (k === "--exe") opts.exe = v;
    else if (k === "--cwd") opts.cwd = v;
    else if (k === "--port") opts.port = parseInt(v, 10) || 0;
    else if (k === "--marker") opts.marker = v;
    else if (k === "--hidden") opts.hidden = v;
  }
  if (opts.hidden !== null) {
    state.hidden = new Set(opts.hidden.split(",").map((s) => s.trim()).filter(Boolean));
  }

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
