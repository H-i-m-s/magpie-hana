// index.js — magpie-hana v2 App 入口
// ─────────────────────────────────────────────────────────────────────────────
// 职责：
//   1. exe 播种：vendor/magpie-windows-amd64.exe -> app-data/bin/magpie.exe
//      （只在缺失时播种；已存在绝不覆盖 —— 否则会造成降级事故，见 doc/接手文档.md §6）
//   2. 托管 + 反代：拉起 runtime/proxy.mjs（local-machine），由它 spawn magpie web
//      —— 代理与 magpie 同在一个 job object，Hana 一停整体回收
//   3. 主题：订阅宿主主题 -> 映射成 magpie 的 CSS 变量 -> 推给代理注入
//   4. 工具 magpie：status / models / quotas / agents / use / start / stop / hidden
//
// 红线（见 doc/接手文档.md §4）：
//   - 不碰用户的 ~/.config/magpie（靠便携 data/ 达成）
//   - 不抢 3425
//   - 不自动更新 exe
// ─────────────────────────────────────────────────────────────────────────────

import { defineApp } from "./sdk/app-contract/server-client.js";
import { randomBytes } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const APP_ID = "magpie-hana";
const APP_VERSION = "0.1.0";

const PROXY_ENTRY = "runtime/proxy.mjs";
const VENDOR_EXE = "vendor/magpie-windows-amd64.exe";
// 与 manifest 里 contributes.cards[].service.id 一致：宿主据此把卡片挂到本服务
const SERVICE_ID = "magpie-ui";

const READY_MAX_MS = 90_000;
const READY_POLL_MS = 400;
const RETRY_DELAYS_MS = [3_000, 10_000, 30_000, 60_000];

const CAP_RUNTIME = "app/runtime.execute";
const CAP_LOCAL = "app/runtime.local-machine";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const name = APP_ID;

export default defineApp(async (sdk) => {
  const fire = (p) => { try { if (p && typeof p.catch === "function") p.catch(() => {}); } catch { /* 忽略 */ } };
  const log = (m) => fire(sdk.logger?.info?.(`[${APP_ID}] ${m}`));
  const warn = (m) => fire(sdk.logger?.warn?.(`[${APP_ID}] ${m}`));
  const err = (m) => fire(sdk.logger?.error?.(`[${APP_ID}] ${m}`));
  const msgOf = (e) => (e && e.message ? String(e.message) : String(e));

  const dataDir = sdk.dataDir;
  if (typeof dataDir !== "string" || !dataDir) throw new Error("magpie-hana: sdk.dataDir 缺失");
  const appRoot = new URL(".", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
  const binDir = join(dataDir, "bin");
  const exePath = join(binDir, "magpie.exe");
  const magpieDataDir = join(binDir, "data");
  const runtimeFile = join(dataDir, "runtime.json");
  const uiFile = join(dataDir, "ui.json");

  log(`apply | dataDir=${dataDir} | appRoot=${appRoot} | v${APP_VERSION}`);

  // ── 状态 ──────────────────────────────────────────────────────────────────
  const state = {
    phase: "idle",           // idle | starting | ready | error | disabled
    runtimeId: null,
    proxyPort: 0,
    upstreamPort: 0,
    magpieVersion: "",
    magpiePid: null,
    lastError: null,
    startPromise: null,
    retryIdx: 0,
    themeCss: "",
    themeAttr: "",
    hidden: readUi().hidden,
    seeding: null,
  };

  function readUi() {
    try {
      if (existsSync(uiFile)) {
        const j = JSON.parse(readFileSync(uiFile, "utf8"));
        return { hidden: Array.isArray(j.hidden) ? j.hidden : ["library", "sessions"] };
      }
    } catch { /* 忽略 */ }
    return { hidden: ["library", "sessions"] };
  }

  function saveUi() {
    try {
      writeFileSync(uiFile, JSON.stringify({ hidden: state.hidden }, null, 2), "utf8");
    } catch (e) { warn(`保存 ui.json 失败：${msgOf(e)}`); }
  }

  // ── exe 播种（绝不在已存在时覆盖，见 doc §6）────────────────────────────
  function seedExe() {
    if (existsSync(exePath)) return { seeded: false, path: exePath, size: statSync(exePath).size };
    const vendor = join(appRoot, VENDOR_EXE);
    if (!existsSync(vendor)) {
      throw new Error(
        `内置的 magpie 可执行文件缺失：${VENDOR_EXE}。` +
        `请重新安装本 App，或把 magpie-windows-amd64.exe 放到 App 目录的 vendor/ 下。`);
    }
    mkdirSync(binDir, { recursive: true });
    // 先拷到临时名，再落定；全程不碰已存在的目标
    const tmp = exePath + ".new";
    if (existsSync(tmp)) { try { rmSync(tmp, { force: true }); } catch { /* 忽略 */ } }
    copyFileSync(vendor, tmp);
    if (existsSync(exePath)) {  // 并发播种：别人先落了，放弃自己的那份
      try { rmSync(tmp, { force: true }); } catch { /* 忽略 */ }
      return { seeded: false, path: exePath, size: statSync(exePath).size };
    }
    copyFileSync(tmp, exePath);
    try { rmSync(tmp, { force: true }); } catch { /* 忽略 */ }
    const size = statSync(exePath).size;
    log(`已从 vendor 播种 exe：${exePath}（${size} 字节）`);
    return { seeded: true, path: exePath, size };
  }

  // ── 能力探测 ──────────────────────────────────────────────────────────────
  async function capabilityMap() {
    try {
      const res = await sdk.bus.request("app:capabilities", {});
      const map = {};
      for (const c of (res && Array.isArray(res.capabilities) ? res.capabilities : [])) {
        if (c && typeof c.capability === "string") map[c.capability] = c.status;
      }
      return map;
    } catch (e) {
      warn(`app:capabilities 查询失败：${msgOf(e)}`);
      return null;
    }
  }

  async function hasCap(word) {
    const m = await capabilityMap();
    if (!m) return false;
    const s = m[word];
    return s === "granted" || s === "allowed" || s === true;
  }

  // ── 代理 HTTP 调用（本进程 <-> proxy.mjs）─────────────────────────────────
  async function proxyJson(path, init) {
    if (!state.proxyPort) throw new Error("代理未就绪");
    const url = `http://127.0.0.1:${state.proxyPort}${path}`;
    const res = await sdk.network.fetch(url, {
      timeoutMs: 15000,
      maxResponseBytes: 4 * 1024 * 1024,
      ...init,
    });
    const text = await res.text();
    return JSON.parse(text);
  }

  // 上游 magpie 网关（3425）的 JSON —— 工具用
  async function gatewayJson(path, timeoutMs = 12000) {
    const url = `http://127.0.0.1:3425${path}`;
    const res = await sdk.network.fetch(url, { timeoutMs, maxResponseBytes: 4 * 1024 * 1024 });
    if (!res.ok) throw new Error(`${path} 返回 ${res.status}`);
    return JSON.parse(await res.text());
  }

  async function gatewayUp() {
    try {
      const r = await sdk.network.fetch("http://127.0.0.1:3425/", { timeoutMs: 4000, maxResponseBytes: 65536 });
      if (!r.ok) return null;
      const j = JSON.parse(await r.text());
      return j && j.name === "magpie" ? j : null;
    } catch { return null; }
  }

  // ── 启动代理（它内部会拉起 magpie web）───────────────────────────────────
  async function startOnce(attempt) {
    if (!sdk.runtime || typeof sdk.runtime.start !== "function") {
      throw new Error("宿主 ctx.runtime 不可用（app/runtime.execute 未授予或宿主过旧）");
    }
    if (!(await hasCap(CAP_RUNTIME))) throw new Error(`缺少能力：${CAP_RUNTIME}`);
    if (!(await hasCap(CAP_LOCAL))) throw new Error(`缺少能力：${CAP_LOCAL}`);

    seedExe();
    mkdirSync(magpieDataDir, { recursive: true });

    const port = 41000 + Math.floor(Math.random() * 8000);
    const readyMarker = "MAGPIE_HANA_READY:" + randomBytes(16).toString("base64url");

    log(`启动受管 runtime（attempt ${attempt}，代理端口 ${port}）`);
    let rt;
    try {
      rt = await sdk.runtime.start({
        runtime: "node",
        entry: PROXY_ENTRY,
        profile: "local-machine",
        network: "external",
        cwd: dataDir,
        args: [
          `--exe=${exePath}`,
          `--cwd=${binDir}`,
          `--port=${port}`,
          `--marker=${readyMarker}`,
          `--hidden=${state.hidden.join(",")}`,
        ],
        service: { id: SERVICE_ID, port, readyMarker },
      });
    } catch (e) {
      const raw = msgOf(e);
      const code = /not authorized|authoriz|DENIED|declined/i.test(raw) ? "未授权" : "启动失败";
      throw new Error(`宿主拒绝启动受管 runtime（${code}）：${raw}`);
    }
    state.runtimeId = rt?.runtimeId || rt?.id || null;

    // 等代理就绪 + 上游 magpie 就绪
    const deadline = Date.now() + READY_MAX_MS;
    while (Date.now() < deadline) {
      try {
        const st = await proxyJson("/_hana/status");
        state.proxyPort = st.port || state.proxyPort;
        state.upstreamPort = st.upstreamPort || 0;
        state.magpiePid = st.magpiePid || null;
        if (st.phase === "ready" && state.upstreamPort) {
          state.phase = "ready";
          state.lastError = null;
          state.retryIdx = 0;
          saveRuntime();
          log(`就绪：代理 ${state.proxyPort} -> magpie ${state.upstreamPort}`);
          pushThemeNow();
          return;
        }
        if (st.phase === "error") {
          throw new Error(st.error || "magpie 启动失败");
        }
      } catch (e) {
        if (/上游|magpie/.test(msgOf(e))) throw e;
        // 代理还没起来，继续等
      }
      await sleep(READY_POLL_MS);
    }
    throw new Error(`${READY_MAX_MS / 1000}s 内未就绪`);
  }

  function saveRuntime() {
    try {
      writeFileSync(runtimeFile, JSON.stringify({
        proxyPort: state.proxyPort,
        upstreamPort: state.upstreamPort,
        magpieVersion: state.magpieVersion,
        updatedAt: new Date().toISOString(),
      }, null, 2), "utf8");
    } catch { /* 忽略 */ }
  }

  async function ensureStarted() {
    if (state.phase === "ready") return true;
    if (state.startPromise) return state.startPromise;
    state.phase = "starting";
    state.startPromise = (async () => {
      let last = null;
      for (let i = 0; i < RETRY_DELAYS_MS.length + 1; i++) {
        try {
          await startOnce(i + 1);
          return true;
        } catch (e) {
          last = e;
          state.lastError = msgOf(e);
          err(`启动失败（第 ${i + 1} 次）：${state.lastError}`);
          if (i < RETRY_DELAYS_MS.length) await sleep(RETRY_DELAYS_MS[i]);
        }
      }
      state.phase = "error";
      throw last || new Error("启动失败");
    })();
    try { return await state.startPromise; }
    finally { state.startPromise = null; }
  }

  async function stopRuntime() {
    try { if (state.proxyPort) await proxyJson("/_hana/quit", { method: "POST", body: "{}" }); } catch { /* 忽略 */ }
    try {
      if (state.runtimeId && sdk.runtime?.stop) await sdk.runtime.stop(state.runtimeId);
    } catch (e) { warn(`停止 runtime 失败：${msgOf(e)}`); }
    state.runtimeId = null; state.proxyPort = 0; state.upstreamPort = 0;
    state.magpiePid = null; state.phase = "idle";
  }

  // ── 主题 ──────────────────────────────────────────────────────────────────
  // magpie 的 app.css 用 :root 上的 CSS 变量。把 Hana 主题映射到它的核心变量。
  // 宿主签发的是 cssUrl + theme + appearance；这里以 Hana 自己的调色为源做映射。
  function buildThemeCss(snap) {
    const dark = (snap?.appearance || snap?.theme || "").toString().toLowerCase().includes("dark")
      || /midnight|night|dark/i.test(String(snap?.theme || ""));
    // Hana 的观感：深色蓝调的克制配色（与 SSS 的偏好一致）
    const p = dark ? {
      bg: "#16171b", card: "#1e1f24", card2: "#1a1b20", pill: "#26272e", pillHover: "#2e3038",
      line: "rgba(255,255,255,.07)", line2: "rgba(255,255,255,.05)",
      fg: "#e8e9ed", fg2: "#c2c4cc", muted: "#8b8d98", faint: "#6a6c76",
      accent: "#6b7dff", accentSoft: "rgba(107,125,255,.14)", sel: "rgba(107,125,255,.16)",
      green: "#3ecf8e", greenSoft: "rgba(62,207,142,.14)",
      red: "#ff6b6b", redSoft: "rgba(255,107,107,.14)",
      amber: "#e0a458", amberSoft: "rgba(224,164,88,.14)",
      shadow: "0 16px 44px rgba(0,0,0,.5), 0 2px 8px rgba(0,0,0,.32), 0 0 0 .5px rgba(255,255,255,.06)",
    } : {
      bg: "#f4f5f8", card: "#ffffff", card2: "#f9fafc", pill: "#eef0f5", pillHover: "#e6e9f0",
      line: "rgba(0,0,0,.08)", line2: "rgba(0,0,0,.05)",
      fg: "#1b1d24", fg2: "#4a4d57", muted: "#82858f", faint: "#a9acb6",
      accent: "#5b6bdd", accentSoft: "rgba(91,107,221,.1)", sel: "rgba(91,107,221,.12)",
      green: "#189a58", greenSoft: "rgba(24,154,88,.1)",
      red: "#d64545", redSoft: "rgba(214,69,69,.1)",
      amber: "#b45309", amberSoft: "rgba(180,83,9,.1)",
      shadow: "0 16px 44px rgba(20,22,30,.14), 0 2px 8px rgba(20,22,30,.06), 0 0 0 .5px rgba(20,22,30,.08)",
    };
    return `/* magpie-hana: 由 Hana 主题映射 */\n:root {\n` +
      `  --bg:${p.bg}; --card:${p.card}; --card-2:${p.card2};\n` +
      `  --pill:${p.pill}; --pill-hover:${p.pillHover};\n` +
      `  --line:${p.line}; --line-2:${p.line2};\n` +
      `  --fg:${p.fg}; --fg-2:${p.fg2}; --muted:${p.muted}; --faint:${p.faint};\n` +
      `  --accent:${p.accent}; --accent-soft:${p.accentSoft}; --sel:${p.sel};\n` +
      `  --green:${p.green}; --green-soft:${p.greenSoft};\n` +
      `  --red:${p.red}; --red-soft:${p.redSoft};\n` +
      `  --amber:${p.amber}; --amber-soft:${p.amberSoft};\n` +
      `  --shadow:${p.shadow};\n` +
      `}\n` +
      `:root[data-theme="dark"] {\n` +
      `  --bg:${p.bg}; --card:${p.card}; --card-2:${p.card2};\n` +
      `  --pill:${p.pill}; --pill-hover:${p.pillHover};\n` +
      `  --line:${p.line}; --line-2:${p.line2};\n` +
      `  --fg:${p.fg}; --fg-2:${p.fg2}; --muted:${p.muted}; --faint:${p.faint};\n` +
      `  --accent:${p.accent}; --accent-soft:${p.accentSoft}; --sel:${p.sel};\n` +
      `  --green:${p.green}; --green-soft:${p.greenSoft};\n` +
      `  --red:${p.red}; --red-soft:${p.redSoft};\n` +
      `  --amber:${p.amber}; --amber-soft:${p.amberSoft};\n` +
      `  --shadow:${p.shadow};\n` +
      `}\n`;
  }

  function pushThemeNow() {
    if (!state.proxyPort || !state.themeCss) return;
    fire(proxyJson("/_hana/theme", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ css: state.themeCss, appearance: state.themeAttr }),
    }).catch((e) => warn(`推送主题失败：${msgOf(e)}`)));
  }

  function applyThemeSnapshot(snap) {
    try {
      state.themeAttr = String(snap?.appearance || "").includes("light") ? "light" : "dark";
      state.themeCss = buildThemeCss(snap);
      pushThemeNow();
    } catch (e) { warn(`主题映射失败：${msgOf(e)}`); }
  }

  // 订阅宿主主题（浏览器 SDK 侧也有，这里是服务端尽力而为）
  try {
    if (sdk.bus && typeof sdk.bus.subscribe === "function") {
      fire(sdk.bus.subscribe((ev) => {
        if (ev && ev.type === "app_event" && ev.themeSnapshot) applyThemeSnapshot(ev.themeSnapshot);
      }));
    }
  } catch { /* 忽略 */ }

  // ── 工具 ──────────────────────────────────────────────────────────────────
  const ACTIONS = ["status", "models", "quotas", "agents", "use", "start", "stop", "hidden", "update-check"];

  async function actStatus() {
    const up = await gatewayUp();
    let proxy = null;
    try { if (state.proxyPort) proxy = await proxyJson("/_hana/status"); } catch { /* 忽略 */ }
    const lines = [
      `托管的 magpie：${state.phase}`,
      `代理端口：${state.proxyPort || "—"}`,
      `magpie 网页：${state.upstreamPort ? `127.0.0.1:${state.upstreamPort}` : "—"}`,
      `网关 3425：${up ? `活着（v${up.version}，${up.models} 个模型）` : "不可达"}`,
      `magpie 进程：${state.magpiePid || proxy?.magpiePid || "—"}`,
      `隐藏的功能：${state.hidden.join(", ") || "（无）"}`,
    ];
    if (state.lastError) lines.push(`最近错误：${state.lastError}`);
    return { text: lines.join("\n"), data: { phase: state.phase, proxy, gateway: up } };
  }

  async function actModels() {
    const j = await gatewayJson("/v1/models");
    const list = Array.isArray(j.data) ? j.data : [];
    const text = list.length
      ? list.map((m) => `• ${m.id}${m.display_name ? `  （${m.display_name}）` : ""}`).join("\n")
      : "（没有可用的模型）";
    return { text: `magpie 当前提供 ${list.length} 个模型：\n${text}`, data: { models: list } };
  }

  async function actQuotas() {
    const j = await gatewayJson("/v1/magpie/quotas");
    const list = Array.isArray(j.data) ? j.data : [];
    if (!list.length) return { text: "没有可报告的订阅额度。", data: { quotas: [] } };
    const text = list.map((q) => {
      const wins = (q.windows || []).map((w) => w.display || `${w.used}/${w.limit} ${w.unit || ""}`).join(" · ");
      return `• ${q.name || q.provider}${q.plan ? `（${q.plan}）` : ""}：${wins || "—"}`;
    }).join("\n");
    return { text: `订阅额度：\n${text}`, data: { quotas: list } };
  }

  async function actAgents() {
    // 用 magpie ls 需要外部命令能力；这里退回网关模型清单做来源分组
    const j = await gatewayJson("/v1/models");
    const list = Array.isArray(j.data) ? j.data : [];
    const byProvider = {};
    for (const m of list) {
      const p = String(m.id || "").split("/")[0] || "?";
      (byProvider[p] ||= []).push(m.id);
    }
    const text = Object.entries(byProvider)
      .map(([p, ids]) => `• ${p}：${ids.length} 个模型`)
      .join("\n");
    return { text: `可用来源：\n${text}\n\n（查看/切换某个 agent 的具体模型请在 Magpie 工作区里操作）`, data: { byProvider } };
  }

  async function actHidden(args) {
    if (Array.isArray(args.hidden)) {
      state.hidden = args.hidden.map(String);
      saveUi();
      if (state.proxyPort) {
        try {
          await proxyJson("/_hana/hide", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ hidden: state.hidden }),
          });
        } catch (e) { warn(`推送隐藏设置失败：${msgOf(e)}`); }
      }
      return { text: `已隐藏：${state.hidden.join(", ") || "（无）"}`, data: { hidden: state.hidden } };
    }
    return { text: `当前隐藏：${state.hidden.join(", ") || "（无）"}`, data: { hidden: state.hidden } };
  }

  async function actStart() {
    await ensureStarted();
    return actStatus();
  }

  async function actStop() {
    await stopRuntime();
    return { text: "托管的 magpie 已停止。", data: { phase: state.phase } };
  }

  async function actUpdateCheck() {
    // 只检查，不执行 —— 执行会替换 exe 并重启，必须由用户显式确认（见 doc §4 R1）
    return {
      text: "内置的 magpie 可以自更新，但更新会重启进程、中断正在进行的对话。"
        + "本 App 不自动执行更新；如需更新，请在工作区里手动触发（后续版本提供）。",
      data: { currentVersion: state.magpieVersion, autoUpdate: false },
    };
  }

  try {
    await sdk.tools.register({
      name: "magpie",
      description:
        "Magpie：操作本机 magpie（多 agent 模型统一管理器）的工具（一个 App 一个同名工具，action 选动作）。" +
        "status=托管状态/端口/网关健康状况；models=列出 magpie 当前提供的全部模型（provider/model 形式）；" +
        "quotas=各订阅的额度余量；agents=按来源分组看可用模型；use=（预留，切模型请在 Magpie 工作区操作）；" +
        "start/stop=手动起停托管的 magpie；hidden=查看或设置在工作区里隐藏哪些功能（传 hidden 数组，如 [\"library\",\"sessions\",\"routing\"]）；" +
        "update-check=查询更新情况（本 App 不自动更新 magpie，更新会中断对话）。",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["action"],
        properties: {
          action: {
            type: "string",
            enum: ACTIONS,
            description: "动作：status / models / quotas / agents / use / start / stop / hidden / update-check",
          },
          hidden: {
            type: "array",
            items: { type: "string" },
            description: "hidden 动作用：要隐藏的功能 id 数组（library、sessions、routing、stats、gateway、settings-otel、settings-sync、settings-privacy）",
          },
        },
      },
      sessionPermission: { readOnly: false },
      execute: async (input) => {
        const args = input && typeof input === "object" ? input : {};
        const action = String(args.action || "status").trim();
        try {
          let out;
          switch (action) {
            case "status": out = await actStatus(); break;
            case "models": out = await actModels(); break;
            case "quotas": out = await actQuotas(); break;
            case "agents": out = await actAgents(); break;
            case "use": out = { text: "请在 Magpie 工作区里选择模型（或使用 magpie 自身的 CLI）。", data: {} }; break;
            case "start": out = await actStart(); break;
            case "stop": out = await actStop(); break;
            case "hidden": out = await actHidden(args); break;
            case "update-check": out = await actUpdateCheck(); break;
            default: throw new Error(`未知 action：${action}`);
          }
          return { content: [{ type: "text", text: out.text }], structuredContent: out.data };
        } catch (e) {
          const m = msgOf(e);
          return {
            isError: true,
            content: [{ type: "text", text: `magpie ${action} 失败：${m}` }],
          };
        }
      },
    });
    log("工具 magpie 已注册");
  } catch (e) {
    err(`注册工具失败：${msgOf(e)}`);
  }

  // ── 路由（卡片页面用）──────────────────────────────────────────────────────
  try {
    await sdk.routes.register((app) => {
      app.get("/magpie-hana/status", async (c) => {
        try {
          const up = await gatewayUp();
          return c.json({
            ok: true, app: { id: APP_ID, version: APP_VERSION },
            phase: state.phase, proxyPort: state.proxyPort, upstreamPort: state.upstreamPort,
            magpiePid: state.magpiePid, magpieVersion: state.magpieVersion,
            lastError: state.lastError, hidden: state.hidden,
            gateway: up ? { ok: true, version: up.version, models: up.models } : { ok: false },
          });
        } catch (e) { return c.json({ ok: false, error: msgOf(e) }, 500); }
      });

      app.post("/magpie-hana/start", async (c) => {
        try { await ensureStarted(); return c.json({ ok: true, phase: state.phase, proxyPort: state.proxyPort }); }
        catch (e) { return c.json({ ok: false, error: msgOf(e) }, 500); }
      });

      app.post("/magpie-hana/stop", async (c) => {
        try { await stopRuntime(); return c.json({ ok: true, phase: state.phase }); }
        catch (e) { return c.json({ ok: false, error: msgOf(e) }, 500); }
      });

      app.post("/magpie-hana/hidden", async (c) => {
        try {
          const body = await c.req.json().catch(() => ({}));
          return c.json(await actHidden({ hidden: body.hidden }));
        } catch (e) { return c.json({ ok: false, error: msgOf(e) }, 500); }
      });

      app.get("/magpie-hana/theme", (c) => c.json({
        ok: true, appearance: state.themeAttr, hasCss: !!state.themeCss,
      }));

      app.get("/magpie-hana/health", (c) => c.json({ ok: true, app: { id: APP_ID, version: APP_VERSION } }));
    });
    log("路由已注册");
  } catch (e) {
    err(`注册路由失败：${msgOf(e)}`);
  }

  // ── 启动 ──────────────────────────────────────────────────────────────────
  // 不阻塞 apply：先返回，让 App 尽快 ready；启动在后台推进。
  fire((async () => {
    try {
      applyThemeSnapshot({ appearance: "dark", theme: "midnight" });
      await ensureStarted();
      log("magpie 托管就绪");
    } catch (e) {
      err(`初始启动失败：${msgOf(e)}`);
    }
  })());

  log("apply 完成");
});
