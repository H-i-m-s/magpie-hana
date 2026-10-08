// media/provider.mjs — magpie 生图供应商（ctx.media / app/media.provide）
// ─────────────────────────────────────────────────────────────────────────────
// 把 magpie 网关的 /v1/images/generations 接成 Hana 的媒体提供方。装完就在
// 「多媒体」里出现一个叫 magpie 的供应商，模型表跟着网关里会画的模型走。
//
// 模型表从哪来：
//   GET <网关>/v1/models 带 X-Magpie-Drawers: 1
//   magpie 只对带这个头的调用方列出「会画画的模型」（kind === "image"）。
//   这个头本来是 magpie 给「另一个 magpie」准备的（internal/provider/
//   remote_magpie.go 里的 DrawersHeader），我们用同一个口子。
//
// 为什么不需要密钥：
//   magpie 的网关只监听 loopback 且接受任何 token（internal/gateway/gateway.go
//   里 Token 就是字面量 "magpie"），Authorization 只决定这次生成记在谁名下。
//   我们报 magpie-hanako，于是用量记在 OpenHanako 头上。
//
// 为什么 submit 一定要点名 model：
//   WorkBuddy 的图花的是套餐积分，magpie 只在模型被点名时才用它
//   （internal/gateway/draw.go 的 AutoDrawer 显式跳过 WorkBuddy）。所以这里
//   宁可报错也不替它挑。
//
// 为什么 submit 是异步的：
//   一次生成最长可能等 5 分钟（magpie 的 drawTimeout）。submit 立刻回 taskId，
//   真正干活在后台，query 轮询结果 —— 和内置的即梦 CLI 应用同一套形状。
//
// 为什么出图与下载交给自己的代理进程跑（runtime/proxy.mjs 的 /_hana/draw）：
//   magpie 把厂商的图以 URL 交回来（WorkBuddy 给的是腾讯云 CDN 上一条带签名的链接），
//   而 App 的 ctx.network.fetch 只放行清单 network.allowedHosts 里列过的主机（本清单
//   只有 127.0.0.1）。把各家 CDN 写进清单等于白名单跟着厂商变，每加一个还要用户重审
//   一次。本 App 本来就申请了一个 network: external 的 runtime（magpie 的父进程，
//   整棵树连着外网），让那个进程跑完网关与下载、把文件落到成品目录、只回文件名，
//   这里就只跟 127.0.0.1 说话。

import { readFileSync } from "node:fs";
import { extname, isAbsolute, join } from "node:path";

export const GATEWAY_BASE = "http://127.0.0.1:3425";
export const PROVIDER_ID = "magpie-hana";
export const PROTOCOL_ID = "magpie-hana-images";
export const ADAPTER_ID = "magpie-hana-images";

const DRAW_TIMEOUT_MS = 295_000;
const JOB_TTL_MS = 30 * 60 * 1000;
const MAX_REFS = 4;

const jobs = new Map();               // taskId -> { status, files, failReason, createdAt }

const pick = (...vals) => {
  for (const v of vals) if (v !== undefined && v !== null && v !== "") return v;
  return "";
};

const MIME_OF_EXT = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".avif": "image/avif",
};

function sweepJobs() {
  const now = Date.now();
  for (const [k, v] of jobs) if (now - (v.createdAt || 0) > JOB_TTL_MS) jobs.delete(k);
}

/** 参考图：caller 给的是会话里的文件引用，先落成本地路径，再读成 data URL。
 *  magpie 的网关只认 URL 或 data URL（它自己去 fetch），不认本地路径。 */
function referenceEntries(params) {
  const raw = [
    ...(Array.isArray(params.referenceImages) ? params.referenceImages : []),
    ...(params.image ? [params.image] : []),
    ...(Array.isArray(params.images) ? params.images : []),
  ];
  return raw.filter((x) => x !== undefined && x !== null && x !== "");
}

function pathOfReference(item) {
  if (typeof item === "string") return item;
  if (item && typeof item === "object") {
    return String(item.path || item.filePath || item.localPath || item.absolutePath || "");
  }
  return "";
}

function dataUrlFor(item) {
  const asPath = pathOfReference(item);
  if (!asPath) {
    // 可能是 http(s)/data URL 直接放在对象里
    const url = item && typeof item === "object" ? String(item.url || "") : "";
    if (/^(https?:|data:)/i.test(url)) return url;
    throw new Error(`认不出这张参考图：${JSON.stringify(item).slice(0, 160)}`);
  }
  if (/^(https?:|data:)/i.test(asPath)) return asPath;
  if (!isAbsolute(asPath)) throw new Error(`参考图不是绝对路径：${asPath}`);
  const buf = readFileSync(asPath);
  const mime = MIME_OF_EXT[extname(asPath).toLowerCase()] || "image/png";
  return `data:${mime};base64,${buf.toString("base64")}`;
}

export function createMagpieImageProvider({ fetchImpl, proxyBase, log = () => {}, warn = () => {} }) {
  if (typeof fetchImpl !== "function") throw new Error("createMagpieImageProvider 需要 fetchImpl");
  if (typeof proxyBase !== "function") throw new Error("createMagpieImageProvider 需要 proxyBase（本 App 代理进程的地址）");
  const proxyUrl = (p) => {
    const base = String(proxyBase() || "").replace(/\/+$/, "");
    if (!base) throw new Error("本 App 的代理进程还没就绪（没有端口），稍后再试");
    return base + p;
  };

  async function listDrawers() {
    const res = await fetchImpl(`${GATEWAY_BASE}/v1/models`, {
      headers: { "X-Magpie-Drawers": "1", accept: "application/json" },
      timeoutMs: 15_000,
      maxResponseBytes: 2 * 1024 * 1024,
      cacheTtlMs: 20_000,
    });
    if (!res.ok) throw new Error(`magpie 网关回了 ${res.status}（${GATEWAY_BASE}/v1/models）`);
    const j = JSON.parse(await res.text());
    const list = Array.isArray(j && j.data) ? j.data : [];
    return list.filter((m) => String((m && m.kind) || "") === "image");
  }

  function toModels(drawers) {
    return drawers.map((m) => {
      const accepts = Array.isArray(m && m.modalities && m.modalities.input)
        ? m.modalities.input.includes("image")
        : false;
      return {
        id: String(m.id),
        displayName: String(m.display_name || m.magpie_label || m.id),
        protocolId: PROTOCOL_ID,
        inputs: accepts ? ["text", "image"] : ["text"],
        outputs: ["image"],
        supportsAsync: true,
      };
    });
  }

  async function runJob(taskId, body, ctx) {
    try {
      const dataDir = String((ctx && ctx.dataDir) || "");
      const payload = {
        ...body,
        outDir: String((ctx && ctx.generatedDir) || ""),
        tmpDir: dataDir ? join(dataDir, "tmp", "draw-" + taskId) : "",
      };
      const res = await fetchImpl(proxyUrl("/_hana/draw"), {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
        timeoutMs: DRAW_TIMEOUT_MS,
        maxResponseBytes: 1024 * 1024,
      });
      const text = await res.text();
      let j = null;
      try { j = JSON.parse(text); } catch { throw new Error(`代理的回包不是 JSON：${String(text).slice(0, 200)}`); }
      if (!res.ok || !j || j.ok === false) {
        throw new Error((j && j.error) || `生图失败（HTTP ${res.status}）`);
      }
      const files = Array.isArray(j.files) ? j.files : [];
      if (!files.length) throw new Error("代理说成功了但没给出文件");
      jobs.set(taskId, { status: "success", files, createdAt: Date.now() });
      log(`生图完成：${body.model} → ${files.join(", ")}${j.ms ? `（${(j.ms / 1000).toFixed(1)}s）` : ""}`);
    } catch (e) {
      const message = e && e.message ? String(e.message) : String(e);
      jobs.set(taskId, { status: "failed", failReason: message, createdAt: Date.now() });
      warn(`生图失败：${message}`);
    }
  }

  // ── 运行时能力来源：模型表 ────────────────────────────────────────────────
  // 宿主会调 refresh({ providerId, capability })，要求回 { media: { <组>: { models } } }。
  const source = {
    async refresh() {
      const drawers = await listDrawers();
      const models = toModels(drawers);
      if (!models.length) {
        throw new Error(
          `magpie 现在没有能生图的模型（${GATEWAY_BASE} 只列出 ${drawers.length} 个可画模型）。`
          + "去 magpie 里签一个有生图能力的供应商，或在 Settings → Images 里选好模型。");
      }
      return {
        media: { imageGeneration: { defaultModelId: models[0].id, models } },
        fingerprint: { gateway: GATEWAY_BASE, models: models.map((m) => m.id).join(",") },
      };
    },
  };

  // ── 适配器 ────────────────────────────────────────────────────────────────
  const adapter = {
    id: ADAPTER_ID,
    protocolId: PROTOCOL_ID,
    name: "magpie Images",
    displayName: "magpie Images",
    types: ["image"],
    // 只声明我们真的能兑现的：参考图最多 4 张（magpie 一次最多 4 张图）。
    // 比例/分辨率不声明：magpie 那边由厂商定，声明了反而会给出兑现不了的选项。
    capabilities: { referenceImages: { min: 0, max: MAX_REFS } },

    async checkAuth() {
      try {
        const res = await fetchImpl(`${GATEWAY_BASE}/`, { timeoutMs: 4_000, maxResponseBytes: 65_536 });
        if (!res.ok) return { ok: false, code: "magpie_gateway", message: `magpie 网关回了 ${res.status}` };
        return { ok: true };
      } catch (e) {
        return {
          ok: false,
          code: "magpie_down",
          message: `连不上 magpie 网关（${GATEWAY_BASE}）：magpie 没在跑，或本插件还没起来。${e && e.message ? ` ${e.message}` : ""}`,
        };
      }
    },

    async submit(params = {}, ctx = {}) {
      sweepJobs();
      const prompt = String(pick(params.prompt) || "").trim();
      if (!prompt) throw new Error("生图需要 prompt");
      const model = String(pick(params.modelId, params.model) || "").trim();
      if (!model) {
        throw new Error(
          "没点名模型：magpie 的 WorkBuddy 生图花的是套餐积分，只在模型被点名时才用它。"
          + "请在生成时指定模型（providerId=magpie-hana）。");
      }
      const resolved = params.resolvedParameters && typeof params.resolvedParameters === "object"
        ? params.resolvedParameters
        : {};
      const options = params.options && typeof params.options === "object" ? params.options : {};

      const n = Number(pick(params.n, params.count, options.n, options.count, resolved.n, 1)) || 1;
      const size = String(pick(params.size, options.size, resolved.size) || "");
      const quality = String(pick(params.quality, options.quality, resolved.quality) || "");
      const background = String(pick(options.background, resolved.background) || "");
      const format = String(pick(params.output_format, options.output_format, resolved.output_format) || "");
      const ratio = String(pick(params.ratio, options.ratio, resolved.ratio) || "");
      const resolution = String(pick(params.resolution, options.resolution, resolved.resolution) || "");
      if ((ratio || resolution) && !size) {
        // 不假装做到了：magpie 这条通道只认 size（像素或 auto），比例由厂商定。
        warn(`magpie 通道不认 ratio=${ratio || "-"} / resolution=${resolution || "-"}，这次按厂商默认出图`);
      }

      const refs = referenceEntries(params);
      if (refs.length > MAX_REFS) throw new Error(`参考图最多 ${MAX_REFS} 张，给了 ${refs.length} 张`);
      const images = refs.map(dataUrlFor);

      const body = { model, prompt, n };
      if (size) body.size = size;
      if (quality) body.quality = quality;
      if (background) body.background = background;
      if (format) body.output_format = format;
      if (images.length) body.images = images;

      const taskId = `magpie-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
      jobs.set(taskId, { status: "pending", createdAt: Date.now() });
      log(`生图提交：${model}（n=${n}${size ? `, size=${size}` : ""}${images.length ? `, 参考图 ${images.length}` : ""}）`);
      void runJob(taskId, body, ctx);
      return { taskId };
    },

    async query(taskId) {
      sweepJobs();
      const job = jobs.get(String(taskId || ""));
      if (!job) {
        return {
          status: "failed",
          failReason: `找不到这次生成（${taskId}）：本插件可能重启过。重新生成一次即可。`,
          error: { code: "MAGPIE_NO_TASK", message: `找不到这次生成（${taskId}）：本插件可能重启过。` },
        };
      }
      if (job.status === "pending") return { status: "pending" };
      if (job.status === "success") return { status: "success", files: job.files };
      return {
        status: "failed",
        failReason: job.failReason,
        error: { code: "MAGPIE_DRAW_FAILED", message: job.failReason },
      };
    },
  };

  return { source, adapter };
}

/** 在 apply 里调一次：把能力来源与适配器交给宿主。
 *  注册本身不需要 magpie 在跑（模型表是刷的时候才去问网关）。 */
export async function registerMagpieMedia(sdk, { log = () => {}, warn = () => {}, proxyBase } = {}) {
  const { source, adapter } = createMagpieImageProvider({
    fetchImpl: (url, init) => sdk.network.fetch(url, init),
    proxyBase,
    log,
    warn,
  });
  await sdk.media.registerCapabilitySource(PROVIDER_ID, source);
  await sdk.media.registerAdapter(adapter);
  return { providerId: PROVIDER_ID, adapterId: ADAPTER_ID };
}
