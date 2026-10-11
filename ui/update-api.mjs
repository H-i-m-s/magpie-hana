// ui/update-api.mjs — 更新说明的取数层。
//
// 只管「怎么问插件要快照、怎么回报看了没」，不管界面长什么样（界面在 ui/update-notice.mjs）。
// 路由与服务端逻辑在 index.js，形态跟 token-tracker-app 那边一致：
//   GET  /update-check          → { update: 快照 }
//   GET  /update-check?force=1  → 强制去 GitHub 取一次
//   GET  /update-check?cached=1 → 只交手里那份，一次网络都不出
//   POST /update-check          → { action: "ack" | "later" | "toggle" | "interval" }
//
// 为什么要收一个 fetchJson 进来而不是自己发请求：本 App 的两个界面拿到的通道不一样。
//   设置页：跑在宿主的沙箱 iframe 里，有 SDK（import { hana } from "./assets/sdk.js"），
//           请求要带宿主下发的 appSurfaceSession 凭据头，走 hana.api.fetch。
//   卡片壳子：route 型文档，宿主不给它注入 SDK（实测 window.hana 不存在），凭据得自己
//           从自己 URL 的查询串里取，见 ui/card.html 里那段阶梯。
// 把通道当参数收进来，两边各喂各的，这一层就能原样共用。

export function createUpdateApi({ fetchJson } = {}) {
  if (typeof fetchJson !== "function") {
    throw new Error("createUpdateApi 需要一个 fetchJson(path, init)");
  }

  // 服务端的应答形状是 { ok, update }（出错是 { ok: false, error }）。
  // 拿不到快照就抛出去，不静默返回 undefined —— 界面会把 message 显示在弹窗里。
  async function call(path, init) {
    const data = await fetchJson(path, init);
    if (!data || !data.update) {
      throw new Error((data && data.error) || "服务端没有给出更新快照");
    }
    return data.update;
  }

  /** 改自动检查的间隔（分钟）。服务端校验，非法值会原样报回来。 */
  function setUpdateInterval(minutes) {
    return dismissUpdate({ action: "interval", minutes });
  }

  /**
   * action：
   *   ack      = 我已知晓（落盘记住这个版本，直到发了下一版才再提）
   *   later    = 先不看（只记在插件进程内存里，App 一重启就忘）
   *   toggle   = 更新提示开关
   *   interval = 自动检查的间隔分钟数
   */
  function dismissUpdate({ action, version = "", enabled = null, minutes = null } = {}) {
    const body = { action };
    if (version) body.version = version;
    if (enabled !== null) body.enabled = !!enabled;
    if (minutes !== null) body.minutes = minutes;
    return call("/update-check", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  return {
    getUpdateCheck({ force = false, cached = false } = {}) {
      const qs = [];
      if (force) qs.push("force=1");
      if (cached) qs.push("cached=1");
      return call(`/update-check${qs.length ? "?" + qs.join("&") : ""}`);
    },
    dismissUpdate,
    setUpdateInterval,
  };
}
