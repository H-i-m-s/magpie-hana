// tools/selftest.mjs — 隔离自测：验证「代理 spawn magpie + 取 key + 反代 + 注入」
// 用法：node tools/selftest.mjs <exe路径> <工作目录>
// 全程只用传入的隔离目录，不碰用户的任何东西。
import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROXY = join(__dirname, "..", "runtime", "proxy.mjs");

const exe = process.argv[2];
const work = process.argv[3];
if (!exe || !work) {
  console.error("用法: node tools/selftest.mjs <exe> <workdir>");
  process.exit(2);
}

const port = 41500 + Math.floor(Math.random() * 3000);
const marker = "SELFTEST_READY";
mkdirSync(join(work, "bin"), { recursive: true });

const child = spawn(process.execPath, [
  PROXY,
  `--exe=${exe}`,
  `--cwd=${join(work, "bin")}`,
  `--port=${port}`,
  `--marker=${marker}`,
], { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });

let out = "";
child.stdout.on("data", (d) => { out += d.toString(); process.stdout.write("[proxy] " + d.toString()); });
child.stderr.on("data", (d) => process.stdout.write("[proxy:err] " + d.toString()));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function j(path) {
  const r = await fetch(`http://127.0.0.1:${port}${path}`);
  return { status: r.status, headers: Object.fromEntries(r.headers), body: await r.text() };
}

(async () => {
  const deadline = Date.now() + 60000;
  let st = null;
  while (Date.now() < deadline) {
    try {
      const r = await j("/_hana/status");
      st = JSON.parse(r.body);
      if (st.phase === "ready" && st.upstreamPort) break;
      if (st.phase === "error") { console.error("代理报错：" + st.error); }
    } catch { /* 还没起来 */ }
    await sleep(500);
  }
  if (!st || st.phase !== "ready") {
    console.error("失败：未在 60s 内就绪。最后状态：" + JSON.stringify(st));
    child.kill();
    process.exit(1);
  }
  console.log("\n=== 状态 ===");
  console.log(JSON.stringify(st, null, 2));

  // 1) 未注入时抓首页
  const home = await j("/");
  console.log("\n=== GET / ===");
  console.log("status:", home.status, "| content-type:", home.headers["content-type"]);
  const hasHide = /hana-hide/.test(home.body);
  const hasTheme = /hana-theme/.test(home.body);
  console.log("含 hana-hide 注入:", hasHide);
  console.log("含 hana-theme 注入:", hasTheme);

  // 2) 推主题 + 隐藏
  await fetch(`http://127.0.0.1:${port}/_hana/theme`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ css: ":root { --accent: #ff00aa; }", appearance: "dark" }),
  });
  await fetch(`http://127.0.0.1:${port}/_hana/hide`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ hidden: ["library", "sessions", "routing"] }),
  });

  const home2 = await j("/");
  console.log("\n=== 注入后 GET / ===");
  console.log("含 hana-theme:", /hana-theme/.test(home2.body));
  console.log("含 --accent: #ff00aa:", /--accent:\s*#ff00aa/.test(home2.body));
  console.log("含 hana-hide:", /hana-hide/.test(home2.body));
  console.log("含 routing 规则:", /data-ptab="routing"/.test(home2.body));
  console.log("含 data-theme=dark:", /data-theme="dark"/.test(home2.body));

  // 3) 验证 API 透传（带 key 转发是否成功）
  const api = await j("/api/state");
  console.log("\n=== GET /api/state（验证 key 转发）===");
  console.log("status:", api.status);
  let apiOk = false;
  try { const o = JSON.parse(api.body); apiOk = !!o && !/unauthorized|key/i.test(api.body.slice(0, 200)); } catch { /* 非 JSON */ }
  console.log("响应前 180 字:", api.body.slice(0, 180).replace(/\s+/g, " "));

  // 4) 静态资源透传
  const css = await j("/app.css");
  console.log("\n=== GET /app.css ===");
  console.log("status:", css.status, "| bytes:", css.body.length);

  // 5) 磁盘留痕检查
  console.log("\n=== 便携 data/ 是否被写 ===");
  const { readdirSync } = await import("node:fs");
  try { console.log(readdirSync(join(work, "bin", "data")).join(", ")); } catch (e) { console.log("(无 data 目录)"); }

  child.kill();
  await sleep(400);
  console.log("\n自测完成。");
  process.exit(0);
})();
