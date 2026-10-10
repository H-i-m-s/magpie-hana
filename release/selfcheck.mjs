// SPDX-License-Identifier: MPL-2.0
//
// 本文件的结构与部分实现取自 H-i-m-s/git-save-load 的 release/selfcheck.mjs；
// 那部分又衍生自 GitHana（Copyright (c) 2026 Nyasers，以 MPL-2.0 授权）。
// 本文件已由 magpie-hana 按本插件的结构重写，修改后的版本同样以 MPL-2.0 发布。
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// This Source Code Form is "Incompatible With Secondary Licenses", as
// defined by the Mozilla Public License, v. 2.0.
//
// release/selfcheck.mjs — Magpie for Hana 的出包前自检。
//
// 整个 release/ 里跟插件绑得最紧的两个文件，一个是 config.json，另一个就是它。
//
// 只做「不依赖任何外部工具也能判定、坏了就是坏了」的那部分，给发版一个实际门槛：
//   1) manifest.json 能解析、必备字段齐全，entry 与 icon 指向的文件都在
//   2) manifest 声明的每个 UI route（卡片、设置页）都有对应的 ui/ 页面
//   3) entry、runtime/、media/、ui/ 下的 .js 与 .mjs 语法通过；
//      ui/*.html 里的内联 <script> 一并过一遍（这张 App 的逻辑大半写在卡片与设置页里）
//   4) 随包携带的 magpie：exe、它的 MIT 许可证、版本号文件都在
//
// 警告不拦发布：它们是「宿主会怎么处理」的提示，不影响这个包能不能装。
//
// 用法：
//   node release/selfcheck.mjs          # 人类可读，失败以非零码退出
//   node release/selfcheck.mjs --json   # { ok, errors, warnings, files }
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const RELEASE_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(RELEASE_DIR, "..");
const JSON_MODE = process.argv.slice(2).includes("--json");

const errors = [];
const warnings = [];
const rel = (p) => relative(ROOT, p).split(sep).join("/");
const fail = (m) => errors.push(m);
const warn = (m) => warnings.push(m);

/** 宿主对卡片形态的取值。写成别的，宿主会「当它没声明」并退回默认，不报错。 */
const CARD_FORMS = new Set(["framed", "flush"]);

// ── 1) manifest.json 与它指向的文件 ─────────────────────────────────────────
let manifest = null;
try {
  manifest = JSON.parse(readFileSync(join(ROOT, "manifest.json"), "utf8"));
} catch (e) {
  fail(`manifest.json 无法解析：${(e && e.message) || e}`);
}

const REQUIRED_FIELDS = ["manifestVersion", "id", "name", "version", "entry", "icon"];
if (manifest) {
  for (const field of REQUIRED_FIELDS) {
    const value = manifest[field];
    if (value === undefined || value === null || value === "") fail(`manifest.json 缺少必备字段：${field}`);
  }
  if (typeof manifest.entry === "string" && manifest.entry && !existsSync(join(ROOT, manifest.entry))) {
    fail(`entry 指向的文件不存在：${manifest.entry}`);
  }
  if (typeof manifest.icon === "string" && manifest.icon && !existsSync(join(ROOT, manifest.icon))) {
    fail(`icon 指向的文件不存在：${manifest.icon}`);
  }
  if (manifest.manifestVersion !== 2) warn(`manifestVersion 是 ${JSON.stringify(manifest.manifestVersion)}，本仓库按 v2 维护`);

  // 卡片形态：写错不报错，只会静默退回默认，所以在这里说一声。
  const cards = Array.isArray(manifest.contributes?.cards) ? manifest.contributes.cards : [];
  for (const card of cards) {
    const form = card?.cardForm;
    if (form !== undefined && !CARD_FORMS.has(form)) {
      warn(`contributes.cards[${card.id ?? "?"}].cardForm 是 ${JSON.stringify(form)}，宿主只认 framed / flush，会当成没声明（现按默认渲染）`);
    }
  }
}

// ── 2) manifest 声明的 UI route 必须有页面 ──────────────────────────────────
if (manifest) {
  const contributes = manifest.contributes || {};
  const routeDecls = [];
  const settingsRoute = contributes.settings?.ui?.route;
  if (typeof settingsRoute === "string") routeDecls.push({ route: settingsRoute, from: "contributes.settings.ui.route" });
  const cards = Array.isArray(contributes.cards) ? contributes.cards : [];
  for (const card of cards) {
    if (typeof card?.route === "string") routeDecls.push({ route: card.route, from: `contributes.cards[${card.id ?? "?"}].route` });
    const fp = card?.functionPanel?.route;
    if (typeof fp === "string") routeDecls.push({ route: fp, from: `contributes.cards[${card.id ?? "?"}].functionPanel.route` });
  }
  for (const { route, from } of routeDecls) {
    const file = join(ROOT, "ui", route.replace(/^\/+/, ""));
    if (!existsSync(file)) fail(`${from} 指向的页面不存在：ui/${route.replace(/^\/+/, "")}`);
  }
}

// ── 3) 语法检查（文件 + ui/*.html 里的内联脚本）──────────────────────────────
/** 用 stdin + 显式 --input-type 检一段源码，两种模式各试一遍。
 *
 * 不用「node --check <文件>」：Node 对含 ESM 语法的无 type 字段 .js 会走模块自动探测，
 * 即便后面真有语法错误也可能返回 0，会漏报。
 */
function checkSource(src, label) {
  const run = (inputType) => {
    try {
      execFileSync(process.execPath, [`--input-type=${inputType}`, "--check"], {
        input: src,
        stdio: ["pipe", "pipe", "pipe"],
      });
      return { ok: true };
    } catch (e) {
      return { ok: false, out: [e.stdout, e.stderr].map((b) => String(b || "")).join("").trim() };
    }
  };
  const asEsm = run("module");
  if (asEsm.ok) return { ok: true };
  const asCjs = run("commonjs");
  if (asCjs.ok) return { ok: true };
  const looksEsm = /\b(import|export)\b/.test(src.toString("utf8"));
  return { ok: false, out: (looksEsm ? asEsm.out : asCjs.out) || "" };
}

function collectFiles(dir, out = []) {
  if (!existsSync(dir)) return out;
  for (const name of readdirSync(dir)) {
    if (name === "node_modules") continue;
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) collectFiles(p, out);
    else if (/\.(js|mjs)$/.test(name)) out.push(p);
  }
  return out;
}

const jsFiles = [];
const seen = new Set();
const addJs = (p) => {
  if (existsSync(p) && !seen.has(p)) {
    seen.add(p);
    jsFiles.push(p);
  }
};
if (typeof manifest?.entry === "string" && manifest.entry) addJs(join(ROOT, manifest.entry));
for (const dir of ["runtime", "media", "ui"]) for (const f of collectFiles(join(ROOT, dir))) addJs(f);

for (const file of jsFiles) {
  const r = checkSource(readFileSync(file), file);
  if (!r.ok) fail(`语法检查失败：${rel(file)}\n    ${(r.out || "").split("\n").slice(0, 3).join("\n    ")}`);
}

// 卡片与设置页的逻辑大半是内联脚本，漏掉它们等于没检。
const htmlPages = existsSync(join(ROOT, "ui"))
  ? readdirSync(join(ROOT, "ui")).filter((n) => n.endsWith(".html")).map((n) => join(ROOT, "ui", n))
  : [];
let inlineCount = 0;
for (const page of htmlPages) {
  const html = readFileSync(page, "utf8");
  const re = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
  let m;
  let i = 0;
  while ((m = re.exec(html))) {
    const attrs = m[1] || "";
    i += 1;
    if (/\bsrc\s*=/i.test(attrs)) continue;              // 外链脚本由上面的文件检查覆盖
    if (/\btype\s*=\s*["']?(?!text\/javascript|application\/javascript|module)/i.test(attrs)) continue;
    const body = m[2];
    if (!body.trim()) continue;
    inlineCount += 1;
    const r = checkSource(body, page);
    if (!r.ok) {
      fail(`语法检查失败：${rel(page)} 第 ${i} 个内联 <script>\n    ${(r.out || "").split("\n").slice(0, 3).join("\n    ")}`);
    }
  }
}

// ── 4) 随包携带的 magpie ────────────────────────────────────────────────────
const VENDOR = join(ROOT, "vendor");
const exe = join(VENDOR, "magpie-windows-amd64.exe");
if (!existsSync(exe)) {
  fail("vendor/magpie-windows-amd64.exe 不存在；没带 exe 的包装上也起不来");
} else {
  const size = statSync(exe).size;
  if (size < 10 * 1024 * 1024) fail(`vendor 里的 exe 只有 ${(size / 1024 / 1024).toFixed(2)} MiB，看起来不是完整件`);
}
const magpieLicense = join(VENDOR, "LICENSE-magpie");
if (!existsSync(magpieLicense)) {
  fail("vendor/LICENSE-magpie 不存在：magpie 以 MIT 授权，随包分发必须带着它的许可证");
} else if (!/MIT License/i.test(readFileSync(magpieLicense, "utf8"))) {
  fail("vendor/LICENSE-magpie 里没有看到 MIT License 字样，确认是不是拿错了文件");
}
if (!existsSync(join(VENDOR, "version.txt"))) warn("vendor/version.txt 不在，出包后就不知道带的是哪一版 magpie");

// ── 输出 ───────────────────────────────────────────────────────────────────
const ok = errors.length === 0;
const checked = jsFiles.length + inlineCount;

if (JSON_MODE) {
  console.log(JSON.stringify({ ok, errors, warnings, files: checked }, null, 2));
} else {
  for (const w of warnings) console.log(`  warn: ${w}`);
  for (const e of errors) console.error(`  FAIL: ${e}`);
  console.log(
    `[selfcheck] 检查 ${jsFiles.length} 个文件 + ${inlineCount} 段内联脚本 · 错误 ${errors.length} · 警告 ${warnings.length} · ${ok ? "OK" : "FAILED"}`,
  );
}

process.exit(ok ? 0 : 1);
