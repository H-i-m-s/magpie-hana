// scripts/pack.mjs — magpie-hana 出包（零依赖，不调外部 zip / 不用 npm 库）
//
// 为什么不用官方 pack_app.mjs：它会把 .git/、doc/、tools/ 一并打进归档。
// 安装位的 App 用不到这些，还会把仓库历史带出去。
//
// 产物（dist/）：
//   app-magpie-hana-<version>.zip            归档
//   app-magpie-hana-<version>.zip.sha256     sha256 校验值
//   app-magpie-hana-<version>.entry.json     市场条目（archive.url 用 {{BASE_URL}} 占位）
//
// 用法：
//   node scripts/pack.mjs                      出包到 <app>/dist
//   node scripts/pack.mjs --out <dir>          自定义输出目录
//   node scripts/pack.mjs --publisher <name>   指定 entry.json 的 publisher
import { createHash } from "node:crypto";
import { deflateRawSync, crc32 } from "node:zlib";
import { mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/** 整目录排除：开发脚本、仓库元数据、测试与产物都不该进包。 */
const SKIP_DIRS = new Set([".git", ".github", "node_modules", "dist", "scripts", "tools", "tests", "doc"]);
/** 临时/系统文件。 */
const SKIP_FILE_RE = /^(\.DS_Store|Thumbs\.db|desktop\.ini)$|\.(tmp|temp|swp|swo|bak)$|~$/i;

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith("--") ? process.argv[i + 1] : null;
}

const readJson = (p) => JSON.parse(readFileSync(p, "utf8"));

// ── 遍历要打包的文件 ─────────────────────────────────────────────────────────
function walk(dir, base = ROOT, out = []) {
  for (const name of readdirSync(dir)) {
    if (SKIP_FILE_RE.test(name)) continue;
    const abs = join(dir, name);
    const rel = relative(base, abs).split(sep).join("/");
    const st = statSync(abs);
    if (st.isDirectory()) {
      if (SKIP_DIRS.has(name) && dir === base) continue;
      if (name === ".git") continue;
      walk(abs, base, out);
    } else if (st.isFile()) {
      out.push({ abs, rel, size: st.size });
    }
  }
  return out;
}

// ── 最小 ZIP 写入（stored / deflate，用 Node 自带 zlib）─────────────────────
function zipWrite(files) {
  const localParts = [];
  const centralParts = [];
  let offset = 0;

  for (const f of files) {
    const raw = readFileSync(f.abs);
    const deflated = deflateRawSync(raw, { level: 9 });
    const useDeflate = deflated.length < raw.length;
    const body = useDeflate ? deflated : raw;
    const method = useDeflate ? 8 : 0;
    const crc = crc32(raw) >>> 0;
    const nameBuf = Buffer.from(f.rel, "utf8");

    const lh = Buffer.alloc(30);
    lh.writeUInt32LE(0x04034b50, 0);
    lh.writeUInt16LE(20, 4);            // version needed
    lh.writeUInt16LE(0x0800, 6);        // flag: UTF-8 名称
    lh.writeUInt16LE(method, 8);
    lh.writeUInt16LE(0, 10);            // time
    lh.writeUInt16LE(0x21, 12);         // date (1980-01-01, 固定值保证可复现)
    lh.writeUInt32LE(crc, 14);
    lh.writeUInt32LE(body.length, 18);
    lh.writeUInt32LE(raw.length, 22);
    lh.writeUInt16LE(nameBuf.length, 26);
    lh.writeUInt16LE(0, 28);
    localParts.push(lh, nameBuf, body);

    const ch = Buffer.alloc(46);
    ch.writeUInt32LE(0x02014b50, 0);
    ch.writeUInt16LE(20, 4);
    ch.writeUInt16LE(20, 6);
    ch.writeUInt16LE(0x0800, 8);
    ch.writeUInt16LE(method, 10);
    ch.writeUInt16LE(0, 12);
    ch.writeUInt16LE(0x21, 14);
    ch.writeUInt32LE(crc, 16);
    ch.writeUInt32LE(body.length, 20);
    ch.writeUInt32LE(raw.length, 24);
    ch.writeUInt16LE(nameBuf.length, 28);
    ch.writeUInt16LE(0, 30);
    ch.writeUInt16LE(0, 32);
    ch.writeUInt16LE(0, 34);
    ch.writeUInt16LE(0, 36);
    ch.writeUInt32LE(0, 38);
    ch.writeUInt32LE(offset, 42);
    centralParts.push(ch, nameBuf);

    offset += lh.length + nameBuf.length + body.length;
  }

  const central = Buffer.concat(centralParts);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(files.length, 8);
  eocd.writeUInt16LE(files.length, 10);
  eocd.writeUInt32LE(central.length, 12);
  eocd.writeUInt32LE(offset, 16);
  eocd.writeUInt16LE(0, 20);

  return Buffer.concat([...localParts, central, eocd]);
}

// ── 主流程 ───────────────────────────────────────────────────────────────────
const manifest = readJson(join(ROOT, "manifest.json"));
const id = manifest.id;
const version = manifest.version;
const publisher = arg("--publisher") || "H-i-M-s";
const outDir = resolve(arg("--out") || join(ROOT, "dist"));

console.log(`[pack] ${id} v${version}`);
const files = walk(ROOT);
files.sort((a, b) => (a.rel < b.rel ? -1 : 1));
console.log(`[pack] ${files.length} 个文件，${(files.reduce((s, f) => s + f.size, 0) / 1024 / 1024).toFixed(2)} MiB 原始`);

// 归档里要求有顶层目录 <id>/，宿主安装时剥壳
const prefixed = files.map((f) => ({ ...f, rel: `${id}/${f.rel}` }));
const zipBuf = zipWrite(prefixed);

mkdirSync(outDir, { recursive: true });
const zipName = `${id}-v${version}.zip`;
const zipPath = join(outDir, zipName);
writeFileSync(zipPath, zipBuf);
const sha = createHash("sha256").update(zipBuf).digest("hex");
writeFileSync(`${zipPath}.sha256`, `${sha}\n`);

const entry = {
  kind: "app",
  id,
  name: manifest.name,
  publisher: publisher,
  description: manifest.description || "",
  version,
  permissions: (manifest.capabilities || []).map((c) => ({ capability: c })),
  compatibility: { minAppVersion: manifest.minAppVersion },
  archive: {
    url: `{{BASE_URL}}/${zipName}`,
    sha256: sha,
    size: zipBuf.length,
    format: "zip",
  },
};
writeFileSync(join(outDir, `${id}-v${version}.entry.json`), JSON.stringify(entry, null, 2) + "\n");

console.log(`[pack] 写出 ${zipName}  (${(zipBuf.length / 1024 / 1024).toFixed(2)} MiB)`);
console.log(`[pack] sha256 ${sha}`);
console.log(`[pack] 写出 ${id}-v${version}.entry.json`);
