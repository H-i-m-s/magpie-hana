// runtime/import-usage.mjs —— 「从本机导入用量信息」的引擎
// ─────────────────────────────────────────────────────────────────────────────
// 背景：magpie 支持便携目录，所以本 App 带的是**第二份** magpie，它的账本从零开始，
// Hana 自己那份历史（配额曲线、用量日志、路由记录）不会自己过来。这个模块把本机
// 另一份 magpie 的家目录读出来、并进本 App 这一份里。
//
// 三条纪律（和手工那次一样，不能因为做成按钮就放松）：
//   1. 源目录只读，一个字节都不写（下面每个写入都过 assertInside）。
//   2. 目标侧先整份备份到 <家目录>/import-backup/<时间戳>/，随时能还原。
//   3. 原子替换（写 .tmp 再 rename），且导入前先停 magpie，别让它从内存里回写覆盖。
//
// 幂等：重复点不会长出重复记录 —— 用量日志按 route_id（或 t+req+status）去重、
// 配额曲线按时间点 at 去重、路由记录按 id 去重。这点是这功能的命门。
//
// 要分享给别人用，所以源目录是**探测**出来的，不是写死的：
// 依次看 ~/.config/magpie、%APPDATA%\magpie、%LOCALAPPDATA%\magpie，
// 取第一个真的带 usage.jsonl 的；都不在就明确说找不到，不瞎猜。
import { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { homedir } from "node:os";
import { gunzipSync, gzipSync } from "node:zlib";

// 只并历史。身份、凭据、设置、插件状态一概不动（那些跟着各自的安装走）。
export const NOT_MERGED = [
  "install-id", "stats-sent", "start-menu-shortcut", "last-version", "migrations.json",
  "applied.json", "library.json", "plugins.json", "plugins", "providers.json", "logins.json",
  "plugin-auth.json", "plugin-providers.json", "plugin-updates.json", "plugin-icons.json",
  "cli-identity.json", "settings.json", "zcode-device-mid", "import-backup", "icons", "cache",
];

/** 候选目录。必须用**真实的**用户环境来算：本模块常跑在 App 的 runtime 子进程里，
 *  那里的 os.homedir() 不是用户的真实家目录（实测踩过：源那份根本看不见）。 */
export function sourceCandidates(userEnv) {
  const env = userEnv || {};
  const home = env.USERPROFILE
    || (env.HOMEDRIVE && env.HOMEPATH ? env.HOMEDRIVE + env.HOMEPATH : "")
    || homedir();
  const roaming = env.APPDATA || join(home, "AppData", "Roaming");
  const local = env.LOCALAPPDATA || join(home, "AppData", "Local");
  return [
    join(home, ".config", "magpie"),
    join(roaming, "magpie"),
    join(local, "magpie"),
    join(home, ".magpie"),
  ];
}

/** 找本机另一个 magpie 的家目录。跳过本 App 自己那份。 */
export function findSource(homeDir, userEnv) {
  const mine = resolve(homeDir).toLowerCase();
  const seen = new Set();
  const found = [];
  for (const cand of sourceCandidates(userEnv)) {
    const abs = resolve(cand);
    const key = abs.toLowerCase();
    if (seen.has(key) || key === mine) continue;
    seen.add(key);
    if (!existsSync(join(abs, "usage.jsonl"))) continue;
    let mtime = 0;
    try { mtime = statSync(join(abs, "usage.jsonl")).mtimeMs; } catch { /* 忽略 */ }
    found.push({ dir: abs, mtime, size: (() => { try { return statSync(join(abs, "usage.jsonl")).size; } catch { return 0; } })() });
  }
  // 有多份时取用量日志最新、最大的那份（老机器上可能留过废弃的目录）
  found.sort((a, b) => (b.mtime - a.mtime) || (b.size - a.size));
  return found;
}

const KEEP = ["usage.jsonl", "quota-history.json", "credits-daily.json", "affinity.json", "served.json",
  "qoder-checkin.json", "workbuddy-checkin.json", "routing"];

function readJson(p, fallback) {
  try { return JSON.parse(readFileSync(p, "utf8")); } catch { return fallback; }
}
function tsOf(s) {
  const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})?$/.exec(String(s || ""));
  if (!m) return NaN;
  return Date.parse(`${m[1]}.${((m[2] || "") + "00").slice(0, 3)}${m[3] || "Z"}`);
}
function usageKey(r) {
  if (r.route_id !== undefined && r.route_id !== null) return "id:" + r.route_id;
  return "k:" + [r.t, r.req, r.status, r.in, r.out].join("|");
}
function pointsOf(obj, meter) {
  const v = obj && obj[meter];
  return Array.isArray(v) ? v : [];
}

/**
 * 把 srcDir 的历史并进 homeDir（magpie 的家目录，即 <binDir>/data）。
 * apply=false 时只算不写（演练）。
 * 返回一份给人看的报告；出错时抛异常，由调用方转成界面上的失败提示。
 */
export function importUsage({ srcDir, homeDir, apply = false, log = () => {} }) {
  const SRC = resolve(srcDir);
  const DST = resolve(homeDir);
  const srcKey = SRC.toLowerCase(), dstKey = DST.toLowerCase();
  if (srcKey === dstKey || srcKey.startsWith(dstKey + "\\") || srcKey.startsWith(dstKey + "/")) {
    throw new Error("要导入的那份就是本 App 自己这份，没什么可并的");
  }
  if (!existsSync(join(SRC, "usage.jsonl"))) throw new Error(`那个目录里没有 usage.jsonl，不像一份 magpie 家目录：${SRC}`);
  mkdirSync(DST, { recursive: true });

  const assertInside = (p) => {
    const k = resolve(p).toLowerCase();
    if (k !== dstKey && !k.startsWith(dstKey + "\\") && !k.startsWith(dstKey + "/")) {
      throw new Error("拒绝写目标目录之外的路径：" + p);
    }
  };
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const BAK = join(DST, "import-backup", stamp);
  const written = [];
  const dry = !apply;

  function backup(p) {
    if (!existsSync(p)) return;
    const to = join(BAK, p.slice(DST.length + 1));
    mkdirSync(dirname(to), { recursive: true });
    copyFileSync(p, to);
  }
  function writeAtomic(p, data) {
    assertInside(p);
    // 新装的机器上 routing/ 这类子目录可能还不存在（手工那次目标碰巧已经有这个目录，
    // 所以没暴露）。写之前把父目录补齐。
    mkdirSync(dirname(p), { recursive: true });
    const tmp = p + ".hana-tmp";
    writeFileSync(tmp, data);
    renameSync(tmp, p);
    written.push(p);
  }
  function loadJsonl(p) {
    const recs = [];
    for (const line of readFileSync(p, "utf8").split(/\r?\n/)) {
      if (!line.trim()) continue;
      try { recs.push({ r: JSON.parse(line), l: line }); } catch { /* 坏行丢掉，不让它带坏整份文件 */ }
    }
    return recs;
  }

  const report = { source: SRC, home: DST, dry, backup: dry ? null : BAK, files: [], notes: [], skipped: NOT_MERGED };

  // ── 1. usage.jsonl ───────────────────────────────────────────────────────
  {
    const dstP = join(DST, "usage.jsonl");
    const srcRecs = loadJsonl(join(SRC, "usage.jsonl"));
    const dstRecs = existsSync(dstP) ? loadJsonl(dstP) : [];
    const seen = new Map();
    for (const x of dstRecs) seen.set(usageKey(x.r), x);
    let dupes = 0;
    for (const x of srcRecs) {
      const k = usageKey(x.r);
      if (seen.has(k)) { dupes++; continue; }
      seen.set(k, x);
    }
    const all = [...seen.values()].sort((a, b) => {
      const ta = tsOf(a.r.t), tb = tsOf(b.r.t);
      if (Number.isNaN(ta) || Number.isNaN(tb)) return 0;
      return ta - tb;
    });
    const text = all.map((x) => x.l).join("\n") + "\n";
    const first = all.length ? all[0].r.t : null;
    const last = all.length ? all[all.length - 1].r.t : null;
    report.files.push({ name: "usage.jsonl", added: all.length - dstRecs.length, duplicates: dupes, total: all.length, from: first, to: last });
    log(`usage.jsonl: 本机 ${srcRecs.length} 行 + 本 App ${dstRecs.length} 行，重复 ${dupes} → ${all.length} 行`);
    if (!dry && all.length !== dstRecs.length) { backup(dstP); writeAtomic(dstP, text); }
  }

  // ── 2. quota-history.json（同一账号同一计量，按 at 取并集）───────────────
  {
    const dstP = join(DST, "quota-history.json");
    const src = readJson(join(SRC, "quota-history.json"), {});
    const dst = readJson(dstP, {});
    const out = {};
    for (const acct of new Set([...Object.keys(src), ...Object.keys(dst)])) {
      const sm = src[acct] || {}, dm = dst[acct] || {};
      const meters = {};
      for (const k of new Set([...Object.keys(sm), ...Object.keys(dm)])) {
        const pts = new Map();
        for (const p of pointsOf(sm, k)) if (p && p.at) pts.set(p.at, p);
        for (const p of pointsOf(dm, k)) if (p && p.at) pts.set(p.at, p);
        meters[k] = [...pts.values()].sort((a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : 0));
      }
      out[acct] = meters;
      const before = Object.values(dm).reduce((n, a) => n + (Array.isArray(a) ? a.length : 0), 0);
      const after = Object.values(meters).reduce((n, a) => n + a.length, 0);
      report.files.push({ name: "quota-history.json", account: acct, added: after - before, total: after });
      log(`quota-history[${acct}]: ${before} 点 → ${after} 点`);
    }
    const changed = JSON.stringify(out) !== JSON.stringify(dst);
    if (!dry && changed) { backup(dstP); writeAtomic(dstP, JSON.stringify(out)); }
  }

  // ── 3. credits-daily.json（同一份额度：按天取较大的那个观测）────────────
  {
    const dstP = join(DST, "credits-daily.json");
    const src = readJson(join(SRC, "credits-daily.json"), {});
    const dst = readJson(dstP, {});
    const out = {};
    for (const acct of new Set([...Object.keys(src), ...Object.keys(dst)])) {
      const s = src[acct] || {}, d = dst[acct] || {};
      const days = {};
      for (const day of new Set([...Object.keys(s.days || {}), ...Object.keys(d.days || {})])) {
        const a = s.days?.[day], b = d.days?.[day];
        days[day] = a === undefined ? b : b === undefined ? a : Math.max(a, b);
      }
      const lastA = s.last || null, lastB = d.last || null;
      const last = !lastA ? lastB : !lastB ? lastA : (String(lastA.at) >= String(lastB.at) ? lastA : lastB);
      const since = [s.since, d.since].filter(Boolean).sort()[0] || null;
      out[acct] = { since, last, days };
      report.files.push({ name: "credits-daily.json", account: acct, added: Math.max(0, Object.keys(days).length - Object.keys(d.days || {}).length), days: Object.keys(days).length, since });
      log(`credits-daily[${acct}]: since ${d.since || "-"} → ${since}；天数 ${Object.keys(d.days || {}).length} → ${Object.keys(days).length}`);
    }
    const changed = JSON.stringify(out) !== JSON.stringify(dst);
    if (!dry && changed) { backup(dstP); writeAtomic(dstP, JSON.stringify(out)); }
  }

  // ── 4. affinity.json（粘性路由：并集，本 App 那份优先）───────────────────
  {
    const dstP = join(DST, "affinity.json");
    const src = readJson(join(SRC, "affinity.json"), {});
    const dst = readJson(dstP, {});
    const out = { ...src, ...dst };
    report.files.push({ name: "affinity.json", added: Object.keys(out).length - Object.keys(dst).length, total: Object.keys(out).length });
    log(`affinity.json: ${Object.keys(dst).length} 键 → ${Object.keys(out).length} 键`);
    if (!dry && Object.keys(out).length !== Object.keys(dst).length) { backup(dstP); writeAtomic(dstP, JSON.stringify(out)); }
  }

  // ── 5. served.json（取较晚的那次）────────────────────────────────────────
  {
    const dstP = join(DST, "served.json");
    const src = readJson(join(SRC, "served.json"), {});
    const dst = readJson(dstP, {});
    const out = { ...src, ...dst };
    for (const k of Object.keys(src)) if (k in dst) out[k] = String(src[k]) > String(dst[k]) ? src[k] : dst[k];
    if (!dry && JSON.stringify(out) !== JSON.stringify(dst)) { backup(dstP); writeAtomic(dstP, JSON.stringify(out)); }
  }

  // ── 6. 签到记录（各自只留最新一条，取较新的）─────────────────────────────
  for (const name of ["qoder-checkin.json", "workbuddy-checkin.json"]) {
    const srcP = join(SRC, name), dstP = join(DST, name);
    if (!existsSync(srcP)) continue;
    const src = readJson(srcP, {}), dst = readJson(dstP, {});
    const out = { ...src, ...dst };
    for (const acct of Object.keys(src)) {
      if (!(acct in dst)) continue;
      out[acct] = String(src[acct]?.at || "") > String(dst[acct]?.at || "") ? src[acct] : dst[acct];
    }
    const changed = JSON.stringify(out) !== JSON.stringify(dst);
    if (changed) report.files.push({ name, kept: Object.keys(out).length });
    if (!dry && changed) { backup(dstP); writeAtomic(dstP, JSON.stringify(out)); }
  }

  // ── 7. routing/（缺的天整份搬，重名的按 id 去重合并）────────────────────
  {
    const srcDir = join(SRC, "routing"), dstDir = join(DST, "routing");
    if (existsSync(srcDir)) {
      const dayRe = /^(\d{4}-\d{2}-\d{2})\.jsonl(\.gz)?$/;
      const dstDays = new Map();
      if (existsSync(dstDir)) {
        for (const f of readdirSync(dstDir)) {
          const m = dayRe.exec(f);
          if (m) dstDays.set(m[1], join(dstDir, f));
        }
      }
      const readLines = (p) => {
        const buf = readFileSync(p);
        const text = p.endsWith(".gz") ? gunzipSync(buf).toString("utf8") : buf.toString("utf8");
        return text.split(/\r?\n/).filter((l) => l.trim());
      };
      const idOf = (l) => { try { const j = JSON.parse(l); return j.id === undefined ? null : j.id; } catch { return null; } };
      const keyOf = (l) => { const id = idOf(l); return id === null ? "line:" + l : "id:" + id; };
      const byId = (a, b) => { const x = idOf(a), y = idOf(b); return (x === null || y === null) ? 0 : (x - y); };

      for (const f of readdirSync(srcDir)) {
        const m = dayRe.exec(f);
        if (!m) continue;
        const day = m[1];
        const srcP = join(srcDir, f);
        const dstP = dstDays.get(day);
        if (!dstP) {
          const to = join(dstDir, day + ".jsonl" + (m[2] ? ".gz" : ""));
          const lines = readLines(srcP);
          report.files.push({ name: `routing/${day}`, added: lines.length, total: lines.length });
          log(`routing/${day}: 本 App 没有 → 整份导入（${lines.length} 行）`);
          if (!dry) {
            assertInside(to);
            writeAtomic(to, m[2] ? gzipSync(Buffer.from(lines.join("\n") + "\n", "utf8")) : lines.join("\n") + "\n");
          }
          continue;
        }
        const sLines = readLines(srcP), dLines = readLines(dstP);
        const seen = new Map();
        for (const l of dLines) seen.set(keyOf(l), l);
        let dupes = 0;
        for (const l of sLines) {
          const k = keyOf(l);
          if (seen.has(k)) { dupes++; continue; }
          seen.set(k, l);
        }
        const all = [...seen.values()].sort(byId);
        const text = all.join("\n") + "\n";
        report.files.push({ name: `routing/${day}`, added: all.length - dLines.length, duplicates: dupes, total: all.length });
        log(`routing/${day}: 本机 ${sLines.length} 行 + 本 App ${dLines.length} 行，重复 ${dupes} → ${all.length} 行`);
        if (!dry && all.length !== dLines.length) {
          backup(dstP);
          writeAtomic(dstP, dstP.endsWith(".gz") ? gzipSync(Buffer.from(text, "utf8")) : text);
        }
      }
    }
  }

  const added = report.files.reduce((n, f) => n + (f.added || 0), 0);
  report.added = added;
  report.written = written;
  if (!added) report.notes.push("本机那份没有本 App 还没有的记录，没有变化");
  return report;
}
