import { readFile, stat } from "fs/promises";
import path from "path";
import { spawn } from "child_process";
import { spawnSync } from "child_process";

const PYTHON_BIN = process.env.PYTHON_BIN || "python";
const SPOT_SCRIPT =
  process.env.SCRAPE_SPOT_PATH ||
  "C:\\Users\\mokkapatin\\Downloads\\options_database\\scrape_spot.py";
const BID_SCRIPT =
  process.env.SCRAPE_BID_PATH ||
  "C:\\Users\\mokkapatin\\Downloads\\options_database\\scrape_bid_yf.py";
const BID_CSV =
  process.env.SCRAPE_BID_OUTPUT ||
  "C:\\Users\\mokkapatin\\Downloads\\options_database\\scrape_bid_yf.csv";

const DEFAULT_CACHE_MS = Number(process.env.SCRAPE_CACHE_MS || 15 * 60 * 1000); // 15 minutes
const DEFAULT_TIMEOUT_MS = Number(process.env.SCRAPE_TIMEOUT_MS || 240000); // 4 minutes
const DEFAULT_MONEYNESS = Number(process.env.MONEYNESS_THRESHOLD || 0.85);
const DEFAULT_LIMIT = Number(process.env.METRICS_LIMIT || 500);

let inflightRefresh = null;
let resolvedPython = null;

function numberOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(String(v).trim().replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }

  out.push(cur);
  return out;
}

function parseCsv(text) {
  if (!text) return [];
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.replace(/\r$/, ""))
    .filter((l) => l.trim().length > 0);
  if (lines.length < 2) return [];

  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cells = splitCsvLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = cells[idx] ?? "";
    });
    return row;
  });
}

function normalizeRow(row) {
  const roiAnnRaw =
    row.roi_annualized ??
    row.roi_annual ??
    row.roi_annua ??
    row.roiAnnualized ??
    row["roi annualized"];

  return {
    trade_dt: row.trade_dt || row.date || null,
    ticker: row.ticker?.trim() || null,
    spot: numberOrNull(row.spot),
    strike: numberOrNull(row.strike),
    moneyness: numberOrNull(row.moneyness),
    premium: numberOrNull(row.premium),
    iv: numberOrNull(row.iv),
    delta: numberOrNull(row.delta),
    roi: numberOrNull(row.roi),
    roi_annualized: numberOrNull(roiAnnRaw),
    option_ticker: row.option_ticker || row.option_tic || row.contractSymbol || null,
    expiry: row.expiry || row.exp || null,
  };
}

async function runPython(scriptPath, label, timeoutMs = DEFAULT_TIMEOUT_MS) {
  if (!resolvedPython) {
    resolvedPython = resolvePython();
  }

  if (!resolvedPython) {
    throw new Error(
      `No Python interpreter found. Set PYTHON_BIN to a valid executable (e.g. C:\\\\Python311\\\\python.exe). Tried: ${pythonCandidates()
        .map((c) => c.label)
        .join(", ")}`
    );
  }

  const { cmd, argsPrefix, useShell } = resolvedPython;
  const cwd = path.dirname(scriptPath);
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, [...argsPrefix, scriptPath], { cwd, shell: useShell });
    let stdout = "";
    let stderr = "";
    const to = setTimeout(() => {
      child.kill();
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    child.stdout.on("data", (d) => {
      stdout += d.toString();
    });
    child.stderr.on("data", (d) => {
      stderr += d.toString();
    });
    child.on("error", (err) => {
      clearTimeout(to);
      reject(err);
    });
    child.on("close", (code) => {
      clearTimeout(to);
      if (code !== 0) {
        const err = new Error(`${label} exited with code ${code}`);
        err.stdout = stdout;
        err.stderr = stderr;
        reject(err);
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

function pythonCandidates() {
  const envBin = process.env.PYTHON_BIN?.trim();
  const list = [];
  if (envBin) list.push({ cmd: envBin, argsPrefix: [], label: envBin });
  list.push({ cmd: "python", argsPrefix: [], label: "python" });
  list.push({ cmd: "python3", argsPrefix: [], label: "python3" });
  list.push({ cmd: "py", argsPrefix: [], label: "py" });
  list.push({ cmd: "py", argsPrefix: ["-3"], label: "py -3" });
  return list;
}

function resolvePython() {
  for (const cand of pythonCandidates()) {
    try {
      const check = spawnSync(cand.cmd, [...cand.argsPrefix, "--version"], {
        stdio: "ignore",
        shell: cand.cmd === "py" && process.platform !== "win32" ? true : false,
      });
      if (check.status === 0) {
        return { ...cand, useShell: cand.cmd === "py" && process.platform !== "win32" ? true : false };
      }
    } catch {
      // try next candidate
    }
  }
  return null;
}

async function isFresh(filePath, maxAgeMs) {
  try {
    const info = await stat(filePath);
    return Date.now() - info.mtimeMs < maxAgeMs;
  } catch {
    return false;
  }
}

function trimLog(s, maxLen = 4000) {
  if (!s) return "";
  return s.length > maxLen ? s.slice(s.length - maxLen) : s;
}

async function refreshData({ force, maxAgeMs }) {
  if (inflightRefresh) {
    return inflightRefresh;
  }

  inflightRefresh = (async () => {
    const freshEnough = !force && (await isFresh(BID_CSV, maxAgeMs));
    if (freshEnough) return { refreshed: false, logs: [] };

    const logs = [];

    const spotRun = await runPython(SPOT_SCRIPT, "scrape_spot.py");
    logs.push(trimLog(spotRun.stdout));
    if (spotRun.stderr) logs.push(trimLog(spotRun.stderr));

    const bidRun = await runPython(BID_SCRIPT, "scrape_bid_yf.py");
    logs.push(trimLog(bidRun.stdout));
    if (bidRun.stderr) logs.push(trimLog(bidRun.stderr));

    return { refreshed: true, logs };
  })();

  try {
    return await inflightRefresh;
  } finally {
    inflightRefresh = null;
  }
}

function pickPositive(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export async function loadYfMetrics({
  moneynessFloor = DEFAULT_MONEYNESS,
  limit = DEFAULT_LIMIT,
  forceRefresh = false,
  maxAgeMs = DEFAULT_CACHE_MS,
} = {}) {
  const { refreshed, logs } = await refreshData({ force: forceRefresh, maxAgeMs });

  const csvText = await readFile(BID_CSV, "utf8");
  const rows = parseCsv(csvText).map(normalizeRow);

  const filtered = rows.filter(
    (r) => r.ticker && r.moneyness != null && r.moneyness > moneynessFloor
  );

  filtered.sort((a, b) => {
    const aScore = a.roi_annualized ?? -Infinity;
    const bScore = b.roi_annualized ?? -Infinity;
    return bScore - aScore;
  });

  const capped = limit > 0 ? filtered.slice(0, limit) : filtered;

  return {
    rows: capped,
    meta: {
      refreshed,
      source: BID_CSV,
      total: filtered.length,
      moneynessFloor,
    },
    logs,
  };
}

export default async function handler(req, res) {
  try {
    const moneynessFloor = Number.isFinite(Number(req.query.moneyness))
      ? Number(req.query.moneyness)
      : DEFAULT_MONEYNESS;
    const limit = Math.min(pickPositive(req.query.limit, DEFAULT_LIMIT), 2000);
    const forceRefresh =
      String(req.query.refresh).toLowerCase() === "true" || req.query.refresh === "1";
    const maxAgeMs = pickPositive(req.query.maxAgeMs, DEFAULT_CACHE_MS);

    const data = await loadYfMetrics({ moneynessFloor, limit, forceRefresh, maxAgeMs });
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({
      error: err?.message || String(err),
      stderr: err?.stderr ? trimLog(err.stderr) : undefined,
    });
  }
}
