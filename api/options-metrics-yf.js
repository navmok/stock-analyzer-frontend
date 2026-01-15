import { readFile } from "fs/promises";
import path from "path";

// In production we cannot spawn Python; use a pre-generated CSV under /public by default.
const BID_CSV =
  process.env.SCRAPE_BID_OUTPUT ||
  path.join(process.cwd(), "public", "scrape_bid_yf.csv");

const DEFAULT_MONEYNESS = Number(process.env.MONEYNESS_THRESHOLD || 0.85);
const DEFAULT_LIMIT = Number(process.env.METRICS_LIMIT || 500);

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

function pickPositive(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export async function loadYfMetrics({
  moneynessFloor = DEFAULT_MONEYNESS,
  limit = DEFAULT_LIMIT,
} = {}) {
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
      refreshed: false,
      source: BID_CSV,
      total: filtered.length,
      moneynessFloor,
    },
    logs: [],
  };
}

export default async function handler(req, res) {
  try {
    const moneynessFloor = Number.isFinite(Number(req.query.moneyness))
      ? Number(req.query.moneyness)
      : DEFAULT_MONEYNESS;
    const limit = Math.min(pickPositive(req.query.limit, DEFAULT_LIMIT), 2000);

    const data = await loadYfMetrics({ moneynessFloor, limit });
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({
      error: err?.message || String(err),
    });
  }
}
