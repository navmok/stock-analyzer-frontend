import { readFile } from "fs/promises";
import path from "path";

// In production we cannot spawn Python; use a pre-generated CSV under /public by default.
const DEFAULT_CSV_NAME = "scrape_bid_polygon_0.15.csv";
const BID_CSV =
  process.env.SCRAPE_BID_OUTPUT ||
  path.join(process.cwd(), "public", DEFAULT_CSV_NAME);
const BID_URL = process.env.SCRAPE_BID_URL || null;

const DEFAULT_MONEYNESS = Number(process.env.MONEYNESS_THRESHOLD || 0.85);
const DEFAULT_LIMIT = Number(process.env.METRICS_LIMIT || 500);

function numberOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(String(v).trim().replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function toPercentUnits(raw) {
  const n = numberOrNull(raw);
  if (n == null) return null;
  return Math.abs(n) <= 1 ? n * 100 : n;
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

  const underlying =
    row.underlying ||
    row.underlying_ticker ||
    row.underlyingTicker ||
    row.ticker_underlying ||
    null;
  const rawTicker = row.ticker?.trim() || null;
  const optionTicker =
    row.option_ticker ||
    row.option_tic ||
    row.contractSymbol ||
    (rawTicker && rawTicker.startsWith("O:") ? rawTicker : null) ||
    null;

  let ticker = underlying?.trim?.() || null;
  if (!ticker && rawTicker) {
    // If the CSV includes both an underlying ticker and an option symbol, prefer the underlying.
    // Otherwise fall back to the raw ticker value.
    ticker = optionTicker && rawTicker === optionTicker ? null : rawTicker;
  }

  const spot =
    numberOrNull(row.spot) ??
    numberOrNull(row.spot_price) ??
    numberOrNull(row.underlying_price) ??
    numberOrNull(row.close);
  const strike = numberOrNull(row.strike) ?? numberOrNull(row.strike_price);
  const moneyness =
    numberOrNull(row.moneyness) ??
    (strike != null && spot != null && spot !== 0
      ? Number(strike) / Number(spot)
      : null);

  return {
    trade_dt: row.trade_dt || row.date || null,
    ticker,
    spot,
    strike,
    moneyness,
    premium: numberOrNull(row.premium) ?? numberOrNull(row.bid_yf),
    iv: toPercentUnits(row.iv ?? row.implied_volatility),
    delta: numberOrNull(row.delta),
    roi: toPercentUnits(row.roi),
    roi_annualized: toPercentUnits(roiAnnRaw),
    option_ticker: optionTicker,
    expiry: row.expiry || row.exp || row.expiration_date || null,
    volume: numberOrNull(row.volume),
    open_interest: numberOrNull(row.open_interest),
  };
}

function pickPositive(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export async function loadYfMetrics({
  moneynessFloor = DEFAULT_MONEYNESS,
  limit = DEFAULT_LIMIT,
  host = null,
} = {}) {
  let csvText = null;
  let sourceUsed = BID_CSV;

  try {
    csvText = await readFile(BID_CSV, "utf8");
  } catch (err) {
    // If the file is missing in the serverless bundle, fall back to an HTTP fetch
    const fallbackUrl =
      BID_URL ||
      (host
        ? `${host.startsWith("localhost") || host.startsWith("127.") ? "http" : "https"}://${host}/${DEFAULT_CSV_NAME}`
        : null);

    if (!fallbackUrl) throw err;

    const resp = await fetch(fallbackUrl);
    if (!resp.ok) {
      const e = new Error(`Failed to fetch CSV from ${fallbackUrl} (${resp.status})`);
      e.cause = err;
      throw e;
    }
    csvText = await resp.text();
    sourceUsed = fallbackUrl;
  }

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
      source: sourceUsed,
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

    const host = req?.headers?.host || null;
    const data = await loadYfMetrics({ moneynessFloor, limit, host });
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({
      error: err?.message || String(err),
    });
  }
}
