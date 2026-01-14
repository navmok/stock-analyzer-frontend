// pages/api/options-scan-live.js
import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("neon.tech") ? { rejectUnauthorized: false } : undefined,
});

function nextFridayISO() {
  // returns YYYY-MM-DD for the upcoming Friday (in local time)
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  const day = d.getDay(); // Sun=0 ... Fri=5
  const add = (5 - day + 7) % 7 || 7; // if today is Fri, take next Fri (weekly scan)
  d.setDate(d.getDate() + add);
  return d.toISOString().slice(0, 10);
}

function daysBetween(yyyyMmDdA, yyyyMmDdB) {
  const a = new Date(yyyyMmDdA + "T00:00:00Z");
  const b = new Date(yyyyMmDdB + "T00:00:00Z");
  return Math.round((b - a) / (1000 * 60 * 60 * 24));
}

async function fetchUnderlyingSpot(symbol, apiKey) {
  const url =
    `https://api.massive.com/v2/aggs/ticker/${encodeURIComponent(symbol)}/prev` +
    `?adjusted=true&apiKey=${encodeURIComponent(apiKey)}`;

  const r = await fetch(url);
  if (!r.ok) return null;

  const j = await r.json();
  const agg = Array.isArray(j?.results) ? j.results[0] : null;

  // prefer close, then vwap, then open/high/low
  return (
    num(agg?.c) ??
    num(agg?.vw) ??
    num(agg?.o) ??
    num(agg?.h) ??
    num(agg?.l) ??
    null
  );
}

function num(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

export default async function handler(req, res) {
  try {
    const limit = Math.min(Number(req.query.limit || 100), 200);

    // 1) top tickers from existing table in Neon
    const { rows: tickers } = await pool.query(
      `SELECT DISTINCT ticker AS symbol
      FROM public.sell_put_candidates_agg
      ORDER BY ticker
      LIMIT $1`,
      [limit]
    );

    const apiKey = process.env.POLYGON_API_KEY || process.env.MASSIVE_API_KEY;
    if (!apiKey) return res.status(500).json({ error: "Missing POLYGON_API_KEY / MASSIVE_API_KEY" });

    const exp = nextFridayISO();
    const trade_dt = new Date().toISOString().slice(0, 10);
    const dte = Math.max(daysBetween(trade_dt, exp), 0);

    // 2) fetch snapshots in parallel (but not too aggressive)
    const BATCH = 5;
    const out = [];
    const spotCache = new Map();

    for (let i = 0; i < tickers.length; i += BATCH) {
      const batch = tickers.slice(i, i + BATCH);

      const results = await Promise.allSettled(
        batch.map(async ({ symbol }) => {
          const url =
            `https://api.massive.com/v3/snapshot/options/${encodeURIComponent(symbol)}` +
            `?apiKey=${encodeURIComponent(apiKey)}` +
            `&contract_type=put` +
            `&expiration_date=${encodeURIComponent(exp)}` +
            `&limit=250`;
          const r = await fetch(url);
          if (!r.ok) throw new Error(`HTTP ${r.status} for ${symbol}`);
          const j = await r.json();

          // Massive returns { results: [ ...contracts... ] }
          const arr = Array.isArray(j.results) ? j.results : [];
          if (!arr.length) return;

          // Underlying spot (stock price) from Polygon/ Massive (cache per symbol)
          let spot = spotCache.get(symbol);
          if (spot == null) {
            spot = await fetchUnderlyingSpot(symbol, apiKey);
            if (spot != null) spotCache.set(symbol, spot);
          }
          if (spot == null) return;

          const minStrike = spot * 0.95; // within ~5% below spot
          const maxStrike = spot;        // OTM puts only

          for (const it of arr) {
            const details = it?.details || {};
            if (details.contract_type !== "put") continue;
            if (details.expiration_date !== exp) continue;

            const strike = num(details.strike_price);
            if (strike == null) continue;
            if (strike < minStrike || strike > maxStrike) continue; // OTM only, within 5%

            const delta = num(it?.greeks?.delta);
            const iv = num(it?.implied_volatility); // decimal (e.g., 0.32)
            const bid = num(it?.last_quote?.bid);
            const ask = num(it?.last_quote?.ask);

            let premium = null;
            if (bid != null && ask != null) premium = (bid + ask) / 2;
            else if (bid != null) premium = bid;
            else if (ask != null) premium = ask;
            else premium = num(it?.day?.close); // fallback to option close

            if (premium == null || premium <= 0) continue;

            const win_rate = delta == null ? null : (1 - Math.abs(delta)) * 100;
            const iv_pct = iv == null ? null : iv * 100;

            const roi = (premium / strike) * 100;
            const roi_annualized = dte > 0 ? (premium / strike) * (365 / dte) * 100 : null;
            const moneyness = strike ? spot / strike : null; // >1 means OTM buffer for puts

            out.push({
              ticker: symbol,
              trade_dt,
              spot,
              exp,
              dte,
              strike,
              premium,
              iv: iv_pct,
              delta,
              pop: win_rate,
              moneyness,
              roi,
              roi_annualized,
            });
          }
        })
      );

      // ignore per-ticker failures
      for (const r of results) {
        if (r.status === "rejected") {
          // optional: console.error(r.reason);
        }
      }
    }

    // 3) return top 100 opportunities by annualized ROI
    out.sort((a, b) => (b.roi_annualized ?? -1) - (a.roi_annualized ?? -1));

    res.status(200).json(out.slice(0, 100));
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
}
