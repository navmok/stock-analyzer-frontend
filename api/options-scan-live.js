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

// Yahoo Finance option chain (source of spot + bid/ask)
const yahooChainCache = new Map();
async function fetchYahooOptions(symbol, expIso) {
  const cacheKey = `${symbol}-${expIso}`;
  if (yahooChainCache.has(cacheKey)) return yahooChainCache.get(cacheKey);

  const [y, m, d] = expIso.split("-").map(Number);
  const dt = Date.UTC(y, m - 1, d) / 1000; // seconds
  const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(symbol)}?date=${dt}`;

  const r = await fetch(url);
  if (!r.ok) {
    yahooChainCache.set(cacheKey, null);
    return null;
  }
  const j = await r.json();
  const res = j?.optionChain?.result?.[0];
  const opt = res?.options?.[0];
  const puts = Array.isArray(opt?.puts) ? opt.puts : [];
  const quote = res?.quote || {};
  const spot =
    num(quote.regularMarketPrice) ??
    num(quote.bid) ??
    num(quote.ask) ??
    num(quote.previousClose) ??
    null;

  const payload = { puts, spot };
  yahooChainCache.set(cacheKey, payload);
  return payload;
}

function num(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

export default async function handler(req, res) {
  try {
    const limit = Math.min(Number(req.query.limit || 100), 200);
    const symbolParam = (req.query.symbol || "").trim();

    // 1) pick tickers: either explicit ?symbol=GOOGL (comma-separated) or fallback to DB list
    let tickers = [];
    if (symbolParam) {
      tickers = symbolParam
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .map((symbol) => ({ symbol: symbol.toUpperCase() }));
    } else {
      const { rows } = await pool.query(
        `SELECT DISTINCT ticker AS symbol
        FROM public.sell_put_candidates_agg
        ORDER BY ticker
        LIMIT $1`,
        [limit]
      );
      tickers = rows;
    }

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
          // Yahoo Finance option chain for this expiry
          const chain = await fetchYahooOptions(symbol, exp);
          if (!chain) return;

          const puts = Array.isArray(chain.puts) ? chain.puts : [];
          const spot = chain.spot;
          if (!puts.length || spot == null) return;

          const minStrike = 0;           // keep all strikes below spot
          const maxStrike = spot;        // OTM puts only (strict)

          for (const it of puts) {
            // Yahoo returns expiration as seconds since epoch
            const expSec = num(it?.expiration);
            const expIso = expSec ? new Date(expSec * 1000).toISOString().slice(0, 10) : null;
            if (expIso !== exp) continue;

            const strike = num(it?.strike);
            if (strike == null) continue;
            if (strike >= maxStrike || strike < minStrike) continue; // only OTM puts

            const delta = num(it?.delta);
            const iv = num(it?.impliedVolatility); // decimal (e.g., 0.32)
            const bid = num(it?.bid);
            const ask = num(it?.ask);

            let premium = null;
            if (bid != null && ask != null) premium = (bid + ask) / 2;
            else if (bid != null) premium = bid;
            else if (ask != null) premium = ask;
            else premium = num(it?.lastPrice); // fallback to last trade

            if (premium == null || premium <= 0) continue;

            const win_rate = delta == null ? null : (1 - Math.abs(delta)) * 100;
            const iv_pct = iv == null ? null : iv * 100;

            const roi = (premium / strike) * 100;
            const roi_annualized = dte > 0 ? (premium / strike) * (365 / dte) * 100 : null;
            const moneyness = spot ? strike / spot : null; // <1 means OTM for puts

            // require high probability of profit
            if (win_rate != null && win_rate < 90) continue;

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

    // 3) return results (sorted for consistency)
    out.sort((a, b) => (b.roi_annualized ?? -1) - (a.roi_annualized ?? -1));

    const payload = symbolParam ? out : out.slice(0, 100);
    res.status(200).json(payload);
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
}
