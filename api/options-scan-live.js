// pages/api/options-scan-live.js
import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("neon.tech") ? { rejectUnauthorized: false } : undefined,
});

// Yahoo crumb/cookie session cache
let yahooSession = null; // { cookie: string, crumb: string, ts: number }
async function ensureYahooSession() {
  const FRESH_MS = 10 * 60 * 1000; // refresh every 10 minutes
  if (yahooSession && Date.now() - yahooSession.ts < FRESH_MS) return yahooSession;

  // Step 1: get cookie from fc.yahoo.com
  const bootstrap = await fetch("https://fc.yahoo.com", { redirect: "follow" });
  const setCookie =
    typeof bootstrap.headers.getSetCookie === "function"
      ? bootstrap.headers.getSetCookie()
      : null;
  const cookie = Array.isArray(setCookie)
    ? setCookie.map((c) => c.split(";")[0]).join("; ")
    : null;
  if (!cookie) return null;

  // Step 2: get crumb using that cookie
  const crumbResp = await fetch("https://query1.finance.yahoo.com/v1/test/getcrumb", {
    headers: { Cookie: cookie, "User-Agent": "Mozilla/5.0" },
  });
  if (!crumbResp.ok) return null;
  const crumb = (await crumbResp.text())?.trim();
  if (!crumb) return null;

  yahooSession = { cookie, crumb, ts: Date.now() };
  return yahooSession;
}

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

  const session = await ensureYahooSession();
  if (!session) {
    yahooChainCache.set(cacheKey, null);
    return null;
  }

  const [y, m, d] = expIso.split("-").map(Number);
  const dt = Date.UTC(y, m - 1, d) / 1000; // seconds
  const baseUrl = `https://query1.finance.yahoo.com/v7/finance/options/${encodeURIComponent(symbol)}?date=${dt}&crumb=${encodeURIComponent(session.crumb)}`;

  // try once; on 401 refresh session and retry
  async function loadOptions() {
    const r = await fetch(baseUrl, {
      headers: { Cookie: session.cookie, "User-Agent": "Mozilla/5.0" },
    });
    if (r.status === 401) {
      yahooSession = null;
      const fresh = await ensureYahooSession();
      if (!fresh) return null;
      return fetch(
        `https://query1.finance.yahoo.com/v7/finance/options/${encodeURIComponent(symbol)}?date=${dt}&crumb=${encodeURIComponent(fresh.crumb)}`,
        { headers: { Cookie: fresh.cookie, "User-Agent": "Mozilla/5.0" } }
      );
    }
    return r;
  }

  const resp = await loadOptions();
  if (!resp || !resp.ok) {
    yahooChainCache.set(cacheKey, null);
    return null;
  }

  const j = await resp.json();
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

function normCdf(x) {
  // Abramowitz and Stegun approximation
  const k = 1 / (1 + 0.2316419 * Math.abs(x));
  const a1 = 0.319381530;
  const a2 = -0.356563782;
  const a3 = 1.781477937;
  const a4 = -1.821255978;
  const a5 = 1.330274429;
  const poly = ((((a5 * k + a4) * k + a3) * k + a2) * k + a1) * k;
  const approx = 1 - (1 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * x * x) * poly;
  return x < 0 ? 1 - approx : approx;
}

function bsPutDelta(spot, strike, iv, dte) {
  const S = num(spot);
  const K = num(strike);
  if (!(S > 0) || !(K > 0)) return null;
  const sigmaRaw = num(iv);
  if (!(sigmaRaw > 0)) return null;
  const sigma = sigmaRaw > 3 ? sigmaRaw / 100 : sigmaRaw; // normalize if percent-like
  const T = Math.max(num(dte) ?? 0, 0) / 365 || 1 / 365;
  const d1 = (Math.log(S / K) + 0.5 * sigma * sigma * T) / (sigma * Math.sqrt(T));
  return normCdf(d1) - 1; // put delta
}

export default async function handler(req, res) {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 1000);
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
        ORDER BY RANDOM()
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

            const iv = num(it?.impliedVolatility); // decimal (e.g., 0.32)
            let delta = num(it?.delta);
            if (delta == null) {
              delta = bsPutDelta(spot, strike, iv, dte);
            }
            const bid = num(it?.bid);
            const ask = num(it?.ask);

            let premium = null;
            if (bid != null && ask != null) premium = (bid + ask) / 2;
            else if (bid != null) premium = bid;
            else if (ask != null) premium = ask;
            else premium = num(it?.lastPrice); // fallback to last trade

            // Require a positive bid to consider the trade; otherwise skip (avoid inflated ROI on zero-bid)
            if (bid == null || bid <= 0) continue;
            if (premium == null || premium <= 0) continue;

            const roiBase = bid;

            const win_rate = delta == null ? null : (1 - Math.abs(delta)) * 100;
            const iv_pct = iv == null ? null : iv * 100;

            const roi = (roiBase / strike) * 100;
            const roi_annualized = dte > 0 ? (roiBase / strike) * (365 / dte) * 100 : null;
            const moneyness = spot ? strike / spot : null; // <1 means OTM for puts

            // require high probability of profit
            if (win_rate != null && win_rate < 90) continue;
            // require sufficient moneyness buffer
            if (moneyness == null || moneyness <= 0.85) continue;

            out.push({
              ticker: symbol,
              trade_dt,
              spot,
              exp,
              dte,
              strike,
              bid,
              ask,
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

    // 3) keep only best ROI entry per ticker
    const bestByTicker = new Map();
    for (const row of out) {
      const key = row.ticker;
      const prev = bestByTicker.get(key);
      const score = row.roi_annualized ?? -Infinity;
      const prevScore = prev?.roi_annualized ?? -Infinity;
      if (!prev || score > prevScore) bestByTicker.set(key, row);
    }

    const payloadArr = Array.from(bestByTicker.values());
    payloadArr.sort((a, b) => (b.roi_annualized ?? -1) - (a.roi_annualized ?? -1));

    res.status(200).json(payloadArr);
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
}
