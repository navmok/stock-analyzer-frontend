// api/prices.js
// Stock OHLCV data from Yahoo Finance chart API

export default async function handler(req, res) {
  const { symbol = "GOOGL", days = "1" } = req.query ?? {};

  const d = Number(days) || 1;

  // Simple mapping from "days" -> Yahoo range + interval
  let range = `${d}d`;
  let interval = "1d";

  if (d <= 5) {
    range = `${d}d`;
    interval = "5m";      // intraday-ish
  } else if (d <= 60) {
    range = `${d}d`;
    interval = "1d";
  } else if (d <= 365) {
    range = "1y";
    interval = "1d";
  } else {
    range = "2y";
    interval = "1d";
  }

  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(
      symbol
    )}?range=${range}&interval=${interval}`;

    const r = await fetch(url);
    if (!r.ok) {
      console.error("Yahoo status", r.status);
      return res.status(200).json([]); // fail soft
    }

    let json;
    try {
      json = await r.json();
    } catch (e) {
      console.error("Yahoo JSON error", e);
      return res.status(200).json([]);
    }

    const chart = json?.chart;
    const result = chart?.result?.[0];
    if (!result) {
      return res.status(200).json([]);
    }

    const timestamps = result.timestamp || [];
    const quote = result.indicators?.quote?.[0] || {};

    const opens = quote.open || [];
    const highs = quote.high || [];
    const lows = quote.low || [];
    const closes = quote.close || [];
    const volumes = quote.volume || [];

    const out = [];
    for (let i = 0; i < timestamps.length; i++) {
      const ts = timestamps[i];
      const o = opens[i];
      const h = highs[i];
      const l = lows[i];
      const c = closes[i];
      const v = volumes[i];

      // Skip incomplete rows
      if (
        ts == null ||
        o == null ||
        h == null ||
        l == null ||
        c == null ||
        v == null
      ) {
        continue;
      }

      out.push({
        ts_utc: new Date(ts * 1000).toISOString(),
        symbol,
        open: o,
        high: h,
        low: l,
        close: c,
        volume: v,
      });
    }

    return res.status(200).json(out);
  } catch (e) {
    console.error("prices handler error", e);
    return res.status(200).json([]); // never 500
  }
}
