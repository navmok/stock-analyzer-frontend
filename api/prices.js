// api/prices.js or pages/api/prices.js

export default async function handler(req, res) {
  const symbol = req.query?.symbol || "GOOG";
  const daysParam = req.query?.days;
  const d = Math.max(1, parseInt(daysParam, 10) || 1);

  // Decide interval + range based on requested days
  let interval;
  let range;

  if (d <= 7) {
    // 1-minute data, up to 7 days
    interval = "1m";
    range = "7d";
  } else if (d <= 60) {
    // 5-minute data, up to 60 days
    interval = "5m";
    range = "60d";
  } else {
    // Beyond 60 days -> daily candles
    interval = "1d";
    // you can fine-tune this; using max gives long history
    range = "max";
  }

  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(
    symbol
  )}?interval=${interval}&range=${range}`;

  console.log("Fetching Yahoo prices:", { url, symbol, d, interval, range });

  try {
    const yfRes = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
      },
    });

    if (!yfRes.ok) {
      console.error("Yahoo HTTP error:", yfRes.status, yfRes.statusText);
      return res
        .status(500)
        .json({ error: `Yahoo Finance HTTP ${yfRes.status}` });
    }

    const json = await yfRes.json();

    const result = json?.chart?.result?.[0];
    const timestamps = result?.timestamp || [];
    const quote = result?.indicators?.quote?.[0] || {};
    const opens = quote.open || [];
    const highs = quote.high || [];
    const lows = quote.low || [];
    const closes = quote.close || [];
    const volumes = quote.volume || [];

    const rows = timestamps
      .map((t, idx) => {
        const open = opens[idx];
        const high = highs[idx];
        const low = lows[idx];
        const close = closes[idx];
        const volume = volumes[idx];

        // Filter out any null/undefined rows
        if (
          open == null ||
          high == null ||
          low == null ||
          close == null ||
          volume == null
        ) {
          return null;
        }

        return {
          ts_utc: new Date(t * 1000).toISOString(),
          symbol,
          open,
          high,
          low,
          close,
          volume,
        };
      })
      .filter(Boolean);

    // Optionally, if you want to *roughly* cap number of days returned
    // you can slice from the end using d:
    // (Yahoo may give slightly more than you asked for)
    // const msPerDay = 24 * 60 * 60 * 1000;
    // const cutoff = Date.now() - d * msPerDay;
    // const filtered = rows.filter(
    //   (row) => new Date(row.ts_utc).getTime() >= cutoff
    // );

    return res.status(200).json(rows);
  } catch (err) {
    console.error("Error calling Yahoo Finance chart API:", err);
    return res.status(500).json({ error: "Failed to load prices" });
  }
}
