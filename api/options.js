// api/options.js
// Robust Yahoo Finance options loader: tries multiple expiration dates

async function fetchOptionsForDate(symbol, dateTs) {
  const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
    symbol
  )}?date=${dateTs}`;

  const r = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
      Accept: "application/json,text/plain,*/*",
    },
  });

  if (!r.ok) {
    console.error("Yahoo options status", r.status, "for date", dateTs);
    return [];
  }

  let json;
  try {
    json = await r.json();
  } catch (e) {
    console.error("Yahoo options JSON error", e, "for date", dateTs);
    return [];
  }

  const chain = json?.optionChain?.result?.[0];
  const opt = chain?.options?.[0];
  if (!opt) return [];

  const calls = opt.calls || [];
  const puts = opt.puts || [];

  const mapOption = (o, type) => ({
    contractSymbol: o.contractSymbol,
    type, // "C" or "P"
    strike: o.strike,
    lastPrice: o.lastPrice,
    bid: o.bid,
    ask: o.ask,
    volume: o.volume,
    openInterest: o.openInterest,
    expiration: o.expiration
      ? new Date(o.expiration * 1000).toISOString()
      : null,
    inTheMoney: o.inTheMoney,
  });

  return [
    ...calls.map((o) => mapOption(o, "C")),
    ...puts.map((o) => mapOption(o, "P")),
  ];
}

export default async function handler(req, res) {
  const { symbol = "GOOGL" } = req.query ?? {};

  try {
    // First request: just to get expirationDates
    const metaUrl = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}`;

    const metaResp = await fetch(metaUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        Accept: "application/json,text/plain,*/*",
      },
    });

    if (!metaResp.ok) {
      console.error("Yahoo options meta status", metaResp.status);
      return res.status(200).json([]);
    }

    let metaJson;
    try {
      metaJson = await metaResp.json();
    } catch (e) {
      console.error("Yahoo options meta JSON error", e);
      return res.status(200).json([]);
    }

    const chain = metaJson?.optionChain?.result?.[0];
    const dates = chain?.expirationDates || [];

    if (!dates.length) {
      // No expiration dates → nothing we can do
      return res.status(200).json([]);
    }

    // Try first few expiries until we find one with actual contracts
    let allOptions = [];
    const maxTries = Math.min(dates.length, 5);

    for (let i = 0; i < maxTries; i++) {
      const dateTs = dates[i];
      const opts = await fetchOptionsForDate(symbol, dateTs);
      if (opts.length) {
        allOptions = opts;
        break;
      }
    }

    return res.status(200).json(allOptions);
  } catch (e) {
    console.error("options handler error", e);
    return res.status(200).json([]); // never 500
  }
}
