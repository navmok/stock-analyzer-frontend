// api/options.js
// Yahoo Finance options via query2 API

export default async function handler(req, res) {
  const { symbol = "GOOGL" } = req.query ?? {};

  try {
    const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}`;

    const r = await fetch(url, {
      headers: {
        // Yahoo sometimes wants a UA header
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        Accept: "application/json,text/plain,*/*",
      },
    });

    if (!r.ok) {
      console.error("Yahoo options status", r.status);
      return res.status(200).json([]); // fail soft
    }

    let json;
    try {
      json = await r.json();
    } catch (e) {
      console.error("Yahoo options JSON error", e);
      return res.status(200).json([]);
    }

    const chain = json?.optionChain?.result?.[0];
    const opt = chain?.options?.[0];
    if (!opt) {
      return res.status(200).json([]);
    }

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

    const options = [
      ...calls.map((o) => mapOption(o, "C")),
      ...puts.map((o) => mapOption(o, "P")),
    ];

    return res.status(200).json(options);
  } catch (e) {
    console.error("options handler error", e);
    return res.status(200).json([]); // never 500
  }
}
