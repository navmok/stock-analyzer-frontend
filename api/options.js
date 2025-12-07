// api/options.js  (Vercel Serverless Function)

export default async function handler(req, res) {
  const symbol = req.query.symbol || "AAPL";

  const url = `https://query2.finance.yahoo.com/v7/finance/options/${symbol}`;

  try {
    const yahooRes = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
      },
    });

    if (!yahooRes.ok) {
      return res.status(yahooRes.status).json({
        error: `Yahoo rejected the request: HTTP ${yahooRes.status}`,
      });
    }

    const json = await yahooRes.json();

    const chain = json?.optionChain?.result?.[0];
    const opt = chain?.options?.[0];

    if (!opt) {
      return res.status(200).json([]);
    }

    const calls = opt.calls || [];
    const puts = opt.puts || [];

    const mapOption = (o, type) => ({
      contractSymbol: o.contractSymbol,
      type,
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

    return res.status(200).json([
      ...calls.map((o) => mapOption(o, "C")),
      ...puts.map((o) => mapOption(o, "P")),
    ]);
  } catch (error) {
    console.error("Error fetching Yahoo options:", error);
    return res.status(500).json({ error: error.message });
  }
}
