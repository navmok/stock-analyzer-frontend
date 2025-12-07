// api/options.js
// Yahoo Finance options

export default async function handler(req, res) {
  const { symbol = "GOOGL" } = req.query ?? {};

  try {
    const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}`;

    const r = await fetch(url);
    if (!r.ok) {
      console.error("Yahoo status", r.status);
      // return empty array instead of 500
      return res.status(200).json([]);
    }

    let json;
    try {
      json = await r.json();
    } catch (e) {
      console.error("Yahoo JSON parse error", e);
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
      expiration: o.expiration ? new Date(o.expiration * 1000).toISOString() : null,
      inTheMoney: o.inTheMoney,
    });

    const options = [
      ...calls.map((o) => mapOption(o, "C")),
      ...puts.map((o) => mapOption(o, "P")),
    ];

    res.status(200).json(options);
  } catch (e) {
    console.error("Options handler error", e);
    // fail soft: empty list, not 500
    res.status(200).json([]);
  }
}
