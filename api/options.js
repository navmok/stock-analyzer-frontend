// api/options.js
// Yahoo Finance options via query2 endpoint

export default async function handler(req, res) {
  const { symbol = "GOOGL" } = req.query || {};

  try {
    const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}`;
    const r = await fetch(url);
    if (!r.ok) {
      throw new Error(`Yahoo HTTP ${r.status}`);
    }

    const json = await r.json();
    const chain = json?.optionChain?.result?.[0];
    if (!chain || !chain.options?.[0]) {
      return res.status(200).json([]);
    }

    const opt = chain.options[0]; // nearest expiry
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
      expiration: new Date(o.expiration * 1000).toISOString(),
      inTheMoney: o.inTheMoney,
    });

    const options = [
      ...calls.map((o) => mapOption(o, "C")),
      ...puts.map((o) => mapOption(o, "P")),
    ];

    res.status(200).json(options);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Failed to load options" });
  }
}
