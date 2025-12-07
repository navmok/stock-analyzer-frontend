// pages/api/options.js (Next.js API route)

function buildMockOptions(symbol) {
  const baseStrike = 300;
  const options = [];

  for (let i = -5; i <= 5; i++) {
    const strike = baseStrike + i * 5;
    const absI = Math.abs(i);

    options.push({
      contractSymbol: symbol + "C" + strike,
      type: "C",
      strike: strike,
      lastPrice: 10 - absI,
      bid: 9 - absI,
      ask: 11 - absI,
      volume: 1000 - absI * 50,
      openInterest: 5000 - absI * 200,
      expiration: "2026-01-16T00:00:00Z",
      inTheMoney: i < 0,
    });

    options.push({
      contractSymbol: symbol + "P" + strike,
      type: "P",
      strike: strike,
      lastPrice: 10 - absI,
      bid: 9 - absI,
      ask: 11 - absI,
      volume: 900 - absI * 50,
      openInterest: 4500 - absI * 200,
      expiration: "2026-01-16T00:00:00Z",
      inTheMoney: i > 0,
    });
  }

  return options;
}

export default async function handler(req, res) {
  const symbol = req.query?.symbol || "GOOGL";
  const useMock = req.query?.mock === "1"; // optional: ?mock=1 to force mock

  // If you want to force mock explicitly
  if (useMock) {
    const mockOptions = buildMockOptions(symbol);
    return res.status(200).json(mockOptions);
  }

  try {
    const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}`;

    // Add a User-Agent just to be safe; some hosts are picky
    const yfRes = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
      },
    });

    if (!yfRes.ok) {
      throw new Error(`Yahoo Finance HTTP ${yfRes.status}`);
    }

    const json = await yfRes.json();
    const chain = json?.optionChain?.result?.[0];
    const opt = chain?.options?.[0];

    if (!opt) {
      // No options data – return empty array (or mock if you prefer)
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
  } catch (err) {
    console.error("Error fetching Yahoo Finance options:", err);

    // Fallback: return mock options so UI still works
    const fallback = buildMockOptions(symbol);
    return res.status(200).json(fallback);
    // If you’d rather fail hard instead, use:
    // return res.status(500).json({ error: "Failed to fetch options" });
  }
}
