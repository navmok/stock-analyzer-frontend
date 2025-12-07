// api/options.js
// Mock options data so the UI always works

export default async function handler(req, res) {
  const { symbol = "GOOGL" } = req.query ?? {};

  // Simple fake dataset
  const baseStrike = 300;
  const options = [];

  for (let i = -5; i <= 5; i++) {
    const strike = baseStrike + i * 5;
    options.push(
      {
        contractSymbol: `${symbol}C${strike}`,
        type: "C",
        strike,
        lastPrice: 10 - Math.abs(i),
        bid: 9 - Math.abs(i),
        ask: 11 - Math.abs(i),
        volume: 1000 - Math.abs(i) * 50,
        openInterest: 5000 - Math.abs(i) * 200,
        expiration: "2026-01-16T00:00:00Z",
        inTheMoney: i < 0,
      },
      {
        contractSymbol: `${symbol}P${strike}`,
        type: "P",
        strike,
        lastPrice: 10 - Math.abs(i),
        bid: 9 - Math.abs(i),
        ask: 11 - Math.abs(i),
        volume: 900 - Math.abs(i) * 50,
        openInterest: 4500 - Math.abs(i) * 200,
        expiration: "2026-01-16T00:00:00Z",
        inTheMoney: i > 0,
      }
    );
  }

  res.status(200).json(options);
}
