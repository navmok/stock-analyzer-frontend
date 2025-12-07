// api/prices.js

export default function handler(req, res) {
  const { symbol = "GOOGL", days = "1" } = req.query ?? {};

  const now = new Date();
  const data = [
    {
      ts_utc: now.toISOString(),
      symbol,
      open: 100,
      high: 105,
      low: 98,
      close: 102,
      volume: 123456,
    },
  ];

  res.status(200).json(data);
}
