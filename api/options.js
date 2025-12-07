export default async function handler(req, res) {
  const symbol = req.query?.symbol || "GOOGL";
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
      inTheMoney: i < 0
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
      inTheMoney: i > 0
    });
  }
  
  res.status(200).json(options);
}