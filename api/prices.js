// api/prices.js
// Mock stock data for testing

module.exports = async function handler(req, res) {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const { symbol = "GOOGL", days = "1" } = req.query || {};

  // Generate mock data based on days requested
  const daysNum = parseInt(days, 10) || 1;
  const data = [];

  // Starting price
  let basePrice = 150;
  
  // Determine interval based on days
  let intervalMinutes;
  let pointsPerDay;
  
  if (daysNum <= 5) {
    // 5-minute intervals for recent data
    intervalMinutes = 5;
    pointsPerDay = (6.5 * 60) / 5; // 78 points per trading day
  } else if (daysNum <= 60) {
    // Hourly intervals
    intervalMinutes = 60;
    pointsPerDay = 6.5; // 6.5 points per trading day
  } else {
    // Daily intervals
    intervalMinutes = 60 * 24;
    pointsPerDay = 1;
  }

  const totalPoints = Math.floor(daysNum * pointsPerDay);
  const now = new Date();

  // Generate data points going backwards in time
  for (let i = totalPoints - 1; i >= 0; i--) {
    const timestamp = new Date(now.getTime() - (i * intervalMinutes * 60 * 1000));
    
    // Skip weekends
    const dayOfWeek = timestamp.getDay();
    if (dayOfWeek === 0 || dayOfWeek === 6) {
      continue;
    }

    // Generate realistic price movement
    const volatility = basePrice * 0.02; // 2% volatility
    const change = (Math.random() - 0.5) * volatility;
    
    const open = basePrice;
    const close = basePrice + change;
    const high = Math.max(open, close) + Math.random() * volatility * 0.5;
    const low = Math.min(open, close) - Math.random() * volatility * 0.5;
    const volume = Math.floor(1000000 + Math.random() * 5000000);

    data.push({
      ts_utc: timestamp.toISOString(),
      symbol: symbol,
      open: parseFloat(open.toFixed(2)),
      high: parseFloat(high.toFixed(2)),
      low: parseFloat(low.toFixed(2)),
      close: parseFloat(close.toFixed(2)),
      volume: volume,
    });

    // Update base price for next candle
    basePrice = close;
  }

  return res.status(200).json(data);
};