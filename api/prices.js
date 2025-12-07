// api/prices.js
module.exports = async function handler(req, res) {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    const { symbol = "GOOGL", days = "1" } = req.query || {};

    // Generate mock data
    const daysNum = parseInt(days, 10) || 1;
    const data = [];
    let basePrice = 150;
    
    // Determine interval
    let intervalMinutes;
    let pointsPerDay;
    
    if (daysNum <= 5) {
      intervalMinutes = 5;
      pointsPerDay = 78; // 6.5 hours * 60 / 5
    } else if (daysNum <= 60) {
      intervalMinutes = 60;
      pointsPerDay = 6.5;
    } else {
      intervalMinutes = 1440; // daily
      pointsPerDay = 1;
    }

    const totalPoints = Math.floor(daysNum * pointsPerDay);
    const now = new Date();

    // Generate data points
    for (let i = totalPoints - 1; i >= 0; i--) {
      const timestamp = new Date(now.getTime() - (i * intervalMinutes * 60 * 1000));
      
      // Skip weekends
      const dayOfWeek = timestamp.getDay();
      if (dayOfWeek === 0 || dayOfWeek === 6) {
        continue;
      }

      const volatility = basePrice * 0.02;
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

      basePrice = close;
    }

    return res.status(200).json(data);

  } catch (error) {
    console.error('API Error:', error);
    return res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
};