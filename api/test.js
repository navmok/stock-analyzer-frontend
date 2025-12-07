// api/test.js
export default function handler(req, res) {
  return res.status(200).json({ 
    message: "Test works!", 
    symbol: req.query.symbol || "none"
  });
}