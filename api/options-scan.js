import pg from "pg";
const { Pool } = pg;

let pool;
function getPool() {
  if (!pool) {
    if (!process.env.DATABASE_URL) {
      throw new Error("Missing DATABASE_URL environment variable");
    }
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });
  }
  return pool;
}

export default async function handler(req, res) {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 500);

    const sql = `
      SELECT
        symbol,
        trade_dt,
        spot,
        exp,
        dte,
        premium,
        iv,
        delta,
        pop,
        moneyness,
        roi,
        roi_annualized
      FROM public.sell_put_candidates_v1
      WHERE roi_annualized = (
        SELECT MAX(roi_annualized)
        FROM public.sell_put_candidates_v1 AS sub
        WHERE sub.symbol = sell_put_candidates_v1.symbol
      )
      ORDER BY roi_annualized DESC
      LIMIT $1;
    `;

    const { rows } = await getPool().query(sql, [limit]);

    return res.status(200).json(rows);
  } catch (e) {
    console.error("options-scan error:", e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
