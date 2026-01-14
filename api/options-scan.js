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
        ticker,
        ROUND(roi_annualized::numeric, 2) AS roi_annualized,
        trade_dt,
        ROUND(spot::numeric, 2)           AS spot,
        exp,
        dte,
        ROUND(strike::numeric, 2)         AS strike,
        ROUND(premium::numeric, 2)        AS premium,
        ROUND(iv::numeric, 2)             AS iv,
        ROUND(delta::numeric, 2)          AS delta,
        ROUND(pop::numeric, 2)            AS pop,
        ROUND(moneyness::numeric, 2)      AS moneyness,
        ROUND(roi::numeric, 2)            AS roi
      FROM public.sell_put_candidates_agg
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
