import pg from "pg";
const { Pool } = pg;

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }, // Neon requires SSL
    });
  }
  return pool;
}

export default async function handler(req, res) {
  try {
    const period = req.query.period || "2025Q3"; // default for testing
    if (!/^\d{4}Q[1-4]$/.test(period)) {
      return res.status(400).json({ error: "period must be like 2025Q3" });
    }

    const year = Number(period.slice(0, 4));
    const q = Number(period.slice(5, 6));
    const qEnd = {
      1: `${year}-03-31`,
      2: `${year}-06-30`,
      3: `${year}-09-30`,
      4: `${year}-12-31`,
    }[q];

    const sql = `
      select cik, manager_name, period_end, total_value_m, num_holdings
      from manager_quarter
      where period_end = $1::date
      order by total_value_m desc
      limit 1000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    res.status(200).json(
      rows.map((r) => ({
        cik: r.cik,
        manager: r.manager_name,
        period_end: r.period_end,
        aum_m: Number(r.total_value_m),
        num_holdings: Number(r.num_holdings),
      }))
    );
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
