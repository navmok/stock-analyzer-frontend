import pg from "pg";
const { Pool } = pg;

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });
  }
  return pool;
}

function pct(curr, prev) {
  if (prev === null || prev === 0 || prev === undefined) return null;
  return (curr - prev) / prev;
}

export default async function handler(req, res) {
  try {
    const period = req.query.period || "2025Q3";
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
      with base as (
        select cik, manager_name, period_end, total_value_m, num_holdings
        from manager_quarter
        where period_end = $1::date
      ),
      prev as (
        select cik, total_value_m as prev_qtr
        from manager_quarter
        where period_end = ($1::date - interval '3 months')::date
      ),
      yoy as (
        select cik, total_value_m as prev_yoy
        from manager_quarter
        where period_end = ($1::date - interval '12 months')::date
      ),
      y5 as (
        select cik, total_value_m as prev_5y
        from manager_quarter
        where period_end = ($1::date - interval '60 months')::date
      ),
      y10 as (
        select cik, total_value_m as prev_10y
        from manager_quarter
        where period_end = ($1::date - interval '120 months')::date
      )
      select
        b.cik,
        b.manager_name,
        b.period_end,
        b.total_value_m,
        b.num_holdings,
        p.prev_qtr,
        y.prev_yoy,
        f.prev_5y,
        t.prev_10y
      from base b
      left join prev p using (cik)
      left join yoy y using (cik)
      left join y5 f using (cik)
      left join y10 t using (cik)
      order by b.total_value_m desc
      limit 1000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    const out = rows.map((r) => {
      const curr = Number(r.total_value_m);
      const prevQ = r.prev_qtr !== null ? Number(r.prev_qtr) : null;
      const prevY = r.prev_yoy !== null ? Number(r.prev_yoy) : null;
      const prev5 = r.prev_5y !== null ? Number(r.prev_5y) : null;
      const prev10 = r.prev_10y !== null ? Number(r.prev_10y) : null;

      return {
        cik: r.cik,
        manager: r.manager_name,
        period_end: r.period_end,
        aum_m: curr,
        num_holdings: Number(r.num_holdings),
        qoq_pct: pct(curr, prevQ),
        yoy_pct: pct(curr, prevY),
        pct_5y: pct(curr, prev5),
        pct_10y: pct(curr, prev10),
      };
    });

    res.status(200).json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
