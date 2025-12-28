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

// --- classify managers into: hedge_fund | bank | asset_manager | other
function classifyManager(nameRaw) {
  const name = String(nameRaw || "").toUpperCase();

  const bankKW = [
    "BANK", "BANC", "JPMORGAN", "CHASE", "WELLS FARGO", "CITIGROUP",
    "MORGAN STANLEY", "GOLDMAN SACHS", "BNY", "BANK OF AMERICA",
    "BARCLAYS", "UBS", "HSBC", "CREDIT SUISSE", "ROYAL BANK", "TD ",
    "STATE STREET"
  ];

  const assetKW = [
    "VANGUARD", "BLACKROCK", "FIDELITY", "SCHWAB", "INVESCO",
    "T ROWE", "T. ROWE", "FRANKLIN", "DIMENSIONAL", "PIMCO",
    "CAPITAL GROUP", "WELLINGTON", "AMUNDI", "NORTHERN TRUST"
  ];

  if (bankKW.some((k) => name.includes(k))) return "bank";
  if (assetKW.some((k) => name.includes(k))) return "asset_manager";

  // default most “MANAGEMENT / CAPITAL / PARTNERS / ADVISORS / FUND” to hedge funds
  const hfKW = ["CAPITAL", "MANAGEMENT", "PARTNERS", "ADVISORS", "FUND", "INVEST"];
  if (hfKW.some((k) => name.includes(k))) return "hedge_fund";

  return "other";
}

function pct(curr, prev) {
  if (prev == null || prev === 0) return null;
  return (curr - prev) / prev;
}

function parsePeriod(periodStr) {
  // expects "2025Q3"
  const m = String(periodStr || "").match(/^(\d{4})Q([1-4])$/i);
  if (!m) return null;
  const year = Number(m[1]);
  const q = Number(m[2]);
  const endMonth = q * 3;          // Q1->3, Q2->6, Q3->9, Q4->12
  const endDay = endMonth === 3 || endMonth === 12 ? 31 : 30;
  const qEnd = new Date(Date.UTC(year, endMonth - 1, endDay));
  return qEnd;
}

export default async function handler(req, res) {
  try {
    const period = String(req.query.period || "2025Q3");
    const search = String(req.query.search || "").toLowerCase().trim();

    // multi-select types: type=hedge_fund&type=bank ...
    const typesParam = req.query.type;
    const types = new Set(
      (Array.isArray(typesParam) ? typesParam : typesParam ? [typesParam] : ["all"])
        .map((x) => String(x).toLowerCase())
    );

    const qEndDate = parsePeriod(period);
    if (!qEndDate) {
      return res.status(400).json({ error: `Bad period. Use like 2025Q3.` });
    }
    const qEnd = qEndDate.toISOString().slice(0, 10); // YYYY-MM-DD

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
      limit 5000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    let out = rows.map((r) => {
      const curr = Number(r.total_value_m);
      const prevQ = r.prev_qtr !== null ? Number(r.prev_qtr) : null;
      const prevY = r.prev_yoy !== null ? Number(r.prev_yoy) : null;
      const prev5 = r.prev_5y !== null ? Number(r.prev_5y) : null;
      const prev10 = r.prev_10y !== null ? Number(r.prev_10y) : null;

      const category = classifyManager(r.manager_name);

      return {
        cik: r.cik,
        manager: r.manager_name,
        category,
        period_end: r.period_end,
        aum_m: curr,
        num_holdings: Number(r.num_holdings),
        qoq_pct: pct(curr, prevQ),
        yoy_pct: pct(curr, prevY),
        pct_5y: pct(curr, prev5),
        pct_10y: pct(curr, prev10),
      };
    });

    // search filter
    if (search) {
      out = out.filter((r) =>
        String(r.manager).toLowerCase().includes(search) ||
        String(r.cik).toLowerCase().includes(search)
      );
    }

    // type filter (multi-select)
    if (!types.has("all")) {
      out = out.filter((r) => types.has(r.category));
    }

    res.status(200).json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
}
