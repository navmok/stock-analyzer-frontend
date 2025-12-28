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
    "STATE STREET",
  ];

  const assetKW = [
    "VANGUARD", "BLACKROCK", "FIDELITY", "SCHWAB", "INVESCO",
    "T ROWE", "T. ROWE", "FRANKLIN", "DIMENSIONAL", "PIMCO",
    "CAPITAL GROUP", "WELLINGTON", "AMUNDI", "NORTHERN TRUST",
  ];

  if (bankKW.some((k) => name.includes(k))) return "bank";
  if (assetKW.some((k) => name.includes(k))) return "asset_manager";

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

  const endMonth = q * 3; // Q1->3, Q2->6, Q3->9, Q4->12
  const endDay = endMonth === 3 || endMonth === 12 ? 31 : 30;

  return new Date(Date.UTC(year, endMonth - 1, endDay));
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

    // ✅ Robust percent calcs: use lag() across available quarters, not date subtraction joins
    // Also ✅ De-dupe in case old/duplicate rows exist for same cik+period_end
    const sql = `
      WITH dedup AS (
        SELECT
          cik,
          MAX(manager_name) AS manager_name,
          period_end::date AS period_end,
          MAX(total_value_m)::double precision AS total_value_m,
          MAX(num_holdings)::int AS num_holdings
        FROM manager_quarter
        GROUP BY cik, period_end::date
      ),
      ranked AS (
        SELECT
          d.*,
          LAG(total_value_m, 1)  OVER (PARTITION BY cik ORDER BY period_end) AS prev_qtr,
          LAG(total_value_m, 4)  OVER (PARTITION BY cik ORDER BY period_end) AS prev_yoy,
          LAG(total_value_m, 20) OVER (PARTITION BY cik ORDER BY period_end) AS prev_5y,
          LAG(total_value_m, 40) OVER (PARTITION BY cik ORDER BY period_end) AS prev_10y
        FROM dedup d
      )
      SELECT
        cik,
        manager_name,
        period_end,
        total_value_m,
        num_holdings,
        prev_qtr,
        prev_yoy,
        prev_5y,
        prev_10y
      FROM ranked
      WHERE period_end = $1::date
      ORDER BY total_value_m DESC
      LIMIT 5000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    let out = rows.map((r) => {
      const curr = r.total_value_m !== null ? Number(r.total_value_m) : null;
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
        num_holdings: r.num_holdings != null ? Number(r.num_holdings) : 0,
        qoq_pct: curr != null ? pct(curr, prevQ) : null,
        yoy_pct: curr != null ? pct(curr, prevY) : null,
        pct_5y: curr != null ? pct(curr, prev5) : null,
        pct_10y: curr != null ? pct(curr, prev10) : null,
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
