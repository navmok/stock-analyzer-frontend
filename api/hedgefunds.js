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
    "BANK", "BANC", "TRUST", "CREDIT UNION",
    "JPMORGAN", "CHASE", "WELLS FARGO", "CITIGROUP",
    "MORGAN STANLEY", "GOLDMAN SACHS", "BANK OF AMERICA",
    "BARCLAYS", "UBS", "HSBC", "STATE STREET", "NORTHERN TRUST",
  ];

  const assetKW = [
    "VANGUARD", "BLACKROCK", "FIDELITY", "SCHWAB", "INVESCO",
    "T ROWE", "T. ROWE", "FRANKLIN", "DIMENSIONAL", "PIMCO",
    "CAPITAL GROUP", "WELLINGTON", "AMUNDI",
  ];

  if (bankKW.some((k) => name.includes(k))) return "bank";
  if (assetKW.some((k) => name.includes(k))) return "asset_manager";

  const hfKW = ["CAPITAL", "MANAGEMENT", "PARTNERS", "ADVISOR", "ADVISERS", "FUND", "INVEST"];
  if (hfKW.some((k) => name.includes(k))) return "hedge_fund";

  return "other";
}

function pct(curr, prev) {
  if (prev == null || prev === 0) return null;
  return (curr - prev) / prev;
}

function periodToQuarterEnd(periodStr) {
  const m = String(periodStr || "").match(/^(\d{4})Q([1-4])$/i);
  if (!m) return null;

  const year = Number(m[1]);
  const q = Number(m[2]);
  const endMonth = q * 3; // 3,6,9,12
  const endDay = endMonth === 3 || endMonth === 12 ? 31 : 30;

  return `${year}-${String(endMonth).padStart(2, "0")}-${String(endDay).padStart(2, "0")}`;
}

export default async function handler(req, res) {
  try {
    const period = String(req.query.period || "2025Q3").trim();
    const qEnd = periodToQuarterEnd(period);
    if (!qEnd) return res.status(400).json({ error: "Bad period. Use like 2025Q3." });

    const search = String(req.query.search || "").toLowerCase().trim();

    const typesParam = req.query.type;
    const types = new Set(
      (Array.isArray(typesParam) ? typesParam : typesParam ? [typesParam] : ["all"])
        .map((x) => String(x).toLowerCase())
    );

    // ✅ IMPORTANT:
    // - Keep AUM as-is (do NOT divide by 1000 here)
    // - Fix % calcs by:
    //   1) de-duping cik+period_end
    //   2) ONLY using true quarter-end dates (filters out old bad rows)
    //   3) strict prior-quarter matches via interval subtraction
    const sql = `
      WITH base AS (
        SELECT
          cik,
          MAX(manager_name) AS manager_name,
          period_end::date AS period_end,
          MAX(total_value_m)::double precision AS total_value_m,
          MAX(num_holdings)::int AS num_holdings
        FROM manager_quarter
        WHERE
          (EXTRACT(MONTH FROM period_end::date), EXTRACT(DAY FROM period_end::date)) IN
            ((3,31),(6,30),(9,30),(12,31))
        GROUP BY cik, period_end::date
      ),
      cur AS (
        SELECT * FROM base WHERE period_end = $1::date
      ),
      prev AS (
        SELECT cik, total_value_m AS prev_qtr
        FROM base
        WHERE period_end = ($1::date - INTERVAL '3 months')::date
      ),
      yoy AS (
        SELECT cik, total_value_m AS prev_yoy
        FROM base
        WHERE period_end = ($1::date - INTERVAL '12 months')::date
      ),
      y5 AS (
        SELECT cik, total_value_m AS prev_5y
        FROM base
        WHERE period_end = ($1::date - INTERVAL '60 months')::date
      ),
      y10 AS (
        SELECT cik, total_value_m AS prev_10y
        FROM base
        WHERE period_end = ($1::date - INTERVAL '120 months')::date
      )
      SELECT
        cur.cik,
        cur.manager_name,
        cur.period_end,
        cur.total_value_m,
        cur.num_holdings,
        prev.prev_qtr,
        yoy.prev_yoy,
        y5.prev_5y,
        y10.prev_10y
      FROM cur
      LEFT JOIN prev USING (cik)
      LEFT JOIN yoy USING (cik)
      LEFT JOIN y5 USING (cik)
      LEFT JOIN y10 USING (cik)
      ORDER BY cur.total_value_m DESC
      LIMIT 5000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    let out = rows.map((r) => {
      const curr = r.total_value_m != null ? Number(r.total_value_m) : null;
      const prevQ = r.prev_qtr != null ? Number(r.prev_qtr) : null;
      const prevY = r.prev_yoy != null ? Number(r.prev_yoy) : null;
      const prev5 = r.prev_5y != null ? Number(r.prev_5y) : null;
      const prev10 = r.prev_10y != null ? Number(r.prev_10y) : null;

      return {
        cik: r.cik,
        manager: r.manager_name,
        category: classifyManager(r.manager_name),
        period_end: r.period_end,
        aum_m: curr, // ✅ unchanged (no scaling)
        num_holdings: r.num_holdings != null ? Number(r.num_holdings) : 0,
        qoq_pct: curr != null ? pct(curr, prevQ) : null,
        yoy_pct: curr != null ? pct(curr, prevY) : null,
        pct_5y: curr != null ? pct(curr, prev5) : null,
        pct_10y: curr != null ? pct(curr, prev10) : null,
      };
    });

    if (search) {
      out = out.filter(
        (r) =>
          String(r.manager).toLowerCase().includes(search) ||
          String(r.cik).toLowerCase().includes(search)
      );
    }

    if (!types.has("all")) {
      out = out.filter((r) => types.has(r.category));
    }

    return res.status(200).json(out);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
