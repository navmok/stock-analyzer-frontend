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
    "BANK",
    "BANC",
    "TRUST",
    "CREDIT UNION",
    "JPMORGAN",
    "CHASE",
    "WELLS FARGO",
    "CITIGROUP",
    "MORGAN STANLEY",
    "GOLDMAN SACHS",
    "BANK OF AMERICA",
    "BARCLAYS",
    "UBS",
    "HSBC",
    "STATE STREET",
    "NORTHERN TRUST",
  ];

  const assetKW = [
    "VANGUARD",
    "BLACKROCK",
    "FIDELITY",
    "SCHWAB",
    "INVESCO",
    "T ROWE",
    "T. ROWE",
    "FRANKLIN",
    "DIMENSIONAL",
    "CAPITAL GROUP",
    "WELLINGTON",
    "PIMCO",
    "AMUNDI",
  ];

  if (bankKW.some((k) => name.includes(k))) return "bank";
  if (assetKW.some((k) => name.includes(k))) return "asset_manager";

  // default: hedge_fund unless it’s clearly “other”
  const hfKW = ["CAPITAL", "MANAGEMENT", "PARTNERS", "ADVISOR", "ADVISERS", "FUND", "INVEST"];
  if (hfKW.some((k) => name.includes(k))) return "hedge_fund";

  return "other";
}

function pct(curr, prev) {
  if (prev == null || prev === 0) return null;
  return (curr - prev) / prev;
}

// period like "2025Q3" -> "YYYY-MM-DD" quarter end
function periodToQuarterEnd(periodStr) {
  const m = String(periodStr || "").match(/^(\d{4})Q([1-4])$/i);
  if (!m) return null;

  const year = Number(m[1]);
  const q = Number(m[2]);

  const endMonth = q * 3; // 3,6,9,12
  const endDay = endMonth === 3 || endMonth === 12 ? 31 : 30;

  const mm = String(endMonth).padStart(2, "0");
  const dd = String(endDay).padStart(2, "0");
  return `${year}-${mm}-${dd}`;
}

export default async function handler(req, res) {
  try {
    const period = String(req.query.period || "2025Q3").trim();
    const qEnd = periodToQuarterEnd(period);
    if (!qEnd) return res.status(400).json({ error: "Bad period. Use like 2025Q3." });

    const search = String(req.query.search || "").toLowerCase().trim();

    // multi-select: type=hedge_fund&type=bank...
    const typesParam = req.query.type;
    const types = new Set(
      (Array.isArray(typesParam) ? typesParam : typesParam ? [typesParam] : ["all"]).map((x) =>
        String(x).toLowerCase()
      )
    );

    /**
     * KEY FIXES:
     * 1) Filter to true quarter ends only (03-31, 06-30, 09-30, 12-31)
     *    -> ignores old bad rows generated when period_end was derived from filingDate.
     * 2) total_value_m is actually in $THOUSANDS (13F typical). Convert to $M by /1000.
     * 3) % changes computed using exact prior quarter-end dates (QoQ/YoY/5Y/10Y strict).
     */
    const sql = `
      WITH base AS (
        SELECT
          cik,
          MAX(manager_name) AS manager_name,
          period_end::date AS period_end,
          MAX(total_value_m)::double precision AS total_value_thousands,  -- treat as $ thousands
          MAX(num_holdings)::int AS num_holdings
        FROM manager_quarter
        WHERE
          -- ✅ keep only true quarter-end dates
          (EXTRACT(MONTH FROM period_end::date), EXTRACT(DAY FROM period_end::date)) IN
            ((3,31),(6,30),(9,30),(12,31))
        GROUP BY cik, period_end::date
      ),
      cur AS (
        SELECT *
        FROM base
        WHERE period_end = $1::date
      ),
      prev AS (
        SELECT cik, total_value_thousands AS prev_thousands
        FROM base
        WHERE period_end = ($1::date - INTERVAL '3 months')
      ),
      yoy AS (
        SELECT cik, total_value_thousands AS yoy_thousands
        FROM base
        WHERE period_end = ($1::date - INTERVAL '12 months')
      ),
      y5 AS (
        SELECT cik, total_value_thousands AS y5_thousands
        FROM base
        WHERE period_end = ($1::date - INTERVAL '60 months')
      ),
      y10 AS (
        SELECT cik, total_value_thousands AS y10_thousands
        FROM base
        WHERE period_end = ($1::date - INTERVAL '120 months')
      )
      SELECT
        cur.cik,
        cur.manager_name,
        cur.period_end,
        cur.total_value_thousands,
        cur.num_holdings,
        prev.prev_thousands,
        yoy.yoy_thousands,
        y5.y5_thousands,
        y10.y10_thousands
      FROM cur
      LEFT JOIN prev USING (cik)
      LEFT JOIN yoy  USING (cik)
      LEFT JOIN y5   USING (cik)
      LEFT JOIN y10  USING (cik)
      ORDER BY cur.total_value_thousands DESC
      LIMIT 5000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    let out = rows.map((r) => {
      const currTh = r.total_value_thousands != null ? Number(r.total_value_thousands) : null;
      const prevTh = r.prev_thousands != null ? Number(r.prev_thousands) : null;
      const yoyTh = r.yoy_thousands != null ? Number(r.yoy_thousands) : null;
      const y5Th = r.y5_thousands != null ? Number(r.y5_thousands) : null;
      const y10Th = r.y10_thousands != null ? Number(r.y10_thousands) : null;

      // ✅ Convert $ thousands -> $ millions for display
      const currM = currTh != null ? currTh / 1000 : null;

      const category = classifyManager(r.manager_name);

      return {
        cik: r.cik,
        manager: r.manager_name,
        category,
        period_end: r.period_end,
        aum_m: currM, // ✅ correct scale now
        num_holdings: r.num_holdings != null ? Number(r.num_holdings) : 0,

        // % math can use thousands (scale cancels), but must be aligned quarters
        qoq_pct: currTh != null ? pct(currTh, prevTh) : null,
        yoy_pct: currTh != null ? pct(currTh, yoyTh) : null,
        pct_5y: currTh != null ? pct(currTh, y5Th) : null,
        pct_10y: currTh != null ? pct(currTh, y10Th) : null,
      };
    });

    // search filter
    if (search) {
      out = out.filter(
        (r) =>
          String(r.manager).toLowerCase().includes(search) ||
          String(r.cik).toLowerCase().includes(search)
      );
    }

    // type filter
    if (!types.has("all")) {
      out = out.filter((r) => types.has(r.category));
    }

    return res.status(200).json(out);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
