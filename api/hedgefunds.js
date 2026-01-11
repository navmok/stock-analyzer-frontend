import pg from "pg";
const { Pool } = pg;

// 🔥 ADD THIS (CACHE)
let CACHE = {};
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

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

function cagr(curr, prev, years) {
  if (prev == null || prev === 0 || years == null || years <= 0) return null;
  return Math.pow(curr / prev, 1 / years) - 1;
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
  console.log("API: /api/hedgefunds called with query:", req.query);
  try {
    const period = String(req.query.period || "2025Q3").trim();
    const qEnd = periodToQuarterEnd(period);

    const search = String(req.query.search || "").toLowerCase().trim();

    const typesParam = req.query.type;
    const types = new Set(
      (Array.isArray(typesParam) ? typesParam : typesParam ? [typesParam] : ["all"])
        .map((x) => String(x).toLowerCase())
    );

    // NOW search exists ✅
    const cacheKey = `${period}|${search}|${[...types].join(",")}`;
    const cached = CACHE[cacheKey];
    if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
      return res.status(200).json(cached.data);
    }
    if (!qEnd) return res.status(400).json({ error: "Bad period. Use like 2025Q3." });

    // ✅ IMPORTANT:
    // - Keep AUM as-is (do NOT divide by 1000 here)
    // - Fix % calcs by:
    //   1) de-duping cik+period_end
    //   2) ONLY using true quarter-end dates (filters out old bad rows)
    //   3) strict prior-quarter matches via interval subtraction
    const sql = `
      WITH holdings AS (
        SELECT
          cik,
          period_end::date AS period_end,
          SUM(value_usd) AS total_value_usd,
          COUNT(*) AS num_holdings
        FROM manager_quarter_holding
        GROUP BY 1, 2
      ),
      cur AS (
        -- dY"? only rows for THIS quarter
        SELECT
          h.cik,
          mq.manager_name,
          h.period_end,
          h.total_value_usd,
          h.num_holdings
        FROM holdings h
        LEFT JOIN manager_quarter mq
          ON mq.cik = h.cik
          AND mq.period_end::date = h.period_end
        WHERE h.period_end = $1::date
      ),
      base AS (
        -- dY"? only historical rows for managers in THIS quarter
        SELECT
          cik,
          period_end,
          total_value_usd
        FROM holdings
        WHERE cik IN (SELECT DISTINCT cik FROM cur)
      ),
      prev AS (
        SELECT cik, total_value_usd AS prev_qtr
        FROM base
        WHERE period_end = ($1::date - INTERVAL '3 months')
      ),
      yoy AS (
        SELECT cik, total_value_usd AS prev_yoy
        FROM base
        WHERE period_end = ($1::date - INTERVAL '12 months')
      ),
      y5 AS (
        SELECT cik, total_value_usd AS prev_5y
        FROM base
        WHERE period_end = ($1::date - INTERVAL '5 years')
      ),
      y10 AS (
        SELECT cik, total_value_usd AS prev_10y
        FROM base
        WHERE period_end = ($1::date - INTERVAL '10 years')
      )
      SELECT
        cur.cik,
        cur.manager_name,
        cur.period_end,
        cur.total_value_usd,
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
      ORDER BY cur.total_value_usd DESC
      LIMIT 2000;
    `;

    const { rows } = await getPool().query(sql, [qEnd]);

    let out = rows.map((r) => {
      const curr_usd = r.total_value_usd != null ? Number(r.total_value_usd) : null;
      const prevQ = r.prev_qtr != null ? Number(r.prev_qtr) : null;
      const prevY = r.prev_yoy != null ? Number(r.prev_yoy) : null;
      const prev5 = r.prev_5y != null ? Number(r.prev_5y) : null;
      const prev10 = r.prev_10y != null ? Number(r.prev_10y) : null;

      return {
        cik: r.cik,
        manager: r.manager_name,
        category: classifyManager(r.manager_name),
        period_end: r.period_end,

        // ✅ canonical truth (USD)
        aum_usd: curr_usd,

        // ✅ UI display (millions)
        aum_m: curr_usd != null ? curr_usd / 1_000_000 : null,

        num_holdings: r.num_holdings != null ? Number(r.num_holdings) : 0,

        qoq_pct: curr_usd != null ? pct(curr_usd, prevQ) : null,
        yoy_pct: curr_usd != null ? pct(curr_usd, prevY) : null,
        pct_5y: curr_usd != null ? cagr(curr_usd, prev5, 5) : null,
        pct_10y: curr_usd != null ? cagr(curr_usd, prev10, 10) : null,
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

    // 🔥 ADD THIS (SAVE TO CACHE)
    CACHE[cacheKey] = { ts: Date.now(), data: out };

    return res.status(200).json(out);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
