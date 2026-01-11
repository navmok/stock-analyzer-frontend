import pg from "pg";
import * as cheerio from "cheerio";

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

// --- utils ---
function formatCIK(raw) {
  const s = String(raw || "").replace(/\D/g, "");
  return s.padStart(10, "0");
}

function parseQStr(qStr) {
  const m = qStr.match(/^Q([1-4])\s+(\d{4})$/);
  if (!m) return null;
  const q = Number(m[1]);
  const y = Number(m[2]);
  const quarterMonths = [3, 6, 9, 12];
  const quarterDays = [31, 30, 30, 31];
  const month = String(quarterMonths[q - 1]).padStart(2, "0");
  const day = String(quarterDays[q - 1]).padStart(2, "0");
  return `${y}-${month}-${day}`;
}

// very lightweight classifier used by your UI filters
function classifyManager(name = "") {
  const n = name.toLowerCase();

  const bankWords = ["bank", "bancorp", "trust", "credit union"];
  const assetMgrWords = [
    "asset management",
    "capital management",
    "investment management",
    "advisors",
    "adviser",
    "partners",
    "management llc",
  ];
  const hedgeWords = ["hedge", "master fund", "opportunity", "capital"];

  if (bankWords.some((w) => n.includes(w))) return "bank";
  if (hedgeWords.some((w) => n.includes(w))) return "hedge_fund";
  if (assetMgrWords.some((w) => n.includes(w))) return "asset_manager";
  return "other";
}

// --- SEC fetch helper (SEC requires a User-Agent) ---
async function fetchSecText(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        process.env.SEC_USER_AGENT ||
        "MyApp/1.0 (contact: youremail@example.com)",
      Accept: "application/xml,text/xml,text/plain,*/*",
    },
  });
  if (!r.ok) throw new Error(`SEC fetch failed ${r.status} for ${url}`);
  return r.text();
}

async function fetchSecJson(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        process.env.SEC_USER_AGENT ||
        "MyApp/1.0 (contact: youremail@example.com)",
      Accept: "application/json,*/*",
    },
  });
  if (!r.ok) throw new Error(`SEC fetch failed ${r.status} for ${url}`);
  return r.json();
}

// --- Parse <tableValueTotal> from primary_doc.xml ---
// NOTE: tableValueTotal is almost always in $ THOUSANDS for 13F.
// We convert to dollars (USD) as BIGINT.
function extractAumUsdFromPrimaryDocXml(xmlText) {
  const m =
    xmlText.match(/<tableValueTotal>\s*([\d,]+)\s*<\/tableValueTotal>/i) ||
    xmlText.match(/<totalValue>\s*([\d,]+)\s*<\/totalValue>/i);

  if (!m || !m[1]) return null;

  const thousands = Number(String(m[1]).replace(/,/g, ""));
  if (!Number.isFinite(thousands) || thousands <= 0) return null;

  // thousands -> dollars
  return Math.round(thousands * 1000);
}

// --- Build primary_doc.xml URL robustly ---
// Prefers the common XSL folder path you provided; falls back to index.json directory scan.
async function getPrimaryDocXmlUrl(cikNoLeadingZeros, accPath) {
  const base = `https://www.sec.gov/Archives/edgar/data/${cikNoLeadingZeros}/${accPath}`;

  // 1) Try the common path first (your example)
  const common = `${base}/xslForm13F_X02/primary_doc.xml`;
  try {
    await fetchSecText(common); // if it succeeds, use it
    return common;
  } catch (_) {
    // fall through
  }

  // 2) Try to locate via index.json (more general)
  const idx = await fetchSecJson(`${base}/index.json`);
  const items = idx?.directory?.item || [];

  // Check if primary_doc.xml exists at top level
  const direct = items.find((it) => String(it?.name || "").toLowerCase() === "primary_doc.xml");
  if (direct) return `${base}/primary_doc.xml`;

  // If xslForm13F_X02 folder exists, assume primary_doc.xml inside
  const xslDir = items.find((it) => String(it?.name || "").toLowerCase() === "xslform13f_x02");
  if (xslDir) return `${base}/xslForm13F_X02/primary_doc.xml`;

  // Last resort: try any item named primary_doc.xml if present (rare)
  const any = items.find((it) => String(it?.name || "").toLowerCase().includes("primary_doc"));
  if (any) return `${base}/${any.name}`;

  throw new Error(`Could not locate primary_doc.xml under ${base}`);
}

export default async function handler(req, res) {
  try {
    const pool = getPool();

    const cikInput = String(req.query.cik || "").trim();
    const managerName = String(req.query.manager || "").trim();

    // Allow either cik or manager name (but CIK is preferred)
    let cik = cikInput ? formatCIK(cikInput) : null;

    // if manager name provided without CIK, try to look up from DB
    if (!cik && managerName) {
      const r = await pool.query(
        `SELECT cik FROM managers WHERE LOWER(name)=LOWER($1) LIMIT 1`,
        [managerName]
      );
      if (r.rows?.length) cik = formatCIK(r.rows[0].cik);
    }

    if (!cik) {
      return res.status(400).json({ error: "Missing cik (or manager not found)." });
    }

    // pull canonical name if possible
    let canonicalName = managerName;
    if (!canonicalName) {
      const r = await pool.query(`SELECT name FROM managers WHERE cik=$1 LIMIT 1`, [cik]);
      if (r.rows?.length) canonicalName = r.rows[0].name;
    }
    if (!canonicalName) canonicalName = `CIK ${cik}`;

    // 🔹 classify manager
    const category = classifyManager(canonicalName);

    // Step 2: Get 13F filings from SEC JSON API (use reportDate, not filingDate)
    let filings = [];
    try {
      const submissionsUrl = `https://data.sec.gov/submissions/CIK${cik}.json`;
      const submissionsData = await fetch(submissionsUrl).then((r) => {
        if (!r.ok) throw new Error(`SEC submissions fetch failed: ${r.status}`);
        return r.json();
      });

      const recent = submissionsData.filings?.recent;
      if (recent?.accessionNumber?.length) {
        // SEC returns "recent" as parallel arrays; normalize into objects
        const n = recent.accessionNumber.length;
        const normalized = [];
        for (let i = 0; i < n; i++) {
          normalized.push({
            accessionNumber: recent.accessionNumber[i],
            filingDate: recent.filingDate?.[i] || null,
            reportDate: recent.reportDate?.[i] || null, // ✅ period of report (quarter end)
            form: recent.form?.[i] || null,
            cikNumber: recent.cikNumber?.[i] || submissionsData.cik || null,
          });
        }

        filings = normalized
          .filter(
            (f) =>
              f.form === "13F-HR" ||
              f.form === "13F-HR/A" ||
              f.form === "13F/A"
          )
          .slice(0, 40); // grab more history for YoY/5Y/10Y calcs
      }
    } catch (e) {
      console.warn(`Could not fetch filings from SEC for ${cik}:`, e.message);
      return res
        .status(500)
        .json({ error: `Could not fetch 13F filings: ${e.message}` });
    }

    if (filings.length === 0) {
      return res
        .status(500)
        .json({ error: "No 13F filings found on SEC for this CIK." });
    }

    // Step 3: Process each filing to extract total value (XML ONLY, unit-safe)
    const rows = [];

    for (const filing of filings) {
      try {
        // ✅ Use reportDate (quarter being reported), not filingDate
        const periodEnd = filing.reportDate;
        if (!periodEnd) continue;

        const accessionNum = filing.accessionNumber;
        const accPath = accessionNum.replace(/-/g, "");

        // EDGAR path requires CIK without leading zeros
        const cikNoLeadingZeros = String(Number(cik));

        // ✅ Fetch primary_doc.xml
        const xmlUrl = await getPrimaryDocXmlUrl(cikNoLeadingZeros, accPath);
        const xmlText = await fetchSecText(xmlUrl);

        const aumUsd = extractAumUsdFromPrimaryDocXml(xmlText);
        if (!aumUsd) continue;

        rows.push({
          cik,
          manager_name: canonicalName,
          type: category,
          period_end: periodEnd,
          aum_usd: aumUsd,              // canonical truth
          aum: aumUsd / 1_000_000,      // $M for backward compatibility
          source_url: xmlUrl,           // optional but very useful
        });
      } catch (e) {
        console.warn(
          `Error processing filing ${filing?.accessionNumber}:`,
          e.message
        );
      }
    }

    if (rows.length === 0) {
      return res.status(500).json({
        error:
          "No usable filings were processed (could not extract totals or missing reportDate).",
      });
    }

    // Step 4: Upsert into DB (use (cik, period_end) as the key)
    const upsertSql = `
      INSERT INTO manager_quarter (cik, manager_name, type, period_end, aum, aum_usd)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (cik, period_end)
      DO UPDATE SET
        manager_name = EXCLUDED.manager_name,
        type = EXCLUDED.type,
        aum = EXCLUDED.aum,
        aum_usd = EXCLUDED.aum_usd
    `;

    for (const r of rows) {
      await pool.query(upsertSql, [
        r.cik,
        r.manager_name,
        r.type,
        r.period_end,
        r.aum,
        r.aum_usd,
      ]);
    }

    return res.status(200).json({
      ok: true,
      source: "SEC.gov EDGAR",
      manager: canonicalName,
      upserted_rows: rows.length,
      sample: rows.slice(0, 3),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
