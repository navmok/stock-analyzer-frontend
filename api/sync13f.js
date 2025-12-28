import pg from "pg";
import * as cheerio from "cheerio";

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

// extract total value (in $) from SEC filing page by looking for the summary table value
function extractTotalValueFromHtml(html) {
  const $ = cheerio.load(html);

  // Strategy:
  // 1) Prefer "Information Table" / "Summary" blocks
  // 2) Fall back to scanning for $ value patterns near "TOTAL" or "VALUE"
  const text = $("body").text().replace(/\s+/g, " ").trim();

  // common patterns: "TOTAL ... 1,234,567" (value is usually in thousands for 13F info table)
  // We'll try a few robust regexes.
  const regexes = [
    /TOTAL\s+VALUE\s*\(?x?\s*1000\)?\s*[:\-]?\s*\$?\s*([0-9,]+)/i,
    /TOTAL\s+VALUE\s*[:\-]?\s*\$?\s*([0-9,]+)/i,
    /TOTAL\s+(\$?\s*[0-9,]+)\s*(?:\(x?\s*1000\))?/i,
  ];

  for (const r of regexes) {
    const m = text.match(r);
    if (m && m[1]) {
      const v = Number(String(m[1]).replace(/[^0-9]/g, ""));
      if (!Number.isNaN(v) && v > 0) {
        // many 13F totals are expressed "x1000"
        // if the match explicitly included x1000 text, multiply; otherwise keep as-is.
        const near = m[0].toLowerCase();
        const isX1000 =
          near.includes("1000") || near.includes("x1000") || near.includes("x 1000");
        return isX1000 ? v * 1000 : v;
      }
    }
  }

  // fallback: scan for a big number near "TOTAL" and "VALUE"
  const fallback = text.match(/TOTAL.*?VALUE.*?([0-9]{1,3}(?:,[0-9]{3})+)/i);
  if (fallback && fallback[1]) {
    const v = Number(fallback[1].replace(/,/g, ""));
    if (!Number.isNaN(v) && v > 0) return v;
  }

  return null;
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

    // Step 3: Process each filing to extract total value
    const rows = [];

    for (const filing of filings) {
      try {
        // ✅ Use reportDate (quarter being reported), not filingDate (when they filed)
        const periodEnd = filing.reportDate; // Format: YYYY-MM-DD (quarter end)
        if (!periodEnd) {
          console.warn(`Missing reportDate for ${filing.accessionNumber}`);
          continue;
        }

        // Build URL to the filing
        const accessionNum = filing.accessionNumber;
        const accPath = accessionNum.replace(/-/g, "");

        // Try to fetch the filing primary document index page
        // Example:
        // https://www.sec.gov/Archives/edgar/data/{cikNoLeadingZeros}/{accessionNoNoDashes}/index.html
        const cikNoLeadingZeros = String(Number(cik)); // strip leading zeros
        const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cikNoLeadingZeros}/${accPath}/index.html`;

        const indexHtml = await fetch(indexUrl, {
          headers: {
            "User-Agent":
              process.env.SEC_USER_AGENT ||
              "MyApp/1.0 (contact: youremail@example.com)",
          },
        }).then((r) => {
          if (!r.ok) throw new Error(`SEC index fetch failed: ${r.status}`);
          return r.text();
        });

        // Parse index to find the actual filing document (prefer .txt or .html)
        const $ = cheerio.load(indexHtml);
        let docHref = null;

        // Prefer "primaryDocument" style links
        $("table.tableFile a").each((_, a) => {
          const href = $(a).attr("href");
          const t = $(a).text().toLowerCase();
          if (!href) return;

          // pick first plausible 13F primary doc
          if (!docHref) {
            if (
              href.endsWith(".txt") ||
              href.endsWith(".htm") ||
              href.endsWith(".html")
            ) {
              docHref = href;
            }
          }

          // stronger preference: something containing "13f"
          if (t.includes("13f") && (href.endsWith(".htm") || href.endsWith(".html"))) {
            docHref = href;
          }
        });

        if (!docHref) {
          console.warn(`No primary doc link found for ${accessionNum}`);
          continue;
        }

        // docHref is usually relative like /Archives/edgar/data/.../filename.htm
        const filingUrl = docHref.startsWith("http")
          ? docHref
          : `https://www.sec.gov${docHref}`;

        const filingHtml = await fetch(filingUrl, {
          headers: {
            "User-Agent":
              process.env.SEC_USER_AGENT ||
              "MyApp/1.0 (contact: youremail@example.com)",
          },
        }).then((r) => {
          if (!r.ok) throw new Error(`SEC filing fetch failed: ${r.status}`);
          return r.text();
        });

        const totalValue = extractTotalValueFromHtml(filingHtml);
        if (!totalValue) {
          console.warn(`Could not extract total value for ${accessionNum}`);
          continue;
        }

        rows.push({
          cik,
          manager_name: canonicalName,
          type: category,
          period_end: periodEnd,
          aum: totalValue,
        });
      } catch (e) {
        console.warn(`Error processing filing:`, e.message);
        // continue to next filing
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
      INSERT INTO manager_quarter (cik, manager_name, type, period_end, aum)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (cik, period_end)
      DO UPDATE SET
        manager_name = EXCLUDED.manager_name,
        type = EXCLUDED.type,
        aum = EXCLUDED.aum
    `;

    for (const r of rows) {
      await pool.query(upsertSql, [
        r.cik,
        r.manager_name,
        r.type,
        r.period_end,
        r.aum,
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
