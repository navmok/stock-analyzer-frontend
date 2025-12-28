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

function quarterEndDate(qStr) {
  // "Q3 2025" -> "2025-09-30"
  const m = qStr.match(/^Q([1-4])\s+(\d{4})$/);
  if (!m) return null;
  const q = Number(m[1]);
  const y = Number(m[2]);
  const end = { 1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31" }[q];
  return `${y}-${end}`;
}

// 🔹 NEW: classify manager type (MVP rules)
function classifyManager(nameRaw) {
  const name = (nameRaw || "").toLowerCase();

  // Banks / broker-dealers
  const bankKeywords = [
    "bank", "bancorp", "bancshares", "national association", "n.a.",
    "jpmorgan", "jp morgan", "goldman", "morgan stanley", "citigroup", "citi",
    "wells fargo", "bank of america", "bofa", "barclays", "hsbc", "ubs",
    "credit suisse", "deutsche", "bnp", "societe generale", "nomura"
  ];
  if (bankKeywords.some(k => name.includes(k))) return "bank";

  // Large asset managers
  const assetMgrKeywords = [
    "vanguard", "blackrock", "state street", "ssga", "fidelity",
    "t. rowe", "t rowe", "invesco", "franklin", "capital group",
    "american funds", "pimco", "alliancebernstein", "nuveen",
    "principal", "dimensional", "dodge & cox", "dodge and cox"
  ];
  if (assetMgrKeywords.some(k => name.includes(k))) return "asset_manager";

  // Hedge funds / trading firms (require 2+ signals)
  const hedgeSignals = [
    "capital management", "investment management", "investments",
    "advisors", "adviser", "partners", "fund", "funds",
    "quant", "trading", "group", "llc", "l.p.", "lp"
  ];
  const hits = hedgeSignals.filter(k => name.includes(k)).length;
  if (hits >= 2) return "hedge_fund";

  return "other";
}

export default async function handler(req, res) {
  try {
    const cik = (req.query.cik || "").trim();
    if (!/^\d{10}$/.test(cik)) {
      return res.status(400).json({ error: "Provide cik as 10 digits, e.g. 0001595888" });
    }

    // Step 1: Fetch manager info from SEC's JSON API
    let managerName = cik;
    
    try {
      const submissionsUrl = `https://data.sec.gov/submissions/CIK${cik}.json`;
      const submissionsData = await fetch(submissionsUrl).then((r) => {
        if (!r.ok) throw new Error(`SEC submissions fetch failed: ${r.status}`);
        return r.json();
      });
      
      if (submissionsData.name) {
        managerName = submissionsData.name;
      }
    } catch (e) {
      console.warn(`Could not fetch manager name from SEC for ${cik}:`, e.message);
      return res.status(500).json({ error: `Could not fetch manager from SEC: ${e.message}` });
    }
    
    // 🔹 classify manager
    const category = classifyManager(managerName);

    // Step 2: Get 13F filings from SEC JSON API (more reliable than HTML parsing)
    let filings = [];
    try {
      const submissionsUrl = `https://data.sec.gov/submissions/CIK${cik}.json`;
      const submissionsData = await fetch(submissionsUrl).then((r) => {
        if (!r.ok) throw new Error(`SEC submissions fetch failed: ${r.status}`);
        return r.json();
      });
      
      if (submissionsData.filings?.recent?.filings) {
        // Filter for 13F-HR and 13F/A forms
        filings = submissionsData.filings.recent.filings
          .filter(f => f.form === "13F-HR" || f.form === "13F/A")
          .slice(0, 10); // Get last 10 filings
      }
    } catch (e) {
      console.warn(`Could not fetch filings from SEC for ${cik}:`, e.message);
      return res.status(500).json({ error: `Could not fetch 13F filings: ${e.message}` });
    }

    if (filings.length === 0) {
      return res.status(500).json({ error: "No 13F filings found on SEC for this CIK." });
    }

    // Step 3: Process each filing to extract total value
    const rows = [];
    
    for (const filing of filings) {
      try {
        const filingDate = filing.filingDate; // Format: YYYY-MM-DD
        if (!filingDate) continue;

        // Determine quarter from filing date
        const [year, month, day] = filingDate.split("-");
        const m = Number(month);
        const q = Math.floor((m - 1) / 3) + 1;
        const quarterMonths = [3, 6, 9, 12];
        const quarterDays = [31, 30, 30, 31];
        const periodEnd = `${year}-${String(quarterMonths[q - 1]).padStart(2, "0")}-${String(quarterDays[q - 1]).padStart(2, "0")}`;

        // Build URL to the filing
        const accessionNum = filing.accessionNumber;
        const accPath = accessionNum.replace(/-/g, "");
        
        // Try to fetch the XBRL instance document JSON
        const xbrlUrl = `https://www.sec.gov/Archives/${filing.cikNumber.padStart(10, "0")}/${accPath}/${accessionNum}-xbrl.json`;
        
        let totalValueM = null;

        // Method 1: Try to fetch XBRL JSON
        try {
          const xbrlData = await fetch(xbrlUrl).then(r => {
            if (!r.ok) return null;
            return r.json();
          });
          
          if (xbrlData) {
            // Look for the value in the XBRL structure
            // The tag is typically "us-gaap:securitiesOwnedAggregateValue" or similar
            for (const key in xbrlData) {
              if (key.includes("securitiesOwned") || key.includes("AggregateValue")) {
                const val = xbrlData[key];
                if (typeof val === 'object' && val.value) {
                  totalValueM = Number(val.value) / 1000; // Convert from thousands to millions
                  break;
                } else if (typeof val === 'number') {
                  totalValueM = val / 1000;
                  break;
                }
              }
            }
          }
        } catch (e) {
          console.warn(`Could not fetch XBRL for ${accessionNum}:`, e.message);
        }

        // Method 2: Fallback - fetch the HTML filing document and parse it
        if (!totalValueM) {
          try {
            const filingHtmlUrl = `https://www.sec.gov/Archives/${filing.cikNumber.padStart(10, "0")}/${accPath}/${accessionNum}.txt`;
            const filingText = await fetch(filingHtmlUrl).then(r => {
              if (!r.ok) return null;
              return r.text();
            });
            
            if (filingText) {
              // Look for "Aggregate Value of Holdings"
              const match = filingText.match(/Aggregate\s+Value[\s\S]{0,200}?\$\s*([\d,]+(?:\.\d+)?)/i);
              if (match) {
                totalValueM = Number(match[1].replace(/,/g, "")) / 1000; // Convert thousands to millions
              } else {
                // Try alternate format
                const altMatch = filingText.match(/confHoldValue[>\s]+([\d]+)/i);
                if (altMatch) {
                  totalValueM = Number(altMatch[1]) / 1000;
                }
              }
            }
          } catch (e) {
            console.warn(`Could not fetch filing text for ${accessionNum}:`, e.message);
          }
        }

        // If we found a value, store it
        if (totalValueM && totalValueM > 0) {
          rows.push({
            cik,
            managerName,
            periodEnd,
            totalValueM,
            numHoldings: null,
          });
        } else {
          console.warn(`No value found for ${accessionNum} (${filingDate})`);
        }
      } catch (e) {
        console.warn(`Error processing filing:`, e.message);
        continue;
      }
    }

    if (rows.length === 0) {
      return res.status(500).json({ error: "Could not parse any 13F values from SEC filings." });
    }

    const sql = `
      insert into manager_quarter (cik, manager_name, period_end, total_value_m, num_holdings)
      values ($1, $2, $3::date, $4, $5)
      on conflict (cik, period_end) do update
      set manager_name = excluded.manager_name,
          total_value_m = excluded.total_value_m,
          num_holdings  = excluded.num_holdings;
    `;

    const db = getPool();
    
    // upsert quarterly values
    for (const r of rows) {
      await db.query(sql, [
        r.cik,
        r.managerName,
        r.periodEnd,
        r.totalValueM,
        r.numHoldings
      ]);
    }

    // 🔹 NEW: upsert classification
    await db.query(
      `
      insert into manager_classification (cik, manager_name, category)
      values ($1,$2,$3)
      on conflict (cik) do update
      set manager_name = excluded.manager_name,
          category = excluded.category,
          updated_at = now()
      `,
      [cik, managerName, category]
    );

    return res.status(200).json({
      ok: true,
      source: "SEC.gov EDGAR",
      manager: managerName,
      upserted_rows: rows.length,
      sample: rows.slice(0, 3),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
