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

    const url = `https://13f.info/manager/${cik}`;
    const html = await fetch(url).then((r) => {
      if (!r.ok) throw new Error(`Fetch failed: ${r.status}`);
      return r.text();
    });

    const $ = cheerio.load(html);

    // Manager name on the page header
    const managerName = $("h1").first().text().trim() || cik;
    
    // 🔹 NEW: classify manager
    const category = classifyManager(managerName);

    // Parse rows that look like: Q3 2025 | 10,653 | 657,114,981 | ...
    const rows = [];
    $("table tr").each((_, tr) => {
      const tds = $(tr).find("td");
      if (tds.length < 3) return;

      const quarter = $(tds[0]).text().trim();           // e.g. "Q3 2025"
      const holdingsStr = $(tds[1]).text().trim();       // e.g. "10,653"
      const valueStr = $(tds[2]).text().trim();          // e.g. "657,114,981"

      const periodEnd = quarterEndDate(quarter);
      if (!periodEnd) return;

      const numHoldings = Number(holdingsStr.replace(/,/g, ""));
      const value000 = Number(valueStr.replace(/,/g, "")); // value in $000 per page
      if (!Number.isFinite(numHoldings) || !Number.isFinite(value000)) return;

      // Store in $ millions for your app (because you’re already using total_value_m)
      const totalValueM = value000 / 1000;

      rows.push({ cik, managerName, periodEnd, totalValueM, numHoldings });
    });

    if (rows.length === 0) {
      return res.status(500).json({ error: "No rows parsed. Page layout may have changed." });
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
      source: url,
      manager: managerName,
      upserted_rows: rows.length,
      sample: rows.slice(0, 3),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
