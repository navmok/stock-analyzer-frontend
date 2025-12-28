import * as cheerio from "cheerio";
import pg from "pg";
const { Pool } = pg;

const APP_BASE = "https://stock-analyzer-frontend-seven.vercel.app";
const MIN_B = 1.0; // >= $1B

function parseHoldingsToBillions(s) {
  // examples: "$1.4 B", "$120 M", "$51 k", "$6.7 T"
  const m = String(s).replace(/\s+/g, " ").match(/\$([\d.]+)\s*([kMBT])/i);
  if (!m) return null;

  const n = Number(m[1]);
  const unit = m[2].toUpperCase();

  if (unit === "K") return n / 1e6;  // K -> billions
  if (unit === "M") return n / 1e3;  // M -> billions
  if (unit === "B") return n;        // B -> billions
  if (unit === "T") return n * 1e3;  // T -> billions
  return null;
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function getManagersFromDB() {
  // Get all unique managers from the database
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });
  
  const { rows } = await pool.query(`
    SELECT DISTINCT ON (cik) cik, manager_name, total_value_m
    FROM manager_quarter
    ORDER BY cik, period_end DESC
  `);
  
  await pool.end();
  
  // Filter to those >= $1B and get unique CIKs
  const byCik = new Map();
  for (const row of rows) {
    const b = Number(row.total_value_m) / 1000; // Convert from millions to billions
    if (b >= MIN_B) {
      if (!byCik.has(row.cik)) {
        byCik.set(row.cik, {
          cik: row.cik,
          name: row.manager_name,
          b: b
        });
      }
    }
  }
  
  return Array.from(byCik.values()).sort((a, b) => b.b - a.b);
}

async function main() {
  console.log("Fetching managers from database...");
  const list = await getManagersFromDB();

  console.log(`\nFound ${list.length} managers >= $${MIN_B}B`);
  console.log(list.slice(0, 15).map(x => `${x.cik} ${x.name} ($${x.b.toFixed(1)}B)`).join("\n"));

  // Sync into your Neon DB using your serverless endpoint
  let ok = 0;
  let fail = 0;

  for (let i = 0; i < list.length; i++) {
    const { cik, name } = list[i];
    const syncUrl = `${APP_BASE}/api/sync13f?cik=${cik}`;
    try {
      const r = await fetch(syncUrl);
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        fail++;
        console.log(`❌ ${i + 1}/${list.length} ${cik} ${name}: ${j.error || r.status}`);
      } else {
        ok++;
        console.log(`✅ ${i + 1}/${list.length} ${cik} ${name}: upserted ${j.upserted_rows ?? "?"}`);
      }
    } catch (e) {
      fail++;
      console.log(`❌ ${i + 1}/${list.length} ${cik} ${name}: ${e.message}`);
    }

    // be polite (avoid hammering)
    await sleep(250);
  }

  console.log(`\nDONE: ok=${ok} fail=${fail}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
