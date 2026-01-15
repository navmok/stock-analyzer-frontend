import { stat, writeFile } from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "..", "public");

const CSV_FILES = [
  "scrape_bid_polygon_0.15.csv",
  "scrape_bid_polygon_0.1.csv",
  "scrape_bid_yf.csv",
];

async function main() {
  const timestamps = {};

  for (const file of CSV_FILES) {
    const filePath = path.join(publicDir, file);
    try {
      const info = await stat(filePath);
      const key = file.replace(".csv", "");
      timestamps[key] = info.mtime.toISOString();
      console.log(`${file}: ${info.mtime.toISOString()}`);
    } catch (err) {
      console.warn(`Skipping ${file}: ${err.message}`);
    }
  }

  const outPath = path.join(publicDir, "data-timestamp.json");
  await writeFile(outPath, JSON.stringify(timestamps, null, 2));
  console.log(`\nWritten to ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
