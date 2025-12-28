import { useEffect, useMemo, useState } from "react";

const fmtPct = (x) => (x == null ? "" : `${(x * 100).toFixed(1)}%`);
const fmtNum = (x) => (x == null ? "" : Number(x).toLocaleString());
const fmtAum = (r) => {
  if (!r) return "";
  if (typeof r.aum_usd === "number") {
    return `$${fmtNum(Math.round(r.aum_usd / 1_000_000))} M`;
  }
  if (typeof r.aum_m === "number") {
    return `$${fmtNum(Math.round(r.aum_m))} M`;
  }
  return "";
};

function downloadCSV(filename, rows) {
  const cols = ["manager", "category", "cik", "aum_m", "qoq_pct", "yoy_pct", "pct_5y", "pct_10y", "num_holdings"];
  const header = cols.join(",");
  const body = rows
    .map((r) =>
      cols
        .map((c) => {
          const v = r[c];
          const s = v == null ? "" : String(v);
          return `"${s.replace(/"/g, '""')}"`;
        })
        .join(",")
    )
    .join("\n");

  const blob = new Blob([header + "\n" + body], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export default function HedgeFundTable() {
  const [period, setPeriod] = useState("2025Q3");
  const [search, setSearch] = useState("");
  const [types, setTypes] = useState({
    hedge_fund: true,
    bank: true,
    asset_manager: true,
    other: true,
  });

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // sorting
  const [sortKey, setSortKey] = useState("aum_m");
  const [sortDir, setSortDir] = useState("desc"); // asc | desc

  function toggleSort(key) {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir("desc");
      return key;
    });
  }

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setErr("");

      const selected = Object.entries(types)
        .filter(([, v]) => v)
        .map(([k]) => `type=${encodeURIComponent(k)}`)
        .join("&");

      const url =
        `/api/hedgefunds?period=${encodeURIComponent(period)}` +
        `&search=${encodeURIComponent(search)}` +
        (selected ? `&${selected}` : "");

      try {
        const r = await fetch(url);
        const j = await r.json();
        if (!r.ok) throw new Error(j?.error || "Failed to load");

        if (!cancelled) setRows(Array.isArray(j) ? j : []);
      } catch (e) {
        if (!cancelled) setErr(String(e?.message || e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    const t = setTimeout(load, 200); // debounce typing
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [period, search, types]);

  const sortedRows = useMemo(() => {
    const arr = [...rows];
    const dir = sortDir === "asc" ? 1 : -1;

    arr.sort((a, b) => {
      const va = a?.[sortKey];
      const vb = b?.[sortKey];

      // numbers first
      const na = typeof va === "number" ? va : va == null ? NaN : Number(va);
      const nb = typeof vb === "number" ? vb : vb == null ? NaN : Number(vb);

      const bothNum = !Number.isNaN(na) && !Number.isNaN(nb);
      if (bothNum) return (na - nb) * dir;

      // string fallback
      const sa = String(va ?? "");
      const sb = String(vb ?? "");
      return sa.localeCompare(sb) * dir;
    });

    return arr;
  }, [rows, sortKey, sortDir]);

  const movers = useMemo(() => {
    const valid = rows.filter((r) => typeof r.qoq_pct === "number");
    const topGainers = [...valid].sort((a, b) => (b.qoq_pct - a.qoq_pct)).slice(0, 10);
    const topLosers = [...valid].sort((a, b) => (a.qoq_pct - b.qoq_pct)).slice(0, 10);
    return { topGainers, topLosers };
  }, [rows]);

  const SortHeader = ({ label, k, align = "left" }) => (
    <th
      align={align}
      style={{ cursor: "pointer", userSelect: "none" }}
      onClick={() => toggleSort(k)}
      title="Click to sort"
    >
      {label} {sortKey === k ? (sortDir === "asc" ? "▲" : "▼") : ""}
    </th>
  );

  return (
    <div style={{ padding: 12 }}>
      <h2 style={{ marginTop: 0 }}>Hedge Fund Performance (13F)</h2>

      <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
        <label>
          Period:&nbsp;
          <input value={period} onChange={(e) => setPeriod(e.target.value)} style={{ width: 90 }} />
        </label>

        <label>
          Search:&nbsp;
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Manager name or CIK..."
            style={{ width: 240 }}
          />
        </label>

        <button onClick={() => downloadCSV(`hedgefunds_${period}.csv`, sortedRows)}>Export CSV</button>

        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <span style={{ opacity: 0.8 }}>Type:</span>
          {[
            ["hedge_fund", "Hedge Fund"],
            ["bank", "Bank"],
            ["asset_manager", "Asset Manager"],
            ["other", "Other"],
          ].map(([k, label]) => (
            <label key={k} style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input
                type="checkbox"
                checked={types[k]}
                onChange={(e) => setTypes((p) => ({ ...p, [k]: e.target.checked }))}
              />
              {label}
            </label>
          ))}
        </div>
      </div>

      {err && <div style={{ marginTop: 10, color: "salmon" }}>{err}</div>}

      <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
        <div style={{ minWidth: 320 }}>
          <div style={{ fontWeight: 700, marginBottom: 6 }}>Top Gainers QoQ</div>
          <ol style={{ marginTop: 0 }}>
            {movers.topGainers.map((r) => (
              <li key={r.cik}>
                {r.manager} — {fmtPct(r.qoq_pct)}
              </li>
            ))}
          </ol>
        </div>
        <div style={{ minWidth: 320 }}>
          <div style={{ fontWeight: 700, marginBottom: 6 }}>Top Losers QoQ</div>
          <ol style={{ marginTop: 0 }}>
            {movers.topLosers.map((r) => (
              <li key={r.cik}>
                {r.manager} — {fmtPct(r.qoq_pct)}
              </li>
            ))}
          </ol>
        </div>
      </div>

      <div style={{ marginTop: 10, opacity: 0.8 }}>
        {loading ? "Loading..." : `Showing ${sortedRows.length} managers`}
      </div>

      <div style={{ overflowX: "auto", marginTop: 8 }}>
        <table width="100%" cellPadding="6" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.15)" }}>
              <SortHeader label="Manager" k="manager" />
              <SortHeader label="Type" k="category" />
              <SortHeader label="CIK" k="cik" />
              <SortHeader label="AUM ($M)" k="aum_m" align="right" />
              <SortHeader label="QoQ %" k="qoq_pct" align="right" />
              <SortHeader label="YoY %" k="yoy_pct" align="right" />
              <SortHeader label="5Y %" k="pct_5y" align="right" />
              <SortHeader label="10Y %" k="pct_10y" align="right" />
              <SortHeader label="#Holdings" k="num_holdings" align="right" />
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((r) => (
              <tr key={r.cik} style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                <td>{r.manager}</td>
                <td>{r.category}</td>
                <td>{r.cik}</td>
                <td align="right">{fmtAum(r)}</td>
                <td align="right">{fmtPct(r.qoq_pct)}</td>
                <td align="right">{fmtPct(r.yoy_pct)}</td>
                <td align="right">{fmtPct(r.pct_5y)}</td>
                <td align="right">{fmtPct(r.pct_10y)}</td>
                <td align="right">{fmtNum(r.num_holdings)}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {!loading && sortedRows.length === 0 && (
          <div style={{ marginTop: 10 }}>No data for {period} (try another period)</div>
        )}
      </div>
    </div>
  );
}
