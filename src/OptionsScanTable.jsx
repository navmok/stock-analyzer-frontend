import { useEffect, useMemo, useState } from "react";

const fmtNum = (x, digits = 2) => {
  if (x == null) return "";
  const n = Number(x);
  if (Number.isNaN(n)) return "";
  return n.toFixed(digits);
};

const fmtPct = (x, digits = 1) => {
  if (x == null) return "";
  const n = Number(x);
  if (Number.isNaN(n)) return "";
  // If your DB stores 0.25 = 25% (fraction), keep this:
  return `${(n * 100).toFixed(digits)}%`;
  // If your DB stores 25 = 25 (already percent), use instead:
  // return `${n.toFixed(digits)}%`;
};

export default function OptionsScanTable() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // client-side sorting (default: roi_annualized desc)
  const [sortKey, setSortKey] = useState("roi_annualized");
  const [sortDir, setSortDir] = useState("desc");

  function toggleSort(k) {
    setSortKey((prev) => {
      if (prev === k) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir("desc");
      return k;
    });
  }

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setErr("");
      try {
        const r = await fetch("/api/options-scan?limit=100");
        const j = await r.json();
        if (!r.ok) throw new Error(j?.error || "Failed to load");
        if (!cancelled) setRows(Array.isArray(j) ? j : []);
      } catch (e) {
        if (!cancelled) setErr(String(e?.message || e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const sortedRows = useMemo(() => {
    const arr = [...rows];
    const dir = sortDir === "asc" ? 1 : -1;

    arr.sort((a, b) => {
      const va = a?.[sortKey];
      const vb = b?.[sortKey];

      const na = typeof va === "number" ? va : va == null ? NaN : Number(va);
      const nb = typeof vb === "number" ? vb : vb == null ? NaN : Number(vb);

      const bothNum = !Number.isNaN(na) && !Number.isNaN(nb);
      if (bothNum) return (na - nb) * dir;

      const sa = String(va ?? "");
      const sb = String(vb ?? "");
      return sa.localeCompare(sb) * dir;
    });

    return arr;
  }, [rows, sortKey, sortDir]);

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
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
        <h2 style={{ margin: 0 }}>Options Scan (Top 100)</h2>
        <button onClick={() => window.location.reload()} style={{ padding: "6px 10px" }}>
          Refresh
        </button>
      </div>

      {err && <div style={{ marginTop: 10, color: "salmon" }}>{err}</div>}
      <div style={{ marginTop: 10, opacity: 0.8 }}>{loading ? "Loading..." : `Showing ${sortedRows.length} rows`}</div>

      <div style={{ overflowX: "auto", marginTop: 8 }}>
        <table width="100%" cellPadding="6" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.15)" }}>
              <SortHeader label="Symbol" k="symbol" />
              <SortHeader label="Trade Date" k="trade_dt" />
              <SortHeader label="Spot" k="spot" align="right" />
              <SortHeader label="Exp" k="exp" />
              <SortHeader label="DTE" k="dte" align="right" />
              <SortHeader label="Premium" k="premium" align="right" />
              <SortHeader label="IV" k="iv" align="right" />
              <SortHeader label="Delta" k="delta" align="right" />
              <SortHeader label="POP" k="pop" align="right" />
              <SortHeader label="Moneyness" k="moneyness" align="right" />
              <SortHeader label="ROI" k="roi" align="right" />
              <SortHeader label="ROI Ann." k="roi_annualized" align="right" />
            </tr>
          </thead>

          <tbody>
            {sortedRows.map((r, i) => (
              <tr key={`${r.symbol}-${r.exp}-${i}`} style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                <td>{r.symbol}</td>
                <td>{r.trade_dt}</td>
                <td align="right">{r.spot == null ? "" : `$${fmtNum(r.spot, 2)}`}</td>
                <td>{r.exp}</td>
                <td align="right">{r.dte ?? ""}</td>
                <td align="right">{r.premium == null ? "" : `$${fmtNum(r.premium, 2)}`}</td>
                <td align="right">{fmtPct(r.iv, 1)}</td>
                <td align="right">{fmtNum(r.delta, 3)}</td>
                <td align="right">{fmtPct(r.pop, 1)}</td>
                <td align="right">{fmtNum(r.moneyness, 3)}</td>
                <td align="right">{fmtPct(r.roi, 1)}</td>
                <td align="right">{fmtPct(r.roi_annualized, 1)}</td>
              </tr>
            ))}

            {!loading && sortedRows.length === 0 && (
              <tr>
                <td colSpan="12" style={{ textAlign: "center" }}>
                  No data
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
