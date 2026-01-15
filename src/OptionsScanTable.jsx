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
  // Values coming from both /api/options-scan and /api/options-scan-live are already in percent units.
  return `${n.toFixed(digits)}%`;
};

const fmtDate = (d) => (d ? String(d).slice(0, 10) : "");

export default function OptionsScanTable() {
  const [rows, setRows] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [refreshTick, setRefreshTick] = useState(0);

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
        const force = refreshTick > 0 ? "&refresh=1" : "";
        const r = await fetch(`/api/options-metrics-yf?limit=200&moneyness=0.85${force}`);
        const j = await r.json();
        if (!r.ok) throw new Error(j?.error || "Failed to load");
        if (!cancelled) {
          setRows(Array.isArray(j?.rows) ? j.rows : (Array.isArray(j) ? j : []));
          setMeta(j?.meta || null);
        }
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
  }, [refreshTick]);

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

  const headerStyle = {
    position: "sticky",
    top: 0,
    background: "rgba(12, 18, 35, 0.98)",
    backdropFilter: "blur(6px)",
    zIndex: 10,
  };

  const tableContainerStyle = {
    marginTop: 8,
    maxHeight: "70vh",
    overflow: "auto",
  };

  const SortHeader = ({ label, k, align = "left", minWidth }) => (
    <th
      align={align}
      style={{
        cursor: "pointer",
        userSelect: "none",
        ...headerStyle,
        minWidth,
      }}
      onClick={() => toggleSort(k)}
      title="Click to sort"
    >
      {label} {sortKey === k ? (sortDir === "asc" ? "▲" : "▼") : ""}
    </th>
  );

  return (
    <div style={{ padding: 12 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
        <h2 style={{ margin: 0 }}>Options Metrics (Yahoo Finance)</h2>
        <button onClick={() => setRefreshTick((x) => x + 1)} style={{ padding: "6px 10px" }}>
          Refresh (force re-run)
        </button>
      </div>

      {err && <div style={{ marginTop: 10, color: "salmon" }}>{err}</div>}
      <div style={{ marginTop: 10, opacity: 0.8 }}>
        {loading
          ? "Loading..."
          : `Showing ${sortedRows.length} rows${meta?.total ? ` of ${meta.total} filtered` : ""}`}
      </div>

      <div style={tableContainerStyle}>
        <table width="100%" cellPadding="6" style={{ borderCollapse: "collapse", tableLayout: "fixed" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.15)", background: headerStyle.background }}>
                <SortHeader label="Ticker" k="ticker" minWidth="90px" />
                <SortHeader label="Trade Date" k="trade_dt" minWidth="110px" />
                <SortHeader label="Expiry" k="expiry" minWidth="110px" />
                <SortHeader label="Option" k="option_ticker" minWidth="160px" />
                <SortHeader label="Spot" k="spot" align="right" minWidth="90px" />
                <SortHeader label="Strike" k="strike" align="right" minWidth="90px" />
                <SortHeader label="Moneyness" k="moneyness" align="right" minWidth="100px" />
                <SortHeader label="Premium" k="premium" align="right" minWidth="100px" />
                <SortHeader label="IV" k="iv" align="right" minWidth="70px" />
                <SortHeader label="Delta" k="delta" align="right" minWidth="80px" />
                <SortHeader label="ROI" k="roi" align="right" minWidth="80px" />
                <SortHeader label="ROI Ann." k="roi_annualized" align="right" minWidth="100px" />
            </tr>
          </thead>

          <tbody>
            {sortedRows.map((r, i) => (
              <tr key={`${r.ticker}-${r.option_ticker || r.expiry || i}`} style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                <td>{r.ticker}</td>
                <td>{fmtDate(r.trade_dt)}</td>
                <td>{fmtDate(r.expiry)}</td>
                <td style={{ fontFamily: "monospace" }}>{r.option_ticker}</td>
                <td align="right">{r.spot == null ? "" : `$${fmtNum(r.spot, 2)}`}</td>
                <td align="right">{r.strike == null ? "" : `$${fmtNum(r.strike, 2)}`}</td>
                <td align="right">{fmtNum(r.moneyness, 3)}</td>
                <td align="right">{r.premium == null ? "" : `$${fmtNum(r.premium, 2)}`}</td>
                <td align="right">{fmtPct(r.iv, 1)}</td>
                <td align="right">{fmtNum(r.delta, 4)}</td>
                <td align="right">{fmtPct(r.roi, 2)}</td>
                <td align="right">{fmtPct(r.roi_annualized, 2)}</td>
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
