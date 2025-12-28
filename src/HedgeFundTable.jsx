import { useEffect, useMemo, useState } from "react";

const fmtPct = (x) => (x == null ? "" : `${(x * 100).toFixed(1)}%`);
const fmtNum = (x) => (x == null ? "" : Number(x).toLocaleString());

const ALL_CATS = ["hedge_fund", "bank", "asset_manager", "other"];

function prettyCat(cat) {
  if (cat === "hedge_fund") return "Hedge Fund";
  if (cat === "bank") return "Bank";
  if (cat === "asset_manager") return "Asset Manager";
  return "Other";
}

function exportToCSV(period, data) {
  const cols = [
    "manager",
    "category",
    "cik",
    "period_end",
    "aum_m",
    "num_holdings",
    "qoq_pct",
    "yoy_pct",
    "pct_5y",
    "pct_10y",
  ];

  const esc = (v) => {
    const s = v === null || v === undefined ? "" : String(v);
    return `"${s.replace(/"/g, '""')}"`;
  };

  const lines = [
    cols.join(","),
    ...data.map((r) =>
      cols.map((c) => esc(r[c])).join(",")
    ),
  ];

  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `hedgefunds_${period}_filtered.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export default function HedgeFundTable() {
  const [period, setPeriod] = useState("2025Q3");

  // Search + filters
  const [search, setSearch] = useState("");
  const [selectedCats, setSelectedCats] = useState(new Set(ALL_CATS));

  // Data
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // Fetch once per period; filter locally for multi-select + search
  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setErr("");
      try {
        const r = await fetch(
          `/api/hedgefunds?period=${encodeURIComponent(period)}&category=all`
        );
        const j = await r.json();
        if (!r.ok) throw new Error(j?.error || "API error");
        if (!cancelled) setRows(Array.isArray(j) ? j : []);
      } catch (e) {
        if (!cancelled) setErr(String(e.message || e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [period]);

  const filteredRows = useMemo(() => {
    const q = search.trim().toLowerCase();
    return (rows || []).filter((r) => {
      const cat = (r.category || "other").toLowerCase();
      if (!selectedCats.has(cat)) return false;

      if (!q) return true;
      const mgr = String(r.manager || "").toLowerCase();
      const cik = String(r.cik || "").toLowerCase();
      return mgr.includes(q) || cik.includes(q);
    });
  }, [rows, search, selectedCats]);

  const { topGainers, topLosers } = useMemo(() => {
    const movers = filteredRows
      .filter((r) => typeof r.qoq_pct === "number")
      .sort((a, b) => b.qoq_pct - a.qoq_pct);

    return {
      topGainers: movers.slice(0, 10),
      topLosers: movers.slice(-10).reverse(),
    };
  }, [filteredRows]);

  return (
    <div className="table-wrapper">
      {/* Header controls */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 10, flexWrap: "wrap" }}>
        <h2 style={{ margin: 0 }}>Hedge Fund Performance (13F)</h2>

        <label style={{ marginLeft: "auto" }}>
          Period:&nbsp;
          <input
            value={period}
            onChange={(e) => setPeriod(e.target.value)}
            placeholder="2025Q3"
            style={{ padding: "6px 8px", borderRadius: 8 }}
          />
        </label>

        <label>
          Search:&nbsp;
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Manager name or CIK…"
            style={{ padding: "6px 8px", borderRadius: 8, minWidth: 220 }}
          />
        </label>

        <button
          onClick={() => exportToCSV(period, filteredRows)}
          style={{ padding: "6px 10px", borderRadius: 8, cursor: "pointer" }}
          title="Export current filtered rows"
          disabled={filteredRows.length === 0}
        >
          Export CSV
        </button>

        {/* Multi-select category checkboxes */}
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <strong>Type:</strong>
          {ALL_CATS.map((cat) => (
            <label key={cat} style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input
                type="checkbox"
                checked={selectedCats.has(cat)}
                onChange={(e) => {
                  setSelectedCats((prev) => {
                    const next = new Set(prev);
                    if (e.target.checked) next.add(cat);
                    else next.delete(cat);
                    return next;
                  });
                }}
              />
              {prettyCat(cat)}
            </label>
          ))}
        </div>
      </div>

      {err && <div style={{ color: "#fca5a5", marginBottom: 8 }}>{err}</div>}

      {loading ? (
        <div>Loading…</div>
      ) : (
        <>
          {/* Top movers QoQ */}
          <div style={{ display: "flex", gap: 16, marginBottom: 12, flexWrap: "wrap" }}>
            <div style={{ minWidth: 340, padding: 12, border: "1px solid #334155", borderRadius: 12 }}>
              <h3 style={{ marginTop: 0 }}>Top Gainers QoQ</h3>
              {topGainers.length === 0 ? (
                <div style={{ opacity: 0.8 }}>No QoQ data.</div>
              ) : (
                <ol style={{ margin: 0, paddingLeft: 18 }}>
                  {topGainers.map((r) => (
                    <li key={`g-${r.cik}-${r.period_end}`}>
                      {r.manager} — {fmtPct(r.qoq_pct)}
                    </li>
                  ))}
                </ol>
              )}
            </div>

            <div style={{ minWidth: 340, padding: 12, border: "1px solid #334155", borderRadius: 12 }}>
              <h3 style={{ marginTop: 0 }}>Top Losers QoQ</h3>
              {topLosers.length === 0 ? (
                <div style={{ opacity: 0.8 }}>No QoQ data.</div>
              ) : (
                <ol style={{ margin: 0, paddingLeft: 18 }}>
                  {topLosers.map((r) => (
                    <li key={`l-${r.cik}-${r.period_end}`}>
                      {r.manager} — {fmtPct(r.qoq_pct)}
                    </li>
                  ))}
                </ol>
              )}
            </div>
          </div>

          {/* Table */}
          <div style={{ overflowX: "auto" }}>
            <div style={{ marginBottom: 8, opacity: 0.85 }}>
              Showing <strong>{filteredRows.length}</strong> of {rows.length} managers
            </div>

            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th align="left">Manager</th>
                  <th align="left">Type</th>
                  <th align="left">CIK</th>
                  <th align="right">AUM ($M)</th>
                  <th align="right">QoQ %</th>
                  <th align="right">YoY %</th>
                  <th align="right">5Y %</th>
                  <th align="right">10Y %</th>
                  <th align="right">#Holdings</th>
                </tr>
              </thead>
              <tbody>
                {filteredRows.map((r) => (
                  <tr key={`${r.cik}-${r.period_end}`}>
                    <td>{r.manager}</td>
                    <td>{prettyCat((r.category || "other").toLowerCase())}</td>
                    <td>{r.cik}</td>
                    <td align="right">{fmtNum(r.aum_m)}</td>
                    <td align="right">{fmtPct(r.qoq_pct)}</td>
                    <td align="right">{fmtPct(r.yoy_pct)}</td>
                    <td align="right">{fmtPct(r.pct_5y)}</td>
                    <td align="right">{fmtPct(r.pct_10y)}</td>
                    <td align="right">{fmtNum(r.num_holdings)}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            {filteredRows.length === 0 && <div style={{ marginTop: 8 }}>No matches for current filters.</div>}
          </div>
        </>
      )}
    </div>
  );
}
