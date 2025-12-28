import { useEffect, useState } from "react";

const fmtPct = (x) => (x == null ? "" : `${(x * 100).toFixed(1)}%`);
const fmtNum = (x) => (x == null ? "" : x.toLocaleString());

export default function HedgeFundTable() {
  const [period, setPeriod] = useState("2025Q3");
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setErr("");
      try {
        const r = await fetch(`/api/hedgefunds?period=${period}`);
        const j = await r.json();
        if (!r.ok) throw new Error(j?.error || "API error");
        if (!cancelled) setRows(j);
      } catch (e) {
        if (!cancelled) setErr(String(e.message || e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [period]);

  return (
    <div className="table-wrapper">
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 10 }}>
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
      </div>

      {err && <div style={{ color: "#fca5a5", marginBottom: 8 }}>{err}</div>}
      {loading ? (
        <div>Loading…</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th align="left">Manager</th>
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
              {rows.map((r) => (
                <tr key={`${r.cik}-${r.period_end}`}>
                  <td>{r.manager}</td>
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

          {rows.length === 0 && !loading && <div style={{ marginTop: 8 }}>No data for {period}</div>}
        </div>
      )}
    </div>
  );
}
