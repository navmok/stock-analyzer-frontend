import { useMemo, useState } from "react";

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function fmt(x, digits = 2) {
  const n = toNum(x);
  if (n === null) return "";
  return n.toFixed(digits);
}

function pct(x, digits = 2) {
  const n = toNum(x);
  if (n === null) return "";
  return (n * 100).toFixed(digits) + "%";
}

function cls(...xs) {
  return xs.filter(Boolean).join(" ");
}

const DEFAULT_COLS = [
  "bid",
  "ask",
  "lastPrice",
  "impliedVolatility",
  "openInterest",
  "volume",
];

const COL_LABEL = {
  bid: "Bid",
  ask: "Ask",
  lastPrice: "Last",
  impliedVolatility: "IV",
  delta: "Delta",
  gamma: "Gamma",
  theta: "Theta",
  vega: "Vega",
  rho: "Rho",
  openInterest: "OI",
  volume: "Vol",
};

export default function OptionsChain({
  options = [],
  symbol,
  expiration,
  setExpiration,
  expirations = [],
  underlyingPrice = null,
}) {
  const [cols, setCols] = useState(DEFAULT_COLS);

  // Sorting
  const [sortKey, setSortKey] = useState("strike");
  const [sortDir, setSortDir] = useState("asc"); // default strike ascending

  // UI toggles
  const [showColPicker, setShowColPicker] = useState(false);
  const [showGreeks, setShowGreeks] = useState(false);

  // Strike band filter
  const [strikeBandPct, setStrikeBandPct] = useState(0.2); // ±20% default

  const allCols = [
    "bid",
    "ask",
    "lastPrice",
    "impliedVolatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "openInterest",
    "volume",
  ];

  const spot = toNum(underlyingPrice);

  const expList = useMemo(() => {
    const set = new Set(
      expirations?.length
        ? expirations
        : options.map((o) => o.expiration).filter(Boolean)
    );
    return Array.from(set).sort();
  }, [expirations, options]);

  const filtered = useMemo(() => {
    return options.filter((o) => !expiration || o.expiration === expiration);
  }, [options, expiration]);

  const displayedCols = useMemo(() => {
    const base = cols.slice();

    // Greeks OFF: remove delta/gamma (and keep everything else)
    if (!showGreeks) {
      return base.filter((c) => c !== "delta" && c !== "gamma");
    }

    // Greeks ON: ensure delta + gamma are present
    const out = base.slice();
    if (!out.includes("delta")) out.push("delta");
    if (!out.includes("gamma")) out.push("gamma");
    return out;
  }, [cols, showGreeks]);

  // Build rows keyed by strike: { strike, call, put }
  const rows = useMemo(() => {
    const m = new Map();

    for (const o of filtered) {
      const strike = toNum(o.strike);
      if (strike === null) continue;

      const key = String(strike);
      if (!m.has(key)) m.set(key, { strike, call: null, put: null });

      const row = m.get(key);
      if (o.type === "C") row.call = o;
      if (o.type === "P") row.put = o;
    }

    return Array.from(m.values());
  }, [filtered]);

  // ATM strike
  const atmStrike = useMemo(() => {
    if (spot === null || rows.length === 0) return null;
    let best = rows[0].strike;
    let bestDist = Math.abs(best - spot);

    for (const r of rows) {
      const d = Math.abs(r.strike - spot);
      if (d < bestDist) {
        bestDist = d;
        best = r.strike;
      }
    }
    return best;
  }, [rows, spot]);

  // ITM/OTM helpers (per side)
  function isCallITM(strike) {
    if (spot === null) return false;
    return strike < spot;
  }
  function isPutITM(strike) {
    if (spot === null) return false;
    return strike > spot;
  }

  // Volume spike threshold (P90) per side
  const spike = useMemo(() => {
    const volsC = [];
    const volsP = [];
    for (const r of rows) {
      const vc = toNum(r.call?.volume);
      const vp = toNum(r.put?.volume);
      if (vc != null) volsC.push(vc);
      if (vp != null) volsP.push(vp);
    }
    const p90 = (arr) => {
      if (!arr.length) return null;
      const s = [...arr].sort((a, b) => a - b);
      const idx = Math.floor(0.9 * (s.length - 1));
      return s[idx];
    };
    return { callP90: p90(volsC), putP90: p90(volsP) };
  }, [rows]);

  function toggleSort(key) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  }

  function renderCell(opt, key) {
    if (!opt) return "";

    if (key === "impliedVolatility") return pct(opt[key], 2);

    if (["delta", "gamma", "theta", "vega", "rho"].includes(key)) {
      return fmt(opt[key], 3);
    }

    if (["bid", "ask", "lastPrice"].includes(key)) return fmt(opt[key], 2);

    if (["openInterest", "volume"].includes(key)) {
      const n = toNum(opt[key]);
      return n === null ? "" : n.toLocaleString();
    }

    return String(opt[key] ?? "");
  }

  // Apply strike band filter + sorting
  const sortedRows = useMemo(() => {
    let arr = [...rows];

    // strike band filter (±%)
    if (strikeBandPct > 0 && spot != null) {
      const lo = spot * (1 - strikeBandPct);
      const hi = spot * (1 + strikeBandPct);
      arr = arr.filter((r) => r.strike >= lo && r.strike <= hi);
    }

    arr.sort((a, b) => {
      let av, bv;

      if (sortKey === "strike") {
        av = a.strike;
        bv = b.strike;
      } else {
        // sort using whichever side has data first
        const aSide = a.call?.[sortKey] ?? a.put?.[sortKey];
        const bSide = b.call?.[sortKey] ?? b.put?.[sortKey];
        av = toNum(aSide);
        bv = toNum(bSide);

        // If both aren't numeric, fallback to string
        if (av === null && bv === null) {
          const sa = String(aSide ?? "");
          const sb = String(bSide ?? "");
          const cmpS = sa.localeCompare(sb);
          return sortDir === "asc" ? cmpS : -cmpS;
        }

        // Put nulls at the bottom
        if (av === null) return 1;
        if (bv === null) return -1;
      }

      const cmp = av < bv ? -1 : av > bv ? 1 : 0;
      return sortDir === "asc" ? cmp : -cmp;
    });

    return arr;
  }, [rows, sortKey, sortDir, strikeBandPct, spot]);

  return (
    <div className="opt-chain">
      {/* Top bar like E*TRADE */}
      <div className="opt-chain-top">
        <div className="opt-chain-left">
          <div className="opt-title">{symbol} Options</div>
          <div className="opt-sub">
            Underlying: <b>{spot != null ? fmt(spot, 2) : "—"}</b>
          </div>
        </div>

        <div className="opt-chain-right">
          <label className="miniLabel">
            Strike band:
            <select
              className="select"
              value={strikeBandPct}
              onChange={(e) => setStrikeBandPct(Number(e.target.value))}
            >
              <option value={0}>All</option>
              <option value={0.1}>±10%</option>
              <option value={0.2}>±20%</option>
              <option value={0.3}>±30%</option>
            </select>
          </label>

          <button className="btn" onClick={() => setShowGreeks((s) => !s)}>
            Greeks: {showGreeks ? "On" : "Off"}
          </button>

          <button className="btn" onClick={() => setShowColPicker((s) => !s)}>
            Column Selection
          </button>
        </div>
      </div>

      {/* Expiration tabs */}
      <div className="opt-exp-tabs">
        {expList.map((exp) => (
          <button
            key={exp}
            className={cls("tab", exp === expiration && "active")}
            onClick={() => setExpiration(exp)}
            title={exp}
          >
            {exp}
          </button>
        ))}
      </div>

      {/* Column picker */}
      {showColPicker && (
        <div className="opt-colpicker">
          <div className="opt-colpicker-title">Columns</div>

          <div className="opt-colpicker-grid">
            {allCols.map((c) => (
              <label key={c} className="chk">
                <input
                  type="checkbox"
                  checked={cols.includes(c)}
                  onChange={(e) => {
                    if (e.target.checked) setCols((prev) => [...prev, c]);
                    else setCols((prev) => prev.filter((x) => x !== c));
                  }}
                />
                {COL_LABEL[c]}
              </label>
            ))}
          </div>

          <div className="opt-colpicker-actions">
            <button className="btn" onClick={() => setCols(DEFAULT_COLS)}>
              Reset
            </button>
            <button className="btn" onClick={() => setShowColPicker(false)}>
              Close
            </button>
          </div>
        </div>
      )}

      {/* The chain table: Calls | Strike | Puts */}
      <div className="opt-table-wrap">
        <table className="opt-table">
          <thead>
            <tr>
              <th className="side-head" colSpan={displayedCols.length}>
                Calls
              </th>

              <th className="strike-head" onClick={() => toggleSort("strike")}>
                Strike{" "}
                {sortKey === "strike" ? (sortDir === "asc" ? "▲" : "▼") : ""}
              </th>

              <th className="side-head" colSpan={displayedCols.length}>
                Puts
              </th>
            </tr>

            <tr>
              {displayedCols.map((c) => (
                <th
                  key={"c-" + c}
                  onClick={() => toggleSort(c)}
                  className="colhead"
                >
                  {COL_LABEL[c]}{" "}
                  {sortKey === c ? (sortDir === "asc" ? "▲" : "▼") : ""}
                </th>
              ))}

              <th className="colhead strike-col"> </th>

              {displayedCols.map((c) => (
                <th
                  key={"p-" + c}
                  onClick={() => toggleSort(c)}
                  className="colhead"
                >
                  {COL_LABEL[c]}{" "}
                  {sortKey === c ? (sortDir === "asc" ? "▲" : "▼") : ""}
                </th>
              ))}
            </tr>
          </thead>

          <tbody>
            {sortedRows.map((r) => {
              const isATM = atmStrike !== null && r.strike === atmStrike;

              return (
                <tr key={r.strike} className={cls(isATM && "atm")}>
                  {/* Calls */}
                  {displayedCols.map((c) => {
                    const vol = toNum(r.call?.volume);
                    const isSpike =
                      c === "volume" &&
                      spike.callP90 != null &&
                      vol != null &&
                      vol >= spike.callP90;

                    return (
                      <td
                        key={"c-" + r.strike + "-" + c}
                        className={cls(
                          "cell call",
                          isCallITM(r.strike) ? "itm" : "otm",
                          c === "bid" ? "bid" : "",
                          c === "ask" ? "ask" : "",
                          isSpike ? "volSpike" : ""
                        )}
                      >
                        {renderCell(r.call, c)}
                      </td>
                    );
                  })}

                  {/* Strike */}
                  <td className="cell strike">{fmt(r.strike, 2)}</td>

                  {/* Puts */}
                  {displayedCols.map((c) => {
                    const vol = toNum(r.put?.volume);
                    const isSpike =
                      c === "volume" &&
                      spike.putP90 != null &&
                      vol != null &&
                      vol >= spike.putP90;

                    return (
                      <td
                        key={"p-" + r.strike + "-" + c}
                        className={cls(
                          "cell put",
                          isPutITM(r.strike) ? "itm" : "otm",
                          c === "bid" ? "bid" : "",
                          c === "ask" ? "ask" : "",
                          isSpike ? "volSpike" : ""
                        )}
                      >
                        {renderCell(r.put, c)}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Minimal styling (can move to App.css) */}
      <style>{`
        .opt-chain { margin-top: 10px; }
        .opt-chain-top { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; }
        .opt-title { font-weight: 700; font-size: 1.0rem; }
        .opt-sub { font-size: 0.85rem; opacity: 0.9; }

        .opt-chain-right { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
        .miniLabel { display:inline-flex; gap:6px; align-items:center; font-size:12px; opacity:0.9; }
        .select {
          background: #0b1220;
          color: #e5e7eb;
          border: 1px solid rgba(148,163,184,0.25);
          border-radius: 8px;
          padding: 4px 8px;
        }

        .btn { padding: 6px 10px; border-radius: 8px; border: 1px solid #334155; background: transparent; color: #e5e7eb; cursor:pointer; }
        .btn:hover { border-color:#64748b; }

        .opt-exp-tabs { display:flex; gap:8px; flex-wrap:wrap; margin: 10px 0; }
        .tab { padding: 6px 10px; border-radius: 999px; border: 1px solid #334155; background: transparent; color:#e5e7eb; cursor:pointer; font-size: 0.85rem; }
        .tab.active { background: #2563eb; border-color:#2563eb; }

        .opt-colpicker { border: 1px solid #334155; border-radius: 12px; padding: 10px; margin: 10px 0; background: rgba(2,6,23,0.6); }
        .opt-colpicker-title { font-weight:700; margin-bottom:8px; }
        .opt-colpicker-grid { display:grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap:6px; }
        .chk { display:flex; gap:8px; align-items:center; font-size: 0.85rem; }
        .opt-colpicker-actions { margin-top: 10px; display:flex; gap:8px; justify-content:flex-end; }

        .opt-table-wrap { width: 100%; max-height: 600px; overflow-y: auto; border:1px solid #334155; border-radius: 12px; }
        .opt-table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 0.85rem; }
        .opt-table th, .opt-table td { padding: 8px 10px; border-bottom: 1px solid rgba(51,65,85,0.6); white-space: nowrap; box-sizing: border-box; }
        .opt-table thead tr:nth-child(1) th {
          position: sticky;
          top: 0;
          height: 40px;
          background-color: #020617; /* Solid hex color for opacity */
          z-index: 50;
          border-bottom: 1px solid #334155;
        }
        .opt-table thead tr:nth-child(2) th {
          position: sticky;
          top: 40px;
          height: 36px;
          background-color: #020617;
          z-index: 40;
          border-bottom: 1px solid #334155;
        }


        .side-head { text-align:center; font-weight: 800; border-bottom: 1px solid #334155; }
        .strike-head { text-align:center; font-weight: 800; cursor:pointer; border-bottom: 1px solid #334155; }
        .colhead { cursor:pointer; opacity:0.95; user-select:none; }
        .strike-col { width: 90px; }

        .cell { text-align:right; }
        .cell.strike { text-align:center; font-weight:700; }
        tr.atm { background: rgba(37, 99, 235, 0.12); }

        /* ITM/OTM shading */
        td.itm { background: rgba(34, 197, 94, 0.08); }
        td.otm { background: rgba(239, 68, 68, 0.06); }

        /* Bid/Ask emphasis */
        td.bid { color: #22c55e; font-weight: 600; }
        td.ask { color: #ef4444; font-weight: 600; }

        /* Volume spikes */
        td.volSpike {
          outline: 1px solid rgba(250, 204, 21, 0.55);
          box-shadow: inset 0 0 0 9999px rgba(250, 204, 21, 0.08);
          font-weight: 700;
        }
      `}</style>
    </div>
  );
}
