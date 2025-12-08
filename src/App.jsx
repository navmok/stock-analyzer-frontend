import { useEffect, useState, useMemo } from "react";
import "./App.css";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

const API_BASE =
  process.env.NODE_ENV === "production"
    ? "https://stock-analyzer-frontend-cxem.vercel.app"
    : "";

// Master list + default active + max
const ALL_SYMBOLS = [
  "RIOT",
  "COST",
  "META",
  "WMT",
  "TGT",
  "AAPL",
  "GOOG",
  "NFLX",
  "CMG",
  "LULU",
  "MRVL",
  "ELF",
  "FISV",
  "UBER",
  "LYFT",
  "BBAI",
  "SOFI",
  "HOOD",
  "IOQ",
];

const DEFAULT_ACTIVE = ALL_SYMBOLS.slice(0, 3); // default 3 stocks in view
const MAX_ACTIVE = 5;

// ---- helper to compute moving averages on 'close' ----
function addMovingAverages(rows) {
  const WEEK = 7; // weekly MA
  const ONE_M = 30; // 1 month ≈ 30 days
  const THREE_M = 90; // 3 months ≈ 90 days
  const TWELVE_M = 365; // 12 months ≈ 365 days
  const EMA_PERIOD = 20; // 20-day EMA (common default)
  const alpha = 2 / (EMA_PERIOD + 1);

  let sumWeek = 0,
    sum1 = 0,
    sum3 = 0,
    sum12 = 0;

  let ema = null;

  return rows.map((row, i) => {
    const close = row.close;

    // running sums for simple MAs
    sumWeek += close;
    sum1 += close;
    sum3 += close;
    sum12 += close;

    if (i >= WEEK) sumWeek -= rows[i - WEEK].close;
    if (i >= ONE_M) sum1 -= rows[i - ONE_M].close;
    if (i >= THREE_M) sum3 -= rows[i - THREE_M].close;
    if (i >= TWELVE_M) sum12 -= rows[i - TWELVE_M].close;

    const maWeek = i >= WEEK - 1 ? sumWeek / WEEK : null;
    const ma1M = i >= ONE_M - 1 ? sum1 / ONE_M : null;
    const ma3M = i >= THREE_M - 1 ? sum3 / THREE_M : null;
    const ma12M = i >= TWELVE_M - 1 ? sum12 / TWELVE_M : null;

    // EMA (20-day)
    if (ema === null) {
      ema = close; // seed with first value
    } else {
      ema = alpha * close + (1 - alpha) * ema;
    }
    const emaVal = i >= EMA_PERIOD - 1 ? ema : null;

    return {
      ...row,
      maWeek,
      ma1M,
      ma3M,
      ma12M,
      ema: emaVal,
    };
  });
}

// Multi-stock tooltip: shows each line + primary MAs if present
const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload || !payload.length) return null;

  const primaryPayload = payload[0]?.payload || {};

  return (
    <div
      style={{
        background: "white",
        padding: "10px",
        border: "1px solid #ccc",
        borderRadius: "4px",
        boxShadow: "0 2px 4px rgba(0, 0, 0, 0.1)",
        color: "black",
      }}
    >
      <p style={{ margin: "2px 0", fontWeight: "bold" }}>{label}</p>

      {payload.map((entry) => (
        <p key={entry.dataKey} style={{ margin: "2px 0" }}>
          {entry.name}: <strong>${entry.value.toFixed(2)}</strong>
        </p>
      ))}

      {/* Only show MAs/EMA if they exist (primary symbol) */}
      {(primaryPayload.maWeek != null ||
        primaryPayload.ma1M != null ||
        primaryPayload.ma3M != null ||
        primaryPayload.ma12M != null ||
        primaryPayload.ema != null) && (
        <>
          <hr />
          {primaryPayload.maWeek != null && (
            <p style={{ margin: "2px 0" }}>
              Weekly MA (primary):{" "}
              <strong>${primaryPayload.maWeek.toFixed(2)}</strong>
            </p>
          )}
          {primaryPayload.ma1M != null && (
            <p style={{ margin: "2px 0" }}>
              Month MA (primary):{" "}
              <strong>${primaryPayload.ma1M.toFixed(2)}</strong>
            </p>
          )}
          {primaryPayload.ma3M != null && (
            <p style={{ margin: "2px 0" }}>
              Quarter MA (primary):{" "}
              <strong>${primaryPayload.ma3M.toFixed(2)}</strong>
            </p>
          )}
          {primaryPayload.ma12M != null && (
            <p style={{ margin: "2px 0" }}>
              Year MA (primary):{" "}
              <strong>${primaryPayload.ma12M.toFixed(2)}</strong>
            </p>
          )}
          {primaryPayload.ema != null && (
            <p style={{ margin: "2px 0" }}>
              EMA (20d, primary):{" "}
              <strong>${primaryPayload.ema.toFixed(2)}</strong>
            </p>
          )}
        </>
      )}
    </div>
  );
};

// Color palette for up to 5 stocks
const LINE_COLORS = ["#60a5fa", "#22c55e", "#f97316", "#a855f7", "#e11d48"];

export default function App() {
  // Active stocks and primary stock (for options + MAs)
  const [activeSymbols, setActiveSymbols] = useState(DEFAULT_ACTIVE);
  const [symbol, setSymbol] = useState(DEFAULT_ACTIVE[0]);

  // NEW: which active stocks are actually shown on chart + table
  const [visibleSymbols, setVisibleSymbols] = useState(DEFAULT_ACTIVE);

  const [days, setDays] = useState(365);

  // Multi-stock data: symbol -> enriched rows with MAs
  const [seriesBySymbol, setSeriesBySymbol] = useState({});

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // Options still tied to the primary symbol
  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

  // Moving average toggles (all OFF by default)
  const [show1M, setShow1M] = useState(false); // Month
  const [show3M, setShow3M] = useState(false); // Quarter
  const [show12M, setShow12M] = useState(false); // Year
  const [showWeek, setShowWeek] = useState(false); // Weekly
  const [showEma, setShowEma] = useState(false); // EMA

  // ---- keep visibleSymbols and primary symbol in sync with activeSymbols ----
  useEffect(() => {
    setVisibleSymbols((prev) => {
      // keep only those still active
      let next = prev.filter((s) => activeSymbols.includes(s));

      // if everything disappeared but we still have activeSymbols, show last one
      if (next.length === 0 && activeSymbols.length > 0) {
        const last = activeSymbols[activeSymbols.length - 1];
        setSymbol(last);
        return [last];
      }

      // ensure primary symbol is visible
      if (!next.includes(symbol) && next.length > 0) {
        const newPrimary = next[next.length - 1];
        setSymbol(newPrimary);
      }

      return next;
    });
  }, [activeSymbols]); // eslint-disable-line react-hooks/exhaustive-deps

  // ---- toggle whether a symbol is visible on chart + table ----
  function handleToggleVisibleSymbol(sym) {
    setVisibleSymbols((prev) => {
      const isVisible = prev.includes(sym);

      // if turning off and it's the last visible, do nothing (must have >=1)
      if (isVisible && prev.length === 1) return prev;

      let next;
      if (isVisible) {
        next = prev.filter((s) => s !== sym);
        // if we hid the primary, move primary to last remaining visible
        if (sym === symbol && next.length > 0) {
          const newPrimary = next[next.length - 1];
          setSymbol(newPrimary);
          loadOptions(newPrimary);
        }
      } else {
        next = [...prev, sym];
        // when we show a symbol, make it primary
        setSymbol(sym);
        loadOptions(sym);
      }

      return next;
    });
  }

  // ---- helper: add a new symbol (max 5) ----
  function handleAddSymbol(newSym) {
    if (!newSym) return;

    setActiveSymbols((prev) => {
      if (prev.includes(newSym)) return prev;

      let updated = [...prev, newSym];
      if (updated.length > MAX_ACTIVE) {
        // keep last MAX_ACTIVE
        updated = updated.slice(updated.length - MAX_ACTIVE);
      }
      return updated;
    });

    // make sure it becomes visible and primary
    setVisibleSymbols((prev) =>
      prev.includes(newSym) ? prev : [...prev, newSym]
    );
    setSymbol(newSym);
    loadOptions(newSym);
  }

  // ---- LOAD PRICE DATA FOR ALL ACTIVE SYMBOLS ----
  async function loadDataForSymbols(symbols = activeSymbols, d = days) {
    if (!symbols.length) return;

    setLoading(true);
    setError("");

    try {
      const fetches = symbols.map(async (sym) => {
        const url = `${API_BASE}/api/prices?symbol=${encodeURIComponent(
          sym
        )}&days=${d}`;
        console.log("Fetching prices:", url);

        const res = await fetch(url);
        if (!res.ok) {
          throw new Error(`HTTP ${res.status} for ${sym}`);
        }
        const json = await res.json();

        if (!json || json.length === 0) {
          return [sym, []];
        }

        const enriched = json.map((row) => {
          const date = new Date(row.ts_utc);
          let timeLabel;

          if (d <= 5) {
            timeLabel = date.toLocaleString("en-US", {
              month: "numeric",
              day: "numeric",
              hour: "2-digit",
              minute: "2-digit",
            });
          } else {
            timeLabel = date.toLocaleDateString("en-US", {
              month: "numeric",
              day: "numeric",
              year: "2-digit",
            });
          }

          return { ...row, timeLabel };
        });

        const withMA = addMovingAverages(enriched);
        return [sym, withMA];
      });

      const results = await Promise.all(fetches);

      const newSeries = {};
      for (const [sym, series] of results) {
        newSeries[sym] = series;
      }

      setSeriesBySymbol(newSeries);

      // Basic error if any symbol had no data
      const emptySymbols = results
        .filter(([, series]) => !series.length)
        .map(([s]) => s);
      if (emptySymbols.length === symbols.length) {
        setError("No data available for the selected symbols/period");
      }
    } catch (e) {
      console.error(e);
      setError("Failed to load data: " + e.message);
    } finally {
      setLoading(false);
    }
  }

  // ---- LOAD OPTIONS DATA (PRIMARY SYMBOL ONLY) ----
  async function loadOptions(sym = symbol) {
    if (!sym) return;

    setOptionsLoading(true);
    setOptionsError("");

    try {
      const url = `${API_BASE}/api/options?symbol=${encodeURIComponent(sym)}`;
      console.log("Fetching options:", url);

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const json = await res.json();
      console.log("Options response:", json);

      setOptions(json || []);
    } catch (err) {
      console.error("Options fetch error:", err);
      setOptionsError("Failed to load options: " + err.message);
    } finally {
      setOptionsLoading(false);
    }
  }

  // Initial load + when activeSymbols or days change
  useEffect(() => {
    loadDataForSymbols();
    loadOptions(symbol);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeSymbols, days]);

  // ----- Build combined chart data (1–5 stocks on same chart) -----
  const chartData = useMemo(() => {
    const map = new Map();

    visibleSymbols.forEach((sym) => {
      const series = seriesBySymbol[sym] || [];
      series.forEach((row) => {
        const key = row.ts_utc;
        let base = map.get(key);
        if (!base) {
          base = {
            ts_utc: row.ts_utc,
            timeLabel: row.timeLabel,
          };
          map.set(key, base);
        }

        // One close field per symbol
        base[`${sym}_close`] = row.close;

        // For the primary symbol, also carry over MAs/EMA
        if (sym === symbol) {
          base.maWeek = row.maWeek;
          base.ma1M = row.ma1M;
          base.ma3M = row.ma3M;
          base.ma12M = row.ma12M;
          base.ema = row.ema;
        }
      });
    });

    const combined = Array.from(map.values());
    combined.sort((a, b) => new Date(a.ts_utc) - new Date(b.ts_utc));
    return combined;
  }, [visibleSymbols, seriesBySymbol, symbol]);

  // ----- Build table data: flatten all visible series with symbol column -----
  const tableRows = useMemo(() => {
    const rows = [];
    visibleSymbols.forEach((sym) => {
      const series = seriesBySymbol[sym] || [];
      series.forEach((row) => {
        rows.push({ ...row, symbol: sym });
      });
    });

    rows.sort((a, b) => new Date(a.ts_utc) - new Date(b.ts_utc));
    return rows;
  }, [visibleSymbols, seriesBySymbol]);

  // Latest = last point of primary symbol
  const latestSeries = seriesBySymbol[symbol] || [];
  const latest = latestSeries.length
    ? latestSeries[latestSeries.length - 1]
    : null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>📈 Stock Dashboard (MVP)</h1>
      </header>

      <section className="controls">
        {/* Active symbols chips (1–5) */}
        <div className="control-group">
          <span className="control-label">Stocks in view (1–5)</span>
          <div className="symbol-chips">
            {activeSymbols.map((s) => {
              const isVisible = visibleSymbols.includes(s);
              return (
                <button
                  key={s}
                  type="button"
                  className={`symbol-chip ${isVisible ? "active" : ""}`}
                  onClick={() => handleToggleVisibleSymbol(s)}
                >
                  {s}
                </button>
              );
            })}
          </div>
        </div>

        {/* Add stock selector (adds, drops oldest if >5) */}
        <div className="control">
          <label>Add stock</label>
          <select
            value=""
            onChange={(e) => {
              handleAddSymbol(e.target.value);
              e.target.value = "";
            }}
          >
            <option value="">Choose…</option>
            {ALL_SYMBOLS.filter((s) => !activeSymbols.includes(s)).map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

        {/* Days + buttons */}
        <div className="control">
          <label>Days</label>
          <input
            type="number"
            min="1"
            max="730"
            value={days}
            onChange={(e) => {
              const d = Number(e.target.value || 1);
              setDays(d);
            }}
          />
        </div>

        <button onClick={() => loadDataForSymbols()} disabled={loading}>
          {loading ? "Loading..." : "Refresh"}
        </button>

        <button onClick={() => loadOptions()} disabled={optionsLoading}>
          {optionsLoading ? "Loading options..." : "Load Options"}
        </button>

        {error && (
          <div className="status">
            <span className="error">{error}</span>
          </div>
        )}
        {optionsError && (
          <div className="status">
            <span className="error">{optionsError}</span>
          </div>
        )}
      </section>

      {/* Moving average toggles (applied to primary symbol only) */}
      <section className="ma-toggles">
        <div className="ma-toggle-group">
          <label className="ma-toggle">
            <span>Weekly MA</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={showWeek}
                onChange={(e) => setShowWeek(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </label>

          <label className="ma-toggle">
            <span>Month MA</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={show1M}
                onChange={(e) => setShow1M(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </label>

          <label className="ma-toggle">
            <span>Quarter MA</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={show3M}
                onChange={(e) => setShow3M(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </label>

          <label className="ma-toggle">
            <span>Year MA</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={show12M}
                onChange={(e) => setShow12M(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </label>

          <label className="ma-toggle">
            <span>EMA (20d)</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={showEma}
                onChange={(e) => setShowEma(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </label>
        </div>
      </section>

      <main>
        {latest && (
          <div className="latest">
            <h2>Latest</h2>
            <p>
              <strong>{symbol}</strong> –{" "}
              {new Date(latest.ts_utc).toLocaleString()} – Close:{" "}
              <strong>${latest.close.toFixed(2)}</strong>
            </p>
          </div>
        )}

        {/* ==== MULTI-STOCK PRICE CHART ==== */}
        <div className="chart-wrapper">
          <h2>
            Price (close) –{" "}
            {visibleSymbols.length ? visibleSymbols.join(", ") : "No symbols"}
          </h2>

          <div className="chart-inner">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="timeLabel"
                  minTickGap={30}
                  tick={{ fontSize: 10 }}
                  angle={-45}
                  textAnchor="end"
                  height={60}
                />
                <YAxis
                  tick={{ fontSize: 10 }}
                  width={70}
                  domain={["auto", "auto"]}
                  tickFormatter={(value) => `$${value.toFixed(0)}`}
                />
                <Tooltip content={<CustomTooltip />} />
                <Legend />

                {/* One Close line per visible symbol */}
                {visibleSymbols.map((s, idx) => (
                  <Line
                    key={s}
                    type="monotone"
                    dataKey={`${s}_close`}
                    name={`${s} Close`}
                    stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                    dot={false}
                    strokeWidth={2}
                    connectNulls
                  />
                ))}

                {/* MAs/EMA for primary symbol only (using fields on combined data) */}
                {showWeek && (
                  <Line
                    type="monotone"
                    dataKey="maWeek"
                    name="Weekly MA (primary)"
                    stroke="#a855f7"
                    dot={false}
                    strokeWidth={1.5}
                    connectNulls
                  />
                )}
                {show1M && (
                  <Line
                    type="monotone"
                    dataKey="ma1M"
                    name="Month MA (primary)"
                    stroke="#22c55e"
                    dot={false}
                    strokeWidth={1.5}
                    connectNulls
                  />
                )}
                {show3M && (
                  <Line
                    type="monotone"
                    dataKey="ma3M"
                    name="Quarter MA (primary)"
                    stroke="#facc15"
                    dot={false}
                    strokeWidth={1.5}
                    connectNulls
                  />
                )}
                {show12M && (
                  <Line
                    type="monotone"
                    dataKey="ma12M"
                    name="Year MA (primary)"
                    stroke="#f97316"
                    dot={false}
                    strokeWidth={1.5}
                    connectNulls
                  />
                )}
                {showEma && (
                  <Line
                    type="monotone"
                    dataKey="ema"
                    name="EMA (20d, primary)"
                    stroke="#e11d48"
                    dot={false}
                    strokeWidth={1.5}
                    connectNulls
                  />
                )}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* ==== PRICE TABLE (VISIBLE SYMBOLS ONLY) ==== */}
        <div className="table-wrapper">
          <h2>Data ({tableRows.length} rows)</h2>
          <div style={{ maxHeight: "400px", overflowY: "auto" }}>
            <table>
              <thead>
                <tr>
                  <th>Time (UTC)</th>
                  <th>Symbol</th>
                  <th>Open</th>
                  <th>High</th>
                  <th>Low</th>
                  <th>Close</th>
                  <th>Volume</th>
                </tr>
              </thead>
              <tbody>
                {tableRows.map((row, idx) => (
                  <tr key={idx}>
                    <td>{new Date(row.ts_utc).toLocaleString()}</td>
                    <td>{row.symbol}</td>
                    <td>${row.open.toFixed(2)}</td>
                    <td>${row.high.toFixed(2)}</td>
                    <td>${row.low.toFixed(2)}</td>
                    <td>${row.close.toFixed(2)}</td>
                    <td>{row.volume.toLocaleString()}</td>
                  </tr>
                ))}
                {!tableRows.length && !loading && (
                  <tr>
                    <td colSpan="7" style={{ textAlign: "center" }}>
                      No data
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* ==== OPTIONS TABLE (PRIMARY SYMBOL) ==== */}
        <div className="table-wrapper">
          <h2>
            Options for {symbol} ({options.length} rows)
          </h2>
          {optionsLoading && <p>Loading options…</p>}
          {!optionsLoading && !options.length && <p>No options loaded.</p>}

          {options.length > 0 && (
            <div style={{ maxHeight: "400px", overflowY: "auto" }}>
              <table>
                <thead>
                  <tr>
                    <th>Contract</th>
                    <th>Type</th>
                    <th>Strike</th>
                    <th>Expiry</th>
                    <th>Last</th>
                    <th>Bid</th>
                    <th>Ask</th>
                    <th>Volume</th>
                    <th>Open Int</th>
                  </tr>
                </thead>
                <tbody>
                  {options.map((o, idx) => (
                    <tr key={idx}>
                      <td>{o.contractSymbol}</td>
                      <td>{o.type}</td>
                      <td>{o.strike}</td>
                      <td>
                        {o.expiration
                          ? new Date(o.expiration).toLocaleDateString()
                          : ""}
                      </td>
                      <td>{o.lastPrice}</td>
                      <td>{o.bid}</td>
                      <td>{o.ask}</td>
                      <td>{o.volume}</td>
                      <td>{o.openInterest}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
