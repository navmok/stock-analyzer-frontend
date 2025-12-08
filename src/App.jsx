import { useEffect, useState } from "react";
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

// === NEW: master list + default active + max ===
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

// Custom tooltip for better chart info
const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
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
        <p style={{ margin: "2px 0", fontWeight: "bold" }}>{data.timeLabel}</p>
        <p style={{ margin: "2px 0" }}>
          Close: <strong>${data.close.toFixed(2)}</strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Open: <strong>${data.open.toFixed(2)}</strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          High: <strong>${data.high.toFixed(2)}</strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Low: <strong>${data.low.toFixed(2)}</strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Volume: <strong>{data.volume.toLocaleString()}</strong>
        </p>
        <hr />
        <p style={{ margin: "2px 0" }}>
          Weekly MA:{" "}
          <strong>
            {data.maWeek != null ? `$${data.maWeek.toFixed(2)}` : "–"}
          </strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Month MA:{" "}
          <strong>
            {data.ma1M != null ? `$${data.ma1M.toFixed(2)}` : "–"}
          </strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Quarter MA:{" "}
          <strong>
            {data.ma3M != null ? `$${data.ma3M.toFixed(2)}` : "–"}
          </strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Year MA:{" "}
          <strong>
            {data.ma12M != null ? `$${data.ma12M.toFixed(2)}` : "–"}
          </strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          EMA (20d):{" "}
          <strong>
            {data.ema != null ? `$${data.ema.toFixed(2)}` : "–"}
          </strong>
        </p>
      </div>
    );
  }
  return null;
};

export default function App() {
  // === CHANGED: symbol state split into activeSymbols + current symbol ===
  const [activeSymbols, setActiveSymbols] = useState(DEFAULT_ACTIVE);
  const [symbol, setSymbol] = useState(DEFAULT_ACTIVE[0]);

  const [days, setDays] = useState(365);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

  const [show1M, setShow1M] = useState(true); // Month
  const [show3M, setShow3M] = useState(true); // Quarter
  const [show12M, setShow12M] = useState(true); // Year

  // NEW: Weekly + EMA toggles
  const [showWeek, setShowWeek] = useState(true);
  const [showEma, setShowEma] = useState(false);

  // === NEW: helpers for selecting / adding symbols ===
  function handleSelectSymbol(sym) {
    setSymbol(sym);
    loadData(sym, days);
    loadOptions(sym);
  }

  function handleAddSymbol(newSym) {
    if (!newSym) return;

    setActiveSymbols((prev) => {
      if (prev.includes(newSym)) return prev;

      let updated = [...prev, newSym];
      if (updated.length > MAX_ACTIVE) {
        // drop oldest to keep last MAX_ACTIVE symbols
        updated = updated.slice(updated.length - MAX_ACTIVE);
      }
      return updated;
    });

    handleSelectSymbol(newSym);
  }

  // ---- LOAD PRICE DATA ----
  async function loadData(sym = symbol, d = days) {
    setLoading(true);
    setError("");

    try {
      const url = `${API_BASE}/api/prices?symbol=${encodeURIComponent(
        sym
      )}&days=${d}`;
      console.log("Fetching prices:", url);

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json = await res.json();

      if (!json || json.length === 0) {
        setError("No data available for this symbol/period");
        setData([]);
        return;
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

        return {
          ...row,
          timeLabel,
        };
      });

      const withMA = addMovingAverages(enriched);
      setData(withMA);
    } catch (e) {
      console.error(e);
      setError("Failed to load data: " + e.message);
    } finally {
      setLoading(false);
    }
  }

  // ---- LOAD OPTIONS DATA ----
  async function loadOptions(sym = symbol) {
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

  useEffect(() => {
    loadData();
    loadOptions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const latest = data.length ? data[data.length - 1] : null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>📈 Stock Dashboard (MVP)</h1>
      </header>

      <section className="controls">
        {/* === NEW: active symbols displayed as chips (max 5) === */}
        <div className="control-group">
          <span className="control-label">Stocks in view (max 5)</span>
          <div className="symbol-chips">
            {activeSymbols.map((s) => (
              <button
                key={s}
                type="button"
                className={`symbol-chip ${s === symbol ? "active" : ""}`}
                onClick={() => handleSelectSymbol(s)}
              >
                {s}
              </button>
            ))}
          </div>
        </div>

        {/* === NEW: Add stock selector (adds, drops oldest if >5) === */}
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

        {/* Days + buttons (unchanged) */}
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
              loadData(symbol, d);
            }}
          />
        </div>

        <button onClick={() => loadData()} disabled={loading}>
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

      {/* Moving average toggles */}
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
              <strong>{latest.symbol}</strong> –{" "}
              {new Date(latest.ts_utc).toLocaleString()} – Close:{" "}
              <strong>${latest.close.toFixed(2)}</strong>
            </p>
          </div>
        )}

        {/* ==== PRICE CHART ==== */}
        <div className="chart-wrapper">
          <h2>Price (close) + Moving Averages</h2>

																	 
          <div className="chart-inner">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data}>
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
                <Line
                  type="monotone"
                  dataKey="close"
                  name="Close"
                  stroke="#60a5fa"
                  dot={false}
                  strokeWidth={2}
                />
                {showWeek && (
                  <Line
                    type="monotone"
                    dataKey="maWeek"
                    name="Weekly MA"
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
                    name="Month MA"
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
                    name="Quarter MA"
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
                    name="Year MA"
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
                    name="EMA (20d)"
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

        {/* ==== PRICE TABLE ==== */}
        <div className="table-wrapper">
          <h2>Data ({data.length} rows)</h2>
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
                {data.map((row, idx) => (
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
                {!data.length && !loading && (
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

        {/* ==== OPTIONS TABLE ==== */}
        <div className="table-wrapper">
          <h2>Options ({options.length} rows)</h2>
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
