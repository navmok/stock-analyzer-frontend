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

const SYMBOLS = [
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

// ---- helper to compute moving averages on 'close' ----
function addMovingAverages(rows) {
  // Calendar-style windows
  const ONE_M = 30; // 1 month ≈ 30 days
  const THREE_M = 90; // 3 months ≈ 90 days
  const TWELVE_M = 365; // 12 months ≈ 365 days

  let sum1 = 0,
    sum3 = 0,
    sum12 = 0;

  return rows.map((row, i) => {
    const close = row.close;

    sum1 += close;
    sum3 += close;
    sum12 += close;

    if (i >= ONE_M) sum1 -= rows[i - ONE_M].close;
    if (i >= THREE_M) sum3 -= rows[i - THREE_M].close;
    if (i >= TWELVE_M) sum12 -= rows[i - TWELVE_M].close;

    const ma1M = i >= ONE_M - 1 ? sum1 / ONE_M : null;
    const ma3M = i >= THREE_M - 1 ? sum3 / THREE_M : null;
    const ma12M = i >= TWELVE_M - 1 ? sum12 / TWELVE_M : null;

    return {
      ...row,
      ma1M,
      ma3M,
      ma12M,
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
          1M MA:{" "}
          <strong>
            {data.ma1M != null ? `$${data.ma1M.toFixed(2)}` : "–"}
          </strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          3M MA:{" "}
          <strong>
            {data.ma3M != null ? `$${data.ma3M.toFixed(2)}` : "–"}
          </strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          12M MA:{" "}
          <strong>
            {data.ma12M != null ? `$${data.ma12M.toFixed(2)}` : "–"}
          </strong>
        </p>
      </div>
    );
  }
  return null;
};

export default function App() {
  const [symbol, setSymbol] = useState("GOOG");
  const [days, setDays] = useState(365);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

  const [show1M, setShow1M] = useState(true);
  const [show3M, setShow3M] = useState(true);
  const [show12M, setShow12M] = useState(true);

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
        <div className="control">
          <label>Symbol</label>
          <select
            value={symbol}
            onChange={(e) => {
              const sym = e.target.value;
              setSymbol(sym);
              loadData(sym, days);
              loadOptions(sym);
            }}
          >
            {SYMBOLS.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

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
            <span>1M MA</span>
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
            <span>3M MA</span>
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
            <span>12M MA</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={show12M}
                onChange={(e) => setShow12M(e.target.checked)}
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
          <div style={{ width: "100%", height: 300 }}>
            <ResponsiveContainer>
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
                {show1M && (
                  <Line
                    type="monotone"
                    dataKey="ma1M"
                    name="1M MA"
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
                    name="3M MA"
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
                    name="12M MA"
                    stroke="#f97316"
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
