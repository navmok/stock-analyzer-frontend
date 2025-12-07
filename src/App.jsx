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
} from "recharts";

const API_BASE = "http://127.0.0.1:8000"; // FastAPI backend

const SYMBOLS = ["AAPL", "MSFT", "GOOGL"];

function App() {
  const [symbol, setSymbol] = useState("GOOGL");
  const [days, setDays] = useState(1);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadData(sym = symbol, d = days) {
    setLoading(true);
    setError("");
    try {
      const url = `${API_BASE}/api/prices?symbol=${encodeURIComponent(
        sym
      )}&days=${d}`;
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json = await res.json();

      // enrich with formatted time label for the chart
      const enriched = json.map((row) => {
        const date = new Date(row.ts_utc);
        
        // For intraday (days <= 5), show date + time
        // For daily data (days > 5), show just date
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

      setData(enriched);
    } catch (e) {
      console.error(e);
      setError("Failed to load data");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const latest = data.length ? data[data.length - 1] : null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>Stock Dashboard (MVP)</h1>
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
          Refresh
        </button>

        <div className="status">
          {loading && <span>Loading…</span>}
          {error && <span className="error">{error}</span>}
        </div>
      </section>

      <main>
        {latest && (
          <div className="latest">
            <h2>Latest</h2>
            <p>
              <strong>{latest.symbol}</strong> –{" "}
              {new Date(latest.ts_utc).toLocaleString()} – Close:{" "}
              <strong>{latest.close}</strong>
            </p>
          </div>
        )}

        {/* ==== PRICE CHART ==== */}
        <div className="chart-wrapper">
          <h2>Price (close)</h2>
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
                  width={60}
                  domain={["auto", "auto"]}
                />
                <Tooltip />
                <Line
                  type="monotone"
                  dataKey="close"
                  stroke="#60a5fa"
                  dot={false}
                  strokeWidth={2}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* ==== TABLE ==== */}
        <div className="table-wrapper">
          <h2>Data ({data.length} rows)</h2>
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
                  <td>{row.open}</td>
                  <td>{row.high}</td>
                  <td>{row.low}</td>
                  <td>{row.close}</td>
                  <td>{row.volume}</td>
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
      </main>
    </div>
  );
}

export default App;