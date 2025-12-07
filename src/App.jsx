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

const API_BASE =
  process.env.NODE_ENV === "production"
    ? "https://stock-analyzer-frontend-cxem.vercel.app"
    : "";

const SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"];

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
          boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
        }}
      >
        <p style={{ margin: "2px 0", fontWeight: "bold" }}>{data.timeLabel}</p>
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
          Close: <strong>${data.close.toFixed(2)}</strong>
        </p>
        <p style={{ margin: "2px 0" }}>
          Volume: <strong>{data.volume.toLocaleString()}</strong>
        </p>
      </div>
    );
  }
  return null;
};

function App() {
  const [symbol, setSymbol] = useState("GOOGL");
  const [days, setDays] = useState(1);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

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

      // Check if data is empty
      if (!json || json.length === 0) {
        setError("No data available for this symbol/period");
        setData([]);
        return;
      }

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
      setError("Failed to load data: " + e.message);
    } finally {
      setLoading(false);
    }
  }

  // ---- LOAD OPTIONS DATA (OUR NEW ROUTE) ----
  async function loadOptions(sym = symbol) {
    setOptionsLoading(true);
    setOptionsError("");

    try {
      const url = `${API_BASE}/api/options?symbol=${encodeURIComponent(sym)}`;
      console.log("Fetching options:", url); // DEBUG

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const json = await res.json();
      console.log("Options response:", json); // DEBUG

      // Backend already returns the right shape
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
                  width={70}
                  domain={["auto", "auto"]}
                  tickFormatter={(value) => `$${value.toFixed(0)}`}
                />
                <Tooltip content={<CustomTooltip />} />
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

export default App;
This does all of these:

Keeps your full UI (chart + price table + options table + tooltip + days selector).

Uses the clean loadOptions that hits /api/options with API_BASE.

Calls both loadData() and loadOptions() on page load.

Reloads options when you change the symbol.

After you paste this, redeploy, then in DevTools → Console you should see:

Fetching prices: https://stock-analyzer-frontend-cxem.vercel.app/api/prices?...

Fetching options: https://stock-analyzer-frontend-cxem.vercel.app/api/options?symbol=GOOGL

…and your options table should fill with the mock rows you saw in the /api/options URL.







You said:
mock data works. let us try yahoo finance once. can you give me updated options.jsexport default async function handler(req, res) {
  const symbol = req.query?.symbol || "GOOGL";
  const baseStrike = 300;
  const options = [];
  
  for (let i = -5; i <= 5; i++) {
    const strike = baseStrike + i * 5;
    const absI = Math.abs(i);
    
    options.push({
      contractSymbol: symbol + "C" + strike,
      type: "C",
      strike: strike,
      lastPrice: 10 - absI,
      bid: 9 - absI,
      ask: 11 - absI,
      volume: 1000 - absI * 50,
      openInterest: 5000 - absI * 200,
      expiration: "2026-01-16T00:00:00Z",
      inTheMoney: i < 0
    });
    
    options.push({
      contractSymbol: symbol + "P" + strike,
      type: "P",
      strike: strike,
      lastPrice: 10 - absI,
      bid: 9 - absI,
      ask: 11 - absI,
      volume: 900 - absI * 50,
      openInterest: 4500 - absI * 200,
      expiration: "2026-01-16T00:00:00Z",
      inTheMoney: i > 0
    });
  }
  
  res.status(200).json(options);
}


ChatGPT said:

Thought for 38s
Yep, now that the mock path works we can safely try Yahoo Finance again from the backend (so no CORS drama).

Here’s an updated pages/api/options.js (or src/app/api/options/route.js translated if you’re on the App Router) that:

Calls Yahoo Finance options endpoint on the server

Normalizes the data into the same shape your frontend already expects

Has a mock fallback if Yahoo fails (so the UI still works)

js
Copy code
// pages/api/options.js (Next.js API route)

function buildMockOptions(symbol) {
  const baseStrike = 300;
  const options = [];

  for (let i = -5; i <= 5; i++) {
    const strike = baseStrike + i * 5;
    const absI = Math.abs(i);

    options.push({
      contractSymbol: symbol + "C" + strike,
      type: "C",
      strike: strike,
      lastPrice: 10 - absI,
      bid: 9 - absI,
      ask: 11 - absI,
      volume: 1000 - absI * 50,
      openInterest: 5000 - absI * 200,
      expiration: "2026-01-16T00:00:00Z",
      inTheMoney: i < 0,
    });

    options.push({
      contractSymbol: symbol + "P" + strike,
      type: "P",
      strike: strike,
      lastPrice: 10 - absI,
      bid: 9 - absI,
      ask: 11 - absI,
      volume: 900 - absI * 50,
      openInterest: 4500 - absI * 200,
      expiration: "2026-01-16T00:00:00Z",
      inTheMoney: i > 0,
    });
  }

  return options;
}

export default async function handler(req, res) {
  const symbol = req.query?.symbol || "GOOGL";
  const useMock = req.query?.mock === "1"; // optional: ?mock=1 to force mock

  // If you want to force mock explicitly
  if (useMock) {
    const mockOptions = buildMockOptions(symbol);
    return res.status(200).json(mockOptions);
  }

  try {
    const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}`;

    // Add a User-Agent just to be safe; some hosts are picky
    const yfRes = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
      },
    });

    if (!yfRes.ok) {
      throw new Error(`Yahoo Finance HTTP ${yfRes.status}`);
    }

    const json = await yfRes.json();
    const chain = json?.optionChain?.result?.[0];
    const opt = chain?.options?.[0];

    if (!opt) {
      // No options data – return empty array (or mock if you prefer)
      return res.status(200).json([]);
    }

    const calls = opt.calls || [];
    const puts = opt.puts || [];

    const mapOption = (o, type) => ({
      contractSymbol: o.contractSymbol,
      type, // "C" or "P"
      strike: o.strike,
      lastPrice: o.lastPrice,
      bid: o.bid,
      ask: o.ask,
      volume: o.volume,
      openInterest: o.openInterest,
      expiration: o.expiration
        ? new Date(o.expiration * 1000).toISOString()
        : null,
      inTheMoney: o.inTheMoney,
    });

    const options = [
      ...calls.map((o) => mapOption(o, "C")),
      ...puts.map((o) => mapOption(o, "P")),
    ];

    return res.status(200).json(options);
  } catch (err) {
    console.error("Error fetching Yahoo Finance options:", err);

    // Fallback: return mock options so UI still works
    const fallback = buildMockOptions(symbol);
    return res.status(200).json(fallback);
    // If you’d rather fail hard instead, use:
    // return res.status(500).json({ error: "Failed to fetch options" });
  }
}
How to test
Deploy this options.js to Vercel.

Hit this in the browser:

https://stock-analyzer-frontend-cxem.vercel.app/api/options?symbol=GOOGL

If Yahoo works, you’ll see real chain data (strikes around current price).

If it fails for any reason, it will silently fall back to your mock data, but Vercel logs will show the Yahoo error.

If you’d prefer no fallback (only real Yahoo data), I can give you a version that returns 500 instead of mock.







You said:

it all works great. can we add moving average charts 1 month 3 month and 12 month

ChatGPT said:

Thought for 37s
Nice, this looks so good already 😄
Yes, we can absolutely add 1-month, 3-month, and 12-month moving averages.

I’ll assume trading days:

1 month ≈ 21 days

3 months ≈ 63 days

12 months ≈ 252 days

We’ll:

Compute moving averages on the frontend from close.

Add 3 extra lines to the existing Recharts chart.

Show them in the tooltip.

Below is a drop-in replacement App.jsx based on the code you just confirmed works, with moving averages added.

✅ Updated App.jsx with 1M / 3M / 12M moving averages
jsx
Copy code
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

const SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"];

// ---- helper to compute moving averages on 'close' ----
function addMovingAverages(rows) {
  const ONE_M = 21;   // ~1 month of trading days
  const THREE_M = 63; // ~3 months
  const TWELVE_M = 252; // ~12 months

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
          boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
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

function App() {
  const [symbol, setSymbol] = useState("GOOGL");
  const [days, setDays] = useState(1);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

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

      // ➜ add moving averages here
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
                {/* Close price */}
                <Line
                  type="monotone"
                  dataKey="close"
                  name="Close"
                  stroke="#60a5fa"
                  dot={false}
                  strokeWidth={2}
                />
                {/* Moving averages */}
                <Line
                  type="monotone"
                  dataKey="ma1M"
                  name="1M MA"
                  stroke="#22c55e"
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="ma3M"
                  name="3M MA"
                  stroke="#facc15"
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="ma12M"
                  name="12M MA"
                  stroke="#f97316"
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
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

export default App;
Once you redeploy with this:

For large Days values (e.g., 252–730), you’ll see all 3 MA lines.

For smaller Days, shorter windows will show first (e.g., with 30 days you’ll only see 1M MA).

If you want separate tabs or toggles to show/hide MAs, we can add checkboxes next.







You said:
assume 1 month ≈ 30 days

3 months ≈ 90 days

12 months ≈ 365 days and we need data for symbols ["RIOT", "COST", "META", "WMT", "TGT", "AAPL", "GOOG", "NFLX", "CMG", "LULU", "MRVL", "ELF", "FISV", "UBER", "LYFT", "BBAI", "SOFI", "HOOD", "IOQ"];


ChatGPT said:

Thought for 23s
Perfect, that’s an easy tweak. We only need to change two things in your current App.jsx:

Update the SYMBOLS list to your new set

Change the moving–average windows to 30 / 90 / 365 days

Everything else can stay exactly as it is.

1️⃣ Update the symbols list
Replace your current SYMBOLS constant with this:

js
Copy code
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
2️⃣ Update the moving-average windows
In the addMovingAverages helper I gave you, change the window sizes.

Find:

js
Copy code
function addMovingAverages(rows) {
  const ONE_M = 21;   // ~1 month of trading days
  const THREE_M = 63; // ~3 months
  const TWELVE_M = 252; // ~12 months of trading days
  ...
}
Replace it with:

js
Copy code
function addMovingAverages(rows) {
  // Calendar-day style windows
  const ONE_M = 30;   // 1 month ≈ 30 days
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
Note on 12-month MA
For the 12-month (365-day) MA to show up:

Your Days input needs to be ≥ 365 (otherwise early points will show – for 12M MA, which is expected).

After these two edits, redeploy and you’ll have:

New symbol dropdown with your full list

1M / 3M / 12M moving averages using 30 / 90 / 365-day windows on the same chart.







You said:
can you update this and share the updated script


ChatGPT said:
Here you go — full updated App.jsx with:

Symbols = ["RIOT","COST","META","WMT","TGT","AAPL","GOOG","NFLX","CMG","LULU","MRVL","ELF","FISV","UBER","LYFT","BBAI","SOFI","HOOD","IOQ"]

Moving averages using 30 / 90 / 365 days (1M / 3M / 12M)

jsx
Copy code
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

function App() {
  const [symbol, setSymbol] = useState("GOOG");
  const [days, setDays] = useState(365);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

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
                <Line
                  type="monotone"
                  dataKey="ma1M"
                  name="1M MA"
                  stroke="#22c55e"
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="ma3M"
                  name="3M MA"
                  stroke="#facc15"
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="ma12M"
                  name="12M MA"
                  stroke="#f97316"
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
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

export default App;