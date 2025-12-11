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
import Chart from "react-apexcharts";  // ⬅️ NEW

const API_BASE =
  process.env.NODE_ENV === "production"
    ? "https://stock-analyzer-frontend-cxem.vercel.app"
    : "";

// master list + default active + max
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
  // Use trading-day style windows
  const WEEK = 5;      // ~1 trading week
  const ONE_M = 21;    // ~1 trading month
  const THREE_M = 63;  // ~3 trading months
  const TWELVE_M = 252; // ~1 trading year (max window)

  const EMA_PERIOD = 20;
  const alpha = 2 / (EMA_PERIOD + 1);

  // 20-day rolling standard deviation (volatility)
  const STD_PERIOD = 20;
  const stdWindow = [];

  let sumWeek = 0,
    sum1 = 0,
    sum3 = 0,
    sum12 = 0;

  let ema = null;

  function computeStdDev(arr) {
  if (!arr.length) return null;
  const mean = arr.reduce((acc, v) => acc + v, 0) / arr.length;
  const variance =
    arr.reduce((acc, v) => acc + (v - mean) * (v - mean), 0) /
    arr.length;
    return Math.sqrt(variance);
 }

  return rows.map((row, i) => {
    const close = row.close;

    // update std-dev window
    stdWindow.push(close);
    if (stdWindow.length > STD_PERIOD) stdWindow.shift();

    // accumulate
    sumWeek += close;
    sum1 += close;
    sum3 += close;
    sum12 += close;

    if (i >= WEEK) sumWeek -= rows[i - WEEK].close;
    if (i >= ONE_M) sum1 -= rows[i - ONE_M].close;
    if (i >= THREE_M) sum3 -= rows[i - THREE_M].close;
    if (i >= TWELVE_M) sum12 -= rows[i - TWELVE_M].close;

    // window lengths actually available at this index
    const lenWeek = Math.min(i + 1, WEEK);
    const len1M = Math.min(i + 1, ONE_M);
    const len3M = Math.min(i + 1, THREE_M);
    const len12M = Math.min(i + 1, TWELVE_M); // up to 252 days, or fewer if that's all we have

    const maWeek = lenWeek >= WEEK ? sumWeek / lenWeek : null;
    const ma1M = len1M >= ONE_M ? sum1 / len1M : null;
    const ma3M = len3M >= THREE_M ? sum3 / len3M : null;

    // 🔹 Year MA: start showing once we have a decent window (e.g., 60+ days)
    const ma12M = len12M >= 60 ? sum12 / len12M : null;

    // 🔹 20-day rolling std dev
    const std20 =
      stdWindow.length === STD_PERIOD
       ? computeStdDev(stdWindow)
       : null;

    // EMA
    if (ema === null) {
      ema = close;
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
      std20,
    };
  });
}

// Multi-stock tooltip: all lines + MAs for ALL visible symbols
const CustomTooltip = ({
  active,
  payload,
  label,
  activeSymbols,
  showWeek,
  show1M,
  show3M,
  show12M,
  showEma,
}) => {
  if (!active || !payload || !payload.length) return null;

  // full data row for this x position
  const row = payload[0]?.payload || {};

  // 🔹 NEW: only keep Close series at the top
  const closeEntries = payload.filter(
    (entry) =>
      typeof entry.dataKey === "string" &&
      entry.dataKey.endsWith("_close") // e.g. "RIOT_close"
  );

  return (
    <div
      style={{
        background: "white",
        padding: "12px",
        border: "1px solid #ccc",
        borderRadius: "6px",
        boxShadow: "0 2px 4px rgba(0, 0, 0, 0.1)",
        color: "black",
        maxWidth: "260px",
      }}
    >
      {/* Date label */}
      <p style={{ margin: "4px 0", fontWeight: "bold" }}>{label}</p>

      {/* 🔹 TOP: CLOSE PRICES ONLY */}
      {closeEntries.map((entry) => (
        <p key={entry.dataKey} style={{ margin: "2px 0" }}>
          {entry.name}: <strong>${entry.value.toFixed(2)}</strong>
        </p>
      ))}

      <hr style={{ margin: "8px 0" }} />

      {/* BOTTOM: CLEAN MOVING AVERAGE SUMMARY (unchanged) */}
      <p style={{ margin: "4px 0", fontWeight: "bold" }}>Moving averages</p>

      {activeSymbols.map((sym) => {
        const parts = [];
        const wk = row[`${sym}_maWeek`];
        const m1 = row[`${sym}_ma1M`];
        const m3 = row[`${sym}_ma3M`];
        const y1 = row[`${sym}_ma12M`];
        const ema = row[`${sym}_ema`];
        const std20 = row[`${sym}_std20`];

        if (showWeek && wk != null) parts.push(`Wk: ${wk.toFixed(2)}`);
        if (show1M && m1 != null) parts.push(`1M: ${m1.toFixed(2)}`);
        if (show3M && m3 != null) parts.push(`3M: ${m3.toFixed(2)}`);
        if (show12M && y1 != null) parts.push(`12M: ${y1.toFixed(2)}`);
        if (showEma && ema != null) parts.push(`EMA: ${ema.toFixed(2)}`);
        if (std20 != null) parts.push(`σ20: ${std20.toFixed(2)}`);

        if (!parts.length) return null;

        return (
          <p key={sym} style={{ margin: "2px 0" }}>
            {sym}: <strong>{parts.join(" · ")}</strong>
          </p>
        );
      })}
    </div>
  );
};

// colors for up to 5 stocks
const LINE_COLORS = ["#60a5fa", "#22c55e", "#f97316", "#a855f7", "#e11d48"];

export default function App() {
  // 1–5 stocks in view
  const [activeSymbols, setActiveSymbols] = useState(DEFAULT_ACTIVE);
  // primary symbol for "Latest" + MAs
  const [symbol, setSymbol] = useState(DEFAULT_ACTIVE[0]);

  const [days, setDays] = useState(365);

  // price data: symbol -> [rows with MAs]
  const [seriesBySymbol, setSeriesBySymbol] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // options data: symbol -> [option rows]
  const [optionsBySymbol, setOptionsBySymbol] = useState({});
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

  // MA toggles (all off by default)
  const [show1M, setShow1M] = useState(false); // Month
  const [show3M, setShow3M] = useState(false); // Quarter
  const [show12M, setShow12M] = useState(false); // Year
  const [showWeek, setShowWeek] = useState(false); // Weekly
  const [showEma, setShowEma] = useState(false); // EMA

  // --- set primary (for Latest + MAs). does NOT reload prices ---
  function handleSelectSymbol(sym) {
    setSymbol(sym);
  }

  // --- add symbol (max 5) ---
  function handleAddSymbol(newSym) {
    if (!newSym) return;

    setActiveSymbols((prev) => {
      if (prev.includes(newSym)) return prev;
      let updated = [...prev, newSym];
      if (updated.length > MAX_ACTIVE) {
        updated = updated.slice(updated.length - MAX_ACTIVE);
      }
      return updated;
    });

    setSymbol(newSym);
  }

  // --- remove symbol via X on chip ---
  function handleRemoveSymbol(sym) {
    setActiveSymbols((prev) => {
      const updated = prev.filter((s) => s !== sym);

      if (updated.length === 0) {
        setSymbol("");
      } else if (sym === symbol) {
        // if we removed primary, pick the last remaining as new primary
        setSymbol(updated[updated.length - 1]);
      }

      return updated;
    });

    // drop cached price + options data for this symbol
    setSeriesBySymbol((prev) => {
      const copy = { ...prev };
      delete copy[sym];
      return copy;
    });
    setOptionsBySymbol((prev) => {
      const copy = { ...prev };
      delete copy[sym];
      return copy;
    });
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

          return {
            ...row,
            timeLabel,
          };
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

      const emptySymbols = results.filter(([, s]) => !s.length).map(([s]) => s);
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

  // ---- LOAD OPTIONS DATA FOR ALL ACTIVE SYMBOLS ----
  async function loadOptionsForSymbols(symbols = activeSymbols) {
    if (!symbols.length) return;

    setOptionsLoading(true);
    setOptionsError("");

    try {
      const fetches = symbols.map(async (sym) => {
        const url = `${API_BASE}/api/options?symbol=${encodeURIComponent(sym)}`;
        console.log("Fetching options:", url);

        const res = await fetch(url);
        if (!res.ok) {
          throw new Error(`HTTP ${res.status} for ${sym}`);
        }
        const json = await res.json();
        return [sym, json || []];
      });

      const results = await Promise.all(fetches);
      const map = {};
      for (const [sym, opts] of results) {
        map[sym] = opts;
      }
      setOptionsBySymbol(map);
    } catch (err) {
      console.error("Options fetch error:", err);
      setOptionsError("Failed to load options: " + err.message);
    } finally {
      setOptionsLoading(false);
    }
  }

  // initial + whenever activeSymbols or days change
  useEffect(() => {
    loadDataForSymbols();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeSymbols, days]);

  // Round numeric values to 2 decimals
function round2(v) {
  if (v == null || isNaN(v)) return null;
  return Number(v.toFixed(2));
}

  // INSERT THIS:
function getESTDateKey(isoTs) {
  const d = new Date(isoTs);
  if (Number.isNaN(d.getTime())) return null;

  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });

  return formatter.format(d); // "YYYY-MM-DD"
}

  // Format a UTC ISO timestamp into EST (New York) label for the candlestick x-axis
  function formatESTLabel(isoTs) {
    const d = new Date(isoTs);
    if (Number.isNaN(d.getTime())) return isoTs;

    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }).format(d);
  }

  // ----- Candlestick series for the PRIMARY symbol (EST labels, no gaps) -----
  const candleSeries = useMemo(() => {
    if (!symbol) return [];

    const series = seriesBySymbol[symbol] || [];
    if (!series.length) return [];

    const data = series.map((row) => ({
      // x is now a CATEGORY label, not a true datetime → no gaps between days
      x: formatESTLabel(row.ts_utc),
      y: [
        round2(row.open),
        round2(row.high),
        round2(row.low),
        round2(row.close),
      ],
    }));

    return [
      {
        name: symbol,
        data,
      },
    ];
  }, [seriesBySymbol, symbol]);

  // ----- Candlestick chart options -----
const candleOptions = useMemo(
  () => ({
    chart: {
      type: "candlestick",
      toolbar: {
        show: true,
      },
      background: "transparent",
    },
    title: {
      text: symbol ? `${symbol} Candlestick` : "Candlestick",
      align: "left",
      style: {
        fontSize: "14px",
        fontWeight: 500,
      },
    },
    // ⚠️ IMPORTANT: use category axis → removes gaps where there is no data
    xaxis: {
      type: "category",
      tickAmount: 8, // you can tweak this
      labels: {
        rotate: -90,
        trim: true,
        hideOverlappingLabels: true,
        style: {
          fontSize: "10px",
        },
      },
    },
    yaxis: {
      tooltip: {
        enabled: true,
      },
      labels: {
        formatter: (val) =>
          val == null || isNaN(val)
            ? ""
            : `$${Number(val).toFixed(2)}`,
      },
    },
    plotOptions: {
      candlestick: {
        colors: {
          upward: "#22c55e",
          downward: "#ef4444",
        },
      },
    },
    tooltip: {
      shared: true,
      theme: "dark",
      custom: function ({ seriesIndex, dataPointIndex, w }) {
        // Get the point we plotted: { x, y: [open, high, low, close] }
        const point =
          w?.config?.series?.[seriesIndex]?.data?.[dataPointIndex];

        if (!point || !Array.isArray(point.y)) return "";

        const [open, high, low, close] = point.y;
        const fmt = (v) => `$${Number(v).toFixed(2)}`;

        return (
          '<div class="apex-tooltip" style="background:#111827;color:#e5e7eb;padding:8px 10px;border-radius:4px;font-size:12px;">' +
          `Open: <b>${fmt(open)}</b><br/>` +
          `High: <b>${fmt(high)}</b><br/>` +
          `Low: <b>${fmt(low)}</b><br/>` +
          `Close: <b>${fmt(close)}</b>` +
          "</div>"
        );
      },
    },
  }),
  [symbol]
);

  // ----- Chart data: merge all active symbols into one timeline -----
  const chartData = useMemo(() => {
    if (!activeSymbols.length) return [];

    const map = new Map();

    activeSymbols.forEach((sym) => {
      const series = seriesBySymbol[sym] || [];
      series.forEach((row) => {
        const key = row.timeLabel;
        if (!map.has(key)) {
          map.set(key, { timeLabel: key, ts_utc: row.ts_utc });
        }
        const entry = map.get(key);
        entry[`${sym}_close`] = row.close;
        entry[`${sym}_maWeek`] = row.maWeek;
        entry[`${sym}_ma1M`] = row.ma1M;
        entry[`${sym}_ma3M`] = row.ma3M;
        entry[`${sym}_ma12M`] = row.ma12M;
        entry[`${sym}_ema`] = row.ema;
        entry[`${sym}_std20`] = row.std20;
      });
    });

    const combined = Array.from(map.values());
    combined.sort((a, b) => new Date(a.ts_utc) - new Date(b.ts_utc));
    return combined;
  }, [activeSymbols, seriesBySymbol]);

  // ----- Price table rows: flatten active symbols -----
  const tableRows = useMemo(() => {
    const rows = [];
    activeSymbols.forEach((sym) => {
      const series = seriesBySymbol[sym] || [];
      series.forEach((row) => {
        rows.push({ ...row, symbol: sym });
      });
    });

    rows.sort((a, b) => new Date(a.ts_utc) - new Date(b.ts_utc));
    return rows;
  }, [activeSymbols, seriesBySymbol]);

  // ----- Options table rows: flatten active symbols -----
const { callRows, putRows } = useMemo(() => {
  const rows = [];
  activeSymbols.forEach((sym) => {
    const opts = optionsBySymbol[sym] || [];
    opts.forEach((o) => {
      rows.push({ symbol: sym, ...o });
    });
  });

  const calls = [];
  const puts = [];

  rows.forEach((r) => {
    const t = (r.type || "").toString().toUpperCase();
    if (t === "CALL" || t === "C") {
      calls.push(r);
    } else if (t === "PUT" || t === "P") {
      puts.push(r);
    } else {
      // if unknown, you can choose where to put; default to calls
      calls.push(r);
    }
  });

  return { callRows: calls, putRows: puts };
}, [activeSymbols, optionsBySymbol]);

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
        {/* active symbols displayed as chips (max 5) */}
        <div className="control-group">
          <span className="control-label">Stocks in view</span>
          <div className="symbol-chips">
            {activeSymbols.map((s) => (
              <button
                key={s}
                type="button"
                className="symbol-chip"
                onClick={() => handleSelectSymbol(s)}
                style={{
                  background: s === symbol ? "#1d4ed8" : "#334155", // blue highlight
                  color: "white",
                  border: "none",
                  padding: "6px 10px",
                  borderRadius: "6px",
                  marginRight: "8px",
                  cursor: "pointer",
                }}
              >
                <span>{s}</span>
                {activeSymbols.length > 1 && (
                  <span
                    className="chip-remove"
                    onClick={(e) => {
                      e.stopPropagation(); // don't also select
                      handleRemoveSymbol(s);
                    }}
                  >
                    ×
                  </span>
                )}
              </button>
            ))}
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
          {loading ? "Loading..." : "Refresh Prices"}
        </button>

        <button
          onClick={() => loadOptionsForSymbols()}
          disabled={optionsLoading}
        >
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
              <strong>{symbol}</strong> –{" "}
              {new Date(latest.ts_utc).toLocaleString()} – Close:{" "}
              <strong>${latest.close.toFixed(2)}</strong>
            </p>
          </div>
        )}

        {/* ==== NEW: CANDLESTICK CHART FOR PRIMARY SYMBOL ==== */}
        <div className="chart-wrapper">
          <h2>{symbol ? `${symbol} Candlestick` : "Candlestick"}</h2>
          <div className="chart-inner">
            {candleSeries.length && candleSeries[0].data.length ? (
              <Chart
                options={candleOptions}
                series={candleSeries}
                type="candlestick"
                height={350}
                width="100%"
              />
            ) : (
              <p>No candlestick data for the selected symbol.</p>
            )}
          </div>
        </div>

        {/* ==== MULTI-STOCK PRICE CHART ==== */}
        <div className="chart-wrapper">
          <h2>Price (close) – {activeSymbols.join(", ")}</h2>
          <div className="chart-inner">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="timeLabel"
                  minTickGap={30}
                  tick={{ fontSize: 10 }}
                  angle={-90}
                  textAnchor="end"
                  height={80}
                />
                <YAxis
                  tick={{ fontSize: 10 }}
                  width={70}
                  domain={["auto", "auto"]}
                  tickFormatter={(value) => `$${value.toFixed(0)}`}
                />
                <Tooltip
                  content={(tp) => (
                    <CustomTooltip
                      {...tp}
                      activeSymbols={activeSymbols}
                      showWeek={showWeek}
                      show1M={show1M}
                      show3M={show3M}
                      show12M={show12M}
                      showEma={showEma}
                    />
                  )}
                  />
                <Legend />

                {/* One Close line per active symbol */}
                {activeSymbols.map((s, idx) => (
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

                {/* MAs/EMA for all active symbols */}
                {showWeek &&
                  activeSymbols.map((s, idx) => (
                    <Line
                      key={`${s}_maWeek`}
                      type="monotone"
                      dataKey={`${s}_maWeek`}
                      name={`${s} Weekly MA`}
                      stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                      strokeDasharray="4 2"
                      dot={false}
                      strokeWidth={1.2}
                      connectNulls
                    />
                  ))}

                {show1M &&
                  activeSymbols.map((s, idx) => (
                    <Line
                      key={`${s}_ma1M`}
                      type="monotone"
                      dataKey={`${s}_ma1M`}
                      name={`${s} Month MA`}
                      stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                      strokeDasharray="4 2"
                      dot={false}
                      strokeWidth={1.2}
                      connectNulls
                    />
                  ))}

                {show3M &&
                  activeSymbols.map((s, idx) => (
                    <Line
                      key={`${s}_ma3M`}
                      type="monotone"
                      dataKey={`${s}_ma3M`}
                      name={`${s} Quarter MA`}
                      stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                      strokeDasharray="4 2"
                      dot={false}
                      strokeWidth={1.2}
                      connectNulls
                    />
                  ))}

                {show12M &&
                  activeSymbols.map((s, idx) => (
                    <Line
                      key={`${s}_ma12M`}
                      type="monotone"
                      dataKey={`${s}_ma12M`}
                      name={`${s} Year MA`}
                      stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                      strokeDasharray="4 2"
                      dot={false}
                      strokeWidth={1.2}
                      connectNulls
                    />
                  ))}

                {showEma &&
                  activeSymbols.map((s, idx) => (
                    <Line
                      key={`${s}_ema`}
                      type="monotone"
                      dataKey={`${s}_ema`}
                      name={`${s} EMA (20d)`}
                      stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                      strokeDasharray="4 2"
                      dot={false}
                      strokeWidth={1.2}
                      connectNulls
                    />
                  ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* ==== PRICE TABLE (active symbols only) ==== */}
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

        {/* ==== OPTIONS TABLES (Calls & Puts) ==== */}
        <div className="table-wrapper">
          <h2>
            Options – Calls &amp; Puts{" "}
            <span style={{ fontSize: "0.9rem", fontWeight: "normal" }}>
              (Calls: {callRows.length} · Puts: {putRows.length})
            </span>
          </h2>

          {optionsLoading && <p>Loading options…</p>}

          {!optionsLoading &&
            callRows.length === 0 &&
            putRows.length === 0 && <p>No options loaded.</p>}

          {!optionsLoading &&
            (callRows.length > 0 || putRows.length > 0) && (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr",
                  gap: "16px",
                  maxHeight: "400px",
                  overflowY: "auto",
                }}
              >
                {/* Calls */}
                <div>
                  <h3>Calls ({callRows.length})</h3>
                  <table>
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Contract</th>
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
                      {callRows.map((o, idx) => (
                        <tr key={idx}>
                          <td>{o.symbol}</td>
                          <td>{o.contractSymbol}</td>
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

                {/* Puts */}
                <div>
                  <h3>Puts ({putRows.length})</h3>
                  <table>
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Contract</th>
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
                      {putRows.map((o, idx) => (
                        <tr key={idx}>
                          <td>{o.symbol}</td>
                          <td>{o.contractSymbol}</td>
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
              </div>
            )}
        </div>
      </main>
    </div>
  );
}
