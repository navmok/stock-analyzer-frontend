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

  // Std dev windows
  const STD5_PERIOD = 5;
  const STD60_PERIOD = 60;
  const std5Window = [];
  const std60Window = [];

  function computeStdDev(arr) {
    if (!arr.length) return null;
    const mean = arr.reduce((acc, v) => acc + v, 0) / arr.length;
    const variance =
      arr.reduce((acc, v) => acc + (v - mean) * (v - mean), 0) /
      arr.length;
    return Math.sqrt(variance);
  }

  let sumWeek = 0,
    sum1 = 0,
    sum3 = 0,
    sum12 = 0;

  let ema = null;

  return rows.map((row, i) => {
    const close = row.close;

    // std-dev windows
    std5Window.push(close);
    if (std5Window.length > STD5_PERIOD) std5Window.shift();

    std60Window.push(close);
    if (std60Window.length > STD60_PERIOD) std60Window.shift();

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

    // Std devs
    const std5 =
      std5Window.length === STD5_PERIOD
        ? computeStdDev(std5Window)
        : null;
    const std60 =
      std60Window.length === STD60_PERIOD
        ? computeStdDev(std60Window)
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
      std5,
      std60,
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
  showStd5,
  showStd60,
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
        const std5 = row[`${sym}_std5`];
        const std60 = row[`${sym}_std60`];

        if (showWeek && wk != null) parts.push(`Wk: ${wk.toFixed(2)}`);
        if (show1M && m1 != null) parts.push(`1M: ${m1.toFixed(2)}`);
        if (show3M && m3 != null) parts.push(`3M: ${m3.toFixed(2)}`);
        if (show12M && y1 != null) parts.push(`12M: ${y1.toFixed(2)}`);
        if (showEma && ema != null) parts.push(`EMA: ${ema.toFixed(2)}`);
        if (showStd5 && std5 != null) parts.push(`σ5: ${std5.toFixed(2)}`);
        if (showStd60 && std60 != null) parts.push(`σ60: ${std60.toFixed(2)}`);

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
  // view grain for multi-stock line chart
  const [viewGrain, setViewGrain] = useState("day"); // "day" | "week" | "month" | "year"
  // Candlestick drill-down (Year → Month → Week → Day → Minute)
  const [candleDrillLevel, setCandleDrillLevel] = useState("year");
  const [drillYear, setDrillYear] = useState(null);
  const [drillMonthKey, setDrillMonthKey] = useState(null);
  const [drillWeekKey, setDrillWeekKey] = useState(null);
  const [drillDayKey, setDrillDayKey] = useState(null);

  // price data: symbol -> [rows with MAs]
  const [seriesBySymbol, setSeriesBySymbol] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // options data: symbol -> [option rows]
  const [optionsBySymbol, setOptionsBySymbol] = useState({});
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");
  const [optionsTabSymbol, setOptionsTabSymbol] = useState(DEFAULT_ACTIVE[0]);
  const [expiryBySymbol, setExpiryBySymbol] = useState({});
  const selectedExpiry = expiryBySymbol[optionsTabSymbol] || "ALL";

  // MA toggles (all off by default)
  const [show1M, setShow1M] = useState(false); // Month
  const [show3M, setShow3M] = useState(false); // Quarter
  const [show12M, setShow12M] = useState(false); // Year
  const [showWeek, setShowWeek] = useState(false); // Weekly
  const [showEma, setShowEma] = useState(false); // EMA
  const [showStd5, setShowStd5] = useState(false);   // σ5
  const [showStd60, setShowStd60] = useState(false); // σ60

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
      setOptionsBySymbol((prev) => ({ ...prev, ...map }));
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

  useEffect(() => {
  if (!activeSymbols.length) return;
  setOptionsTabSymbol((prev) =>
    activeSymbols.includes(prev) ? prev : activeSymbols[0]
  );
}, [activeSymbols]);

  // AUTO-LOAD OPTIONS for newly added / re-added symbols
  useEffect(() => {
  const missing = activeSymbols.filter((s) => !(s in optionsBySymbol));
  if (missing.length > 0) loadOptionsForSymbols(missing);
}, [activeSymbols]);

  // Reset candlestick drill when user changes symbol or days
useEffect(() => {
  setCandleDrillLevel("year");
  setDrillYear(null);
  setDrillMonthKey(null);
  setDrillWeekKey(null);
  setDrillDayKey(null);
}, [symbol, days]);

  // Round numeric values to 2 decimals
function round2(v) {
  if (v == null || isNaN(v)) return null;
  return Number(v.toFixed(2));
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

  // ISO week number (for Year → Month → Week drill)
function getISOWeekNumber(d) {
  const date = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  const dayNum = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil(((date - yearStart) / 86400000 + 1) / 7);
  return weekNo;
}

// Group a row into a grain bucket (day/week/month/year)
function getGrainBucket(row, grain) {
  const d = new Date(row.ts_utc);
  if (Number.isNaN(d.getTime())) return null;

  const year = d.getFullYear();
  const month = d.getMonth() + 1;
  const week = getISOWeekNumber(d); // helper defined above
  const dayLabel = row.timeLabel; // already formatted for small-day views

  switch (grain) {
    case "year": {
      const key = `${year}`;
      return { key, label: key, ts: d };
    }
    case "month": {
      const key = `${year}-${String(month).padStart(2, "0")}`;
      return { key, label: `${month}/${year}`, ts: d };
    }
    case "week": {
      const key = `${year}-W${String(week).padStart(2, "0")}`;
      return { key, label: `W${week} ${year}`, ts: d };
    }
    case "day":
    default: {
      const key = dayLabel;
      return { key, label: dayLabel, ts: d };
    }
  }
}

// Build candlestick OHLC buckets for a given drill level
function buildCandlestickData(series, level, ctx = {}) {
  if (!series || !series.length) return [];

  const groups = new Map();

  series.forEach((row) => {
    const date = new Date(row.ts_utc);
    if (Number.isNaN(date.getTime())) return;

    const year = date.getFullYear();
    const monthKey = `${year}-${String(date.getMonth() + 1).padStart(2, "0")}`;
    const weekKey = `${year}-W${String(getISOWeekNumber(date)).padStart(2, "0")}`;
    const dayKey = date.toISOString().slice(0, 10);

    // Filter by parent context
    if (level === "month" && ctx.year && year !== ctx.year) return;
    if (level === "week" && ctx.monthKey && monthKey !== ctx.monthKey) return;
    if (level === "day" && ctx.weekKey && weekKey !== ctx.weekKey) return;
    if (level === "minute" && ctx.dayKey && dayKey !== ctx.dayKey) return;

    let bucketKey;
    let label;

    switch (level) {
      case "year":
        bucketKey = String(year);
        label = bucketKey;
        break;
      case "month":
        bucketKey = monthKey;            // e.g. 2025-05
        label = monthKey;
        break;
      case "week":
        bucketKey = weekKey;             // e.g. 2025-W18
        label = weekKey;
        break;
      case "day":
        bucketKey = dayKey;              // e.g. 2025-05-06
        label = dayKey;
        break;
      case "minute":
        bucketKey = `${dayKey} ${date.toISOString().slice(11, 16)}`; // YYYY-MM-DD HH:MM
        label = formatESTLabel(row.ts_utc); // nice EST label
        break;
      default:
        bucketKey = dayKey;
        label = dayKey;
    }

    const existing = groups.get(bucketKey);
    if (!existing) {
      groups.set(bucketKey, {
        key: bucketKey,
        label,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        ts_utc: row.ts_utc,
      });
    } else {
      existing.high = Math.max(existing.high, row.high);
      existing.low = Math.min(existing.low, row.low);
      // keep latest close / timestamp
      if (new Date(row.ts_utc) >= new Date(existing.ts_utc)) {
        existing.close = row.close;
        existing.ts_utc = row.ts_utc;
      }
    }
  });

  const arr = Array.from(groups.values());
  arr.sort((a, b) => new Date(a.ts_utc) - new Date(b.ts_utc));

  return arr.map((d) => ({
    x: d.label,
    y: [
      round2(d.open),
      round2(d.high),
      round2(d.low),
      round2(d.close),
    ],
    drillKey: d.key,
  }));
}

// ----- Candlestick series with drill-down -----
const candleSeries = useMemo(() => {
  if (!symbol) return [];

  const series = seriesBySymbol[symbol] || [];
  if (!series.length) return [];

  const data = buildCandlestickData(series, candleDrillLevel, {
    year: drillYear,
    monthKey: drillMonthKey,
    weekKey: drillWeekKey,
    dayKey: drillDayKey,
  });

  return [
    {
      name: symbol,
      data,
    },
  ];
}, [
  seriesBySymbol,
  symbol,
  candleDrillLevel,
  drillYear,
  drillMonthKey,
  drillWeekKey,
  drillDayKey,
]);

  // ----- Candlestick chart options -----
const candleOptions = useMemo(
  () => ({
    chart: {
      type: "candlestick",
      toolbar: {
        show: true,
      },
      background: "transparent",
      events: {
        dataPointSelection: (event, chartContext, config) => {
          const sIdx = config.seriesIndex;
          const pIdx = config.dataPointIndex;
          const point =
            config?.w?.config?.series?.[sIdx]?.data?.[pIdx];

          if (!point || !point.drillKey) return;

          setCandleDrillLevel((prev) => {
            if (prev === "year") {
              setDrillYear(Number(point.drillKey));
              return "month";
            }
            if (prev === "month") {
              setDrillMonthKey(point.drillKey);
              return "week";
            }
            if (prev === "week") {
              setDrillWeekKey(point.drillKey);
              return "day";
            }
            if (prev === "day") {
              setDrillDayKey(point.drillKey);
              return "minute";
            }
            return prev;
          });
        },
      },
    },
    title: {
      text: symbol
        ? `${symbol} Candlestick – ${candleDrillLevel.toUpperCase()} view`
        : `Candlestick – ${candleDrillLevel.toUpperCase()} view`,
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
  [symbol, candleDrillLevel]
);

  // ----- Chart data: merge all active symbols into one timeline, aggregated by viewGrain -----
  const chartData = useMemo(() => {
    if (!activeSymbols.length) return [];

    const buckets = new Map();

    activeSymbols.forEach((sym) => {
      const series = seriesBySymbol[sym] || [];
      series.forEach((row) => {
        const bucket = getGrainBucket(row, viewGrain);
        if (!bucket) return;

        const { key, label, ts } = bucket;
        let entry = buckets.get(key);
        if (!entry) {
          entry = {
            bucketKey: key,
            timeLabel: label,
            ts_utc: ts.toISOString(),
          };
          buckets.set(key, entry);
        }

        const t = ts.getTime();
        const prevTs = entry[`${sym}_ts`] || 0;

        // use the *latest* row in the bucket for that symbol
        if (t >= prevTs) {
          entry[`${sym}_ts`] = t;
          entry[`${sym}_close`] = row.close;
          entry[`${sym}_maWeek`] = row.maWeek;
          entry[`${sym}_ma1M`] = row.ma1M;
          entry[`${sym}_ma3M`] = row.ma3M;
          entry[`${sym}_ma12M`] = row.ma12M;
          entry[`${sym}_ema`] = row.ema;
          entry[`${sym}_std5`] = row.std5;
          entry[`${sym}_std60`] = row.std60;
        }
      });
    });

    const combined = Array.from(buckets.values()).map((entry) => {
      const clean = { ...entry };
      Object.keys(clean).forEach((k) => {
        if (k.endsWith("_ts") || k === "bucketKey") delete clean[k];
      });
      return clean;
    });

    combined.sort((a, b) => new Date(a.ts_utc) - new Date(b.ts_utc));
    return combined;
  }, [activeSymbols, seriesBySymbol, viewGrain]);

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

  const expirationsForTab = useMemo(() => {
    const rows = optionsBySymbol[optionsTabSymbol] || [];
    const set = new Set();
    rows.forEach((o) => {
      if (!o.expiration) return;
      const d = new Date(o.expiration);
      if (!Number.isNaN(d.getTime())) set.add(d.toISOString().slice(0, 10)); // YYYY-MM-DD
    });
    return Array.from(set).sort();
  }, [optionsBySymbol, optionsTabSymbol]);

  // ----- Options table rows: flatten active symbols -----
const { callRows, putRows } = useMemo(() => {
  const rows = (optionsBySymbol[optionsTabSymbol] || []).map((o) => ({
    symbol: optionsTabSymbol,
    ...o,
  }));

  const filtered =
    selectedExpiry === "ALL"
      ? rows
      : rows.filter((r) => {
          if (!r.expiration) return false;
          const d = new Date(r.expiration);
          if (Number.isNaN(d.getTime())) return false;
          return d.toISOString().slice(0, 10) === selectedExpiry;
        });

  const calls = [];
  const puts = [];

  filtered.forEach((r) => {
    const t = (r.type || "").toString().toUpperCase();
    if (t === "CALL" || t === "C") calls.push(r);
    else if (t === "PUT" || t === "P") puts.push(r);
    else calls.push(r);
  });

  return { callRows: calls, putRows: puts };
}, [optionsBySymbol, optionsTabSymbol, selectedExpiry]);

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

        {/* View grain for second chart (like Google Calendar) */}
        <div className="control">
          <label>View</label>
          <div style={{ display: "flex", gap: 8 }}>
            {["day", "week", "month", "year"].map((g) => (
              <button
                key={g}
                type="button"
                onClick={() => setViewGrain(g)}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #4b5563",
                  background: viewGrain === g ? "#2563eb" : "transparent",
                  color: "#e5e7eb",
                  fontSize: "0.8rem",
                  cursor: "pointer",
                }}
              >
                {g[0].toUpperCase() + g.slice(1)}
              </button>
            ))}
          </div>
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
            <label className="ma-toggle">
            <span>σ5 (std dev)</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={showStd5}
                onChange={(e) => setShowStd5(e.target.checked)}
              />
              <span className="slider"></span>
            </label>
          </label>

          <label className="ma-toggle">
            <span>σ60 (std dev)</span>
            <label className="switch">
              <input
                type="checkbox"
                checked={showStd60}
                onChange={(e) => setShowStd60(e.target.checked)}
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
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: "4px",
            }}
          >
            <h2>
              {symbol ? `${symbol} Candlestick` : "Candlestick"} ·{" "}
              {candleDrillLevel === "year"
                ? "Year"
                : candleDrillLevel === "month"
                ? "Month"
                : candleDrillLevel === "week"
                ? "Week"
                : candleDrillLevel === "day"
                ? "Day"
                : "Minute"}
            </h2>

            {candleDrillLevel !== "year" && (
              <button
                type="button"
                onClick={() => {
                  setCandleDrillLevel((prev) => {
                    if (prev === "minute") {
                      setDrillDayKey(null);
                      return "day";
                    }
                    if (prev === "day") {
                      setDrillWeekKey(null);
                      return "week";
                    }
                    if (prev === "week") {
                      setDrillMonthKey(null);
                      return "month";
                    }
                    if (prev === "month") {
                      setDrillYear(null);
                      return "year";
                    }
                    return prev;
                  });
                }}
                style={{
                  fontSize: "0.8rem",
                  padding: "4px 8px",
                  borderRadius: "6px",
                  border: "none",
                  background: "#1f2937",
                  color: "#e5e7eb",
                  cursor: "pointer",
                }}
              >
                Drill up
              </button>
            )}
          </div>
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
          <h2>
            Price (close) – {activeSymbols.join(", ")} ·{" "}
            {viewGrain === "year"
              ? "Year"
              : viewGrain === "month"
              ? "Month"
              : viewGrain === "week"
              ? "Week"
              : "Day"}{" "}
            view
          </h2>
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
                      showStd5={showStd5}
                      showStd60={showStd60}
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

  {/* Tabs + Expiration dropdown */}
  <div
    style={{
      display: "flex",
      gap: 10,
      alignItems: "center",
      flexWrap: "wrap",
      marginBottom: 8,
    }}
  >
    {/* Symbol Tabs */}
    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
      {activeSymbols.map((s) => (
        <button
          key={s}
          type="button"
          onClick={() => setOptionsTabSymbol(s)}
          style={{
            padding: "4px 10px",
            borderRadius: 999,
            border: "1px solid #4b5563",
            background: optionsTabSymbol === s ? "#2563eb" : "transparent",
            color: "#e5e7eb",
            cursor: "pointer",
            fontSize: "0.85rem",
          }}
        >
          {s}
        </button>
      ))}
    </div>

    {/* Expiration Dropdown */}
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <span style={{ fontSize: "0.85rem", opacity: 0.9 }}>Expiration</span>
              <select
                value={selectedExpiry}
                onChange={(e) =>
                  setExpiryBySymbol((prev) => ({
                    ...prev,
                    [optionsTabSymbol]: e.target.value,
                  }))
                }
              >
                <option value="ALL">All</option>
                {expirationsForTab.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </div>
          </div>

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
                {/* ===== CALLS ===== */}
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

                {/* ===== PUTS ===== */}
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