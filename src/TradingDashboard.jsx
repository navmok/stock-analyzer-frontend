import { useEffect, useState, useMemo, useCallback } from "react";
import Chart from "react-apexcharts";
import "./TradingDashboard.css";

const API_BASE = "";

const DEFAULT_WATCHLIST = ["AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "SPY"];

export default function TradingDashboard() {
  const [watchlist, setWatchlist] = useState(DEFAULT_WATCHLIST);
  const [selectedSymbol, setSelectedSymbol] = useState("GOOGL");
  const [timeframe, setTimeframe] = useState("1D");
  const [chartType, setChartType] = useState("candlestick");
  const [priceData, setPriceData] = useState({});
  const [loading, setLoading] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [watchlistPrices, setWatchlistPrices] = useState({});
  const [newSymbol, setNewSymbol] = useState("");

  const timeframeToDays = { "1D": 1, "5D": 5, "1M": 30, "3M": 90, "1Y": 365 };

  const fetchPriceData = useCallback(async (symbol, days) => {
    try {
      const url = `${API_BASE}/api/prices?symbol=${encodeURIComponent(symbol)}&days=${days}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json() || [];
    } catch (err) {
      console.error(`Error fetching ${symbol}:`, err);
      return [];
    }
  }, []);

  const loadChartData = useCallback(async () => {
    if (!selectedSymbol) return;
    setLoading(true);
    const days = timeframeToDays[timeframe] || 30;
    const data = await fetchPriceData(selectedSymbol, days);
    setPriceData(prev => ({ ...prev, [selectedSymbol]: data }));
    setLastUpdate(new Date());
    setLoading(false);
  }, [selectedSymbol, timeframe, fetchPriceData]);

  const loadWatchlistPrices = useCallback(async () => {
    const prices = {};
    await Promise.all(
      watchlist.map(async (symbol) => {
        const data = await fetchPriceData(symbol, 2);
        if (data.length > 0) {
          const latest = data[data.length - 1];
          const prev = data.length > 1 ? data[data.length - 2] : latest;
          const change = latest.close - prev.close;
          const changePercent = prev.close ? (change / prev.close) * 100 : 0;
          prices[symbol] = {
            price: latest.close, change, changePercent,
            high: latest.high, low: latest.low, volume: latest.volume,
          };
        }
      })
    );
    setWatchlistPrices(prices);
  }, [watchlist, fetchPriceData]);

  useEffect(() => { loadChartData(); loadWatchlistPrices(); }, []);
  useEffect(() => { loadChartData(); }, [selectedSymbol, timeframe]);
  useEffect(() => {
    const interval = setInterval(() => { loadChartData(); loadWatchlistPrices(); }, 30000);
    return () => clearInterval(interval);
  }, [loadChartData, loadWatchlistPrices]);

  const candlestickData = useMemo(() => {
    const data = priceData[selectedSymbol] || [];
    return data.map(row => ({
      x: new Date(row.ts_utc),
      y: [parseFloat(row.open?.toFixed(2)), parseFloat(row.high?.toFixed(2)),
          parseFloat(row.low?.toFixed(2)), parseFloat(row.close?.toFixed(2))]
    }));
  }, [priceData, selectedSymbol]);

  const lineData = useMemo(() => {
    const data = priceData[selectedSymbol] || [];
    return data.map(row => ({ x: new Date(row.ts_utc), y: parseFloat(row.close?.toFixed(2)) }));
  }, [priceData, selectedSymbol]);

  const latestPrice = useMemo(() => {
    const data = priceData[selectedSymbol] || [];
    if (!data.length) return null;
    const latest = data[data.length - 1];
    const prev = data.length > 1 ? data[0] : latest;
    const change = latest.close - prev.close;
    const changePercent = prev.close ? (change / prev.close) * 100 : 0;
    return {
      price: latest.close, change, changePercent, open: latest.open,
      high: Math.max(...data.map(d => d.high)), low: Math.min(...data.map(d => d.low)),
      volume: latest.volume, timestamp: latest.ts_utc,
    };
  }, [priceData, selectedSymbol]);

  const chartOptions = useMemo(() => ({
    chart: {
      type: chartType === "candlestick" ? "candlestick" : "line",
      height: 500, background: "transparent", foreColor: "#9ca3af",
      animations: { enabled: true, easing: "easeinout", speed: 500, dynamicAnimation: { enabled: true, speed: 300 } },
      toolbar: { show: true, tools: { download: false, selection: true, zoom: true, zoomin: true, zoomout: true, pan: true, reset: true }, autoSelected: "zoom" },
      zoom: { enabled: true, type: "x", autoScaleYaxis: true }
    },
    grid: { borderColor: "#1f2937", strokeDashArray: 3 },
    xaxis: {
      type: "datetime",
      labels: { style: { colors: "#9ca3af", fontSize: "11px" }, datetimeFormatter: { year: "yyyy", month: "MMM 'yy", day: "dd MMM", hour: "HH:mm" } },
      axisBorder: { color: "#374151" }, axisTicks: { color: "#374151" },
      crosshairs: { show: true, stroke: { color: "#6366f1", width: 1, dashArray: 3 } }
    },
    yaxis: {
      tooltip: { enabled: true },
      labels: { style: { colors: "#9ca3af", fontSize: "11px" }, formatter: (val) => `$${val?.toFixed(2) || "0.00"}` },
      crosshairs: { show: true, stroke: { color: "#6366f1", width: 1, dashArray: 3 } }
    },
    plotOptions: { candlestick: { colors: { upward: "#22c55e", downward: "#ef4444" }, wick: { useFillColor: true } } },
    stroke: chartType === "line" ? { curve: "smooth", width: 2, colors: ["#3b82f6"] } : undefined,
    tooltip: { enabled: true, theme: "dark", x: { format: "MMM dd, yyyy HH:mm" }, y: { formatter: (val) => `$${val?.toFixed(2)}` } },
  }), [chartType]);

  const addToWatchlist = () => {
    const symbol = newSymbol.toUpperCase().trim();
    if (symbol && !watchlist.includes(symbol)) { setWatchlist(prev => [...prev, symbol]); setNewSymbol(""); }
  };
  const removeFromWatchlist = (symbol) => { setWatchlist(prev => prev.filter(s => s !== symbol)); };

  return (
    <div className="trading-dashboard">
      <header className="td-header">
        <div className="td-header-left">
          <h1>📈 Trading Dashboard</h1>
          <span className="market-status"><span className="status-dot live"></span>Market Open</span>
        </div>
        <div className="td-header-right">
          <span className="last-update">Last update: {lastUpdate?.toLocaleTimeString() || "--:--:--"}</span>
          <button className="refresh-btn" onClick={() => { loadChartData(); loadWatchlistPrices(); }} disabled={loading}>
            {loading ? "⟳" : "↻"} Refresh
          </button>
        </div>
      </header>

      <div className="td-main">
        <aside className="td-sidebar-left">
          <div className="watchlist-panel">
            <div className="panel-header">
              <h3>Watchlist</h3>
              <div className="add-symbol">
                <input type="text" placeholder="Add..." value={newSymbol} onChange={(e) => setNewSymbol(e.target.value)} onKeyDown={(e) => e.key === "Enter" && addToWatchlist()} />
                <button onClick={addToWatchlist}>+</button>
              </div>
            </div>
            <div className="watchlist-items">
              {watchlist.map(symbol => {
                const data = watchlistPrices[symbol];
                const isSelected = symbol === selectedSymbol;
                const isUp = data?.change >= 0;
                return (
                  <div key={symbol} className={`watchlist-item ${isSelected ? "selected" : ""} ${isUp ? "up" : "down"}`} onClick={() => setSelectedSymbol(symbol)}>
                    <div className="wi-left"><span className="wi-symbol">{symbol}</span></div>
                    <div className="wi-right">
                      <span className="wi-price">${data?.price?.toFixed(2) || "--"}</span>
                      <span className={`wi-change ${isUp ? "up" : "down"}`}>{isUp ? "+" : ""}{data?.change?.toFixed(2) || "0.00"} ({isUp ? "+" : ""}{data?.changePercent?.toFixed(2) || "0.00"}%)</span>
                    </div>
                    <button className="wi-remove" onClick={(e) => { e.stopPropagation(); removeFromWatchlist(symbol); }}>×</button>
                  </div>
                );
              })}
            </div>
          </div>
        </aside>

        <main className="td-chart-area">
          <div className="chart-header">
            <div className="symbol-info">
              <h2 className="symbol-name">{selectedSymbol}</h2>
              {latestPrice && (
                <div className="price-info">
                  <span className={`current-price ${latestPrice.change >= 0 ? "up" : "down"}`}>${latestPrice.price?.toFixed(2)}</span>
                  <span className={`price-change ${latestPrice.change >= 0 ? "up" : "down"}`}>{latestPrice.change >= 0 ? "+" : ""}{latestPrice.change?.toFixed(2)} ({latestPrice.change >= 0 ? "+" : ""}{latestPrice.changePercent?.toFixed(2)}%)</span>
                </div>
              )}
            </div>
            <div className="chart-controls">
              <div className="timeframe-buttons">
                {["1D", "5D", "1M", "3M", "1Y"].map(tf => (
                  <button key={tf} className={`tf-btn ${timeframe === tf ? "active" : ""}`} onClick={() => setTimeframe(tf)}>{tf}</button>
                ))}
              </div>
              <div className="chart-type-toggle">
                <button className={`ct-btn ${chartType === "candlestick" ? "active" : ""}`} onClick={() => setChartType("candlestick")}>🕯️</button>
                <button className={`ct-btn ${chartType === "line" ? "active" : ""}`} onClick={() => setChartType("line")}>📈</button>
              </div>
            </div>
          </div>
          <div className="chart-container">
            {loading && <div className="chart-loading">Loading...</div>}
            {candlestickData.length > 0 ? (
              <Chart options={chartOptions} series={[{ name: selectedSymbol, data: chartType === "candlestick" ? candlestickData : lineData }]} type={chartType === "candlestick" ? "candlestick" : "line"} height={480} width="100%" />
            ) : (
              <div className="no-data">{loading ? "Loading..." : "No data available"}</div>
            )}
          </div>
        </main>

        <aside className="td-sidebar-right">
          <div className="quote-panel">
            <div className="panel-header"><h3>Quote</h3></div>
            {latestPrice && (
              <div className="quote-details">
                <div className="quote-row"><span className="quote-label">Open</span><span className="quote-value">${latestPrice.open?.toFixed(2)}</span></div>
                <div className="quote-row"><span className="quote-label">High</span><span className="quote-value up">${latestPrice.high?.toFixed(2)}</span></div>
                <div className="quote-row"><span className="quote-label">Low</span><span className="quote-value down">${latestPrice.low?.toFixed(2)}</span></div>
                <div className="quote-row"><span className="quote-label">Volume</span><span className="quote-value">{latestPrice.volume?.toLocaleString()}</span></div>
              </div>
            )}
          </div>
          <div className="quick-trade-panel">
            <div className="panel-header"><h3>Quick Trade</h3></div>
            <div className="trade-buttons">
              <button className="trade-btn buy">Buy</button>
              <button className="trade-btn sell">Sell</button>
            </div>
            <div className="trade-inputs">
              <div className="trade-input-group"><label>Symbol</label><input type="text" value={selectedSymbol} readOnly /></div>
              <div className="trade-input-group"><label>Qty</label><input type="number" defaultValue={100} /></div>
            </div>
            <div className="trade-summary"><span>Est. Total:</span><span className="trade-total">${((latestPrice?.price || 0) * 100).toFixed(2)}</span></div>
          </div>
        </aside>
      </div>
    </div>
  );
}
