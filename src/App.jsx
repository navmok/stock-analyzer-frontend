import { useEffect, useState } from "react";
import "./App.css";

const API_BASE =
  process.env.NODE_ENV === "production"
    ? "https://stock-analyzer-frontend-cxem.vercel.app"
    : "";

const SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"];

function App() {
  const [symbol, setSymbol] = useState("GOOGL");
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [options, setOptions] = useState([]);
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [optionsError, setOptionsError] = useState("");

  // ---- LOAD PRICE DATA ----
  async function loadData(sym = symbol) {
    setLoading(true);
    setError("");

    try {
      const url = `${API_BASE}/api/prices?symbol=${encodeURIComponent(sym)}&days=1`;
      console.log("Fetching prices:", url);

      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const json = await res.json();
      setData(json || []);
    } catch (err) {
      console.error(err);
      setError("Failed to load prices: " + err.message);
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
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const json = await res.json();
      console.log("Options response:", json); // DEBUG

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
  }, []);

  return (
    <div style={{ padding: "20px", color: "white" }}>
      <h1>Stock Dashboard (MVP)</h1>

      <div style={{ marginBottom: "20px" }}>
        <label>Symbol: </label>
        <select
          value={symbol}
          onChange={(e) => {
            setSymbol(e.target.value);
            loadData(e.target.value);
          }}
        >
          {SYMBOLS.map((s) => (
            <option key={s}>{s}</option>
          ))}
        </select>

        <button onClick={() => loadData()} disabled={loading}>
          {loading ? "Loading..." : "Refresh"}
        </button>

        <button onClick={() => loadOptions()} disabled={optionsLoading}>
          {optionsLoading ? "Loading options..." : "Load Options"}
        </button>

        {error && <p style={{ color: "red" }}>{error}</p>}
        {optionsError && <p style={{ color: "red" }}>{optionsError}</p>}
      </div>

      <h2>Options ({options.length} rows)</h2>
      {options.length > 0 ? (
        <table style={{ width: "100%", color: "white" }}>
          <thead>
            <tr>
              <th>Contract</th>
              <th>Type</th>
              <th>Strike</th>
              <th>Last</th>
              <th>Bid</th>
              <th>Ask</th>
              <th>Volume</th>
              <th>Open Int</th>
              <th>Expiry</th>
            </tr>
          </thead>
          <tbody>
            {options.map((o, i) => (
              <tr key={i}>
                <td>{o.contractSymbol}</td>
                <td>{o.type}</td>
                <td>{o.strike}</td>
                <td>{o.lastPrice}</td>
                <td>{o.bid}</td>
                <td>{o.ask}</td>
                <td>{o.volume}</td>
                <td>{o.openInterest}</td>
                <td>{o.expiration}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p>No options loaded.</p>
      )}
    </div>
  );
}

export default App;
