// api/options.js
// Polygon Options Chain Snapshot (paid plan) implementation.
// Docs: GET /v3/snapshot/options/{underlyingAsset}

function pickNumber(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function mapPolygonContract(r) {
  // Polygon snapshot results structure varies slightly; keep mapping defensive.
  const details = r?.details || {};
  const day = r?.day || {};
  const lastQuote = r?.last_quote || {};
  const greeks = r?.greeks || {};
  const underlying = r?.underlying_asset || {};

  // Prefer Polygon’s own ticker if present (ex: O:RIOT250117C00015000)
  const contractSymbol = details?.ticker || r?.ticker || null;

  return {
    contractSymbol,
    type: details?.contract_type === "call" ? "C" : details?.contract_type === "put" ? "P" : null,
    strike: pickNumber(details?.strike_price) ?? null,
    lastPrice: pickNumber(day?.close) ?? null,
    bid: pickNumber(lastQuote?.bid) ?? null,
    ask: pickNumber(lastQuote?.ask) ?? null,
    volume: pickNumber(day?.volume) ?? null,
    openInterest: pickNumber(r?.open_interest) ?? null,
    expiration: details?.expiration_date ?? null,
    impliedVolatility: pickNumber(r?.implied_volatility) ?? null,

    // Underlying (optional)
    underlyingSymbol: underlying?.ticker ?? null,
    underlyingPrice: pickNumber(underlying?.price) ?? null,

    // Greeks (optional)
    delta: pickNumber(greeks?.delta) ?? null,
    gamma: pickNumber(greeks?.gamma) ?? null,
    theta: pickNumber(greeks?.theta) ?? null,
    vega: pickNumber(greeks?.vega) ?? null,
    rho: pickNumber(greeks?.rho) ?? null,
  };
}

async function fetchPolygonChain({ symbol, apiKey, contractType, expirationDate, limit = 250 }) {
  const base = `https://api.polygon.io/v3/snapshot/options/${encodeURIComponent(symbol)}`;
  const qs = new URLSearchParams();
  qs.set("apiKey", apiKey);
  qs.set("limit", String(limit));

  // Optional filters supported by Polygon:
  if (contractType) qs.set("contract_type", contractType); // "call" | "put"
  if (expirationDate) qs.set("expiration_date", expirationDate); // "YYYY-MM-DD"

  // Add a timeout so Vercel functions don't hang forever
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  try {
    const url = `${base}?${qs.toString()}`;
    const resp = await fetch(url, { signal: controller.signal });

    const text = await resp.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      json = null;
    }

    if (!resp.ok) {
      // Bubble up Polygon error details (very useful for plan/permission issues)
      const msg = `Polygon error ${resp.status} ${resp.statusText}: ${text?.slice(0, 500) || ""}`;
      const err = new Error(msg);
      err.status = resp.status;
      throw err;
    }

    return json;
  } finally {
    clearTimeout(timeout);
  }
}

export default async function handler(req, res) {
  // GET /api/options?symbol=RIOT&expiration=2026-01-16
  const symbol = String(req.query?.symbol || "").trim().toUpperCase();
  const expiration = req.query?.expiration ? String(req.query.expiration).trim() : null;

  if (!symbol) {
    return res.status(400).json({ error: "Missing required query param: symbol" });
  }

  const apiKey = String(process.env.POLYGON_API_KEY || "").trim();
  if (!apiKey) {
    return res.status(500).json({
      error:
        "Missing POLYGON_API_KEY environment variable. Add it in Vercel → Project → Settings → Environment Variables, then redeploy.",
    });
  }

  try {
    // Grab both calls + puts.
    const [callsJson, putsJson] = await Promise.all([
      fetchPolygonChain({ symbol, apiKey, contractType: "call", expirationDate: expiration, limit: 250 }),
      fetchPolygonChain({ symbol, apiKey, contractType: "put", expirationDate: expiration, limit: 250 }),
    ]);

    const calls = (callsJson?.results || []).map(mapPolygonContract).filter((o) => o.contractSymbol);
    const puts = (putsJson?.results || []).map(mapPolygonContract).filter((o) => o.contractSymbol);

    return res.status(200).json([...calls, ...puts]);
  } catch (err) {
    // No mock fallback — fail loudly
    return res.status(502).json({
      error: "Failed to fetch options from Polygon",
      details: String(err?.message || err),
    });
  }
}
