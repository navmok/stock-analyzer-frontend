// pages/api/options.js (Next.js API route)
// Polygon Options Chain Snapshot (paid plan) implementation.
// Docs: GET /v3/snapshot/options/{underlyingAsset}

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
      impliedVolatility: null,
      delta: null,
      gamma: null,
      theta: null,
      vega: null,
      rho: null,
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
      impliedVolatility: null,
      delta: null,
      gamma: null,
      theta: null,
      vega: null,
      rho: null,
    });
  }

  return options;
}

function toIsoDateMaybe(v) {
  if (!v) return null;
  if (typeof v === "number") return new Date(v).toISOString();
  if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v)) return v + "T00:00:00Z";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function pickNumber(...vals) {
  for (const v of vals) {
    if (typeof v === "number" && Number.isFinite(v)) return v;
  }
  return null;
}

function mapPolygonContract(r) {
  const details = r?.details || {};
  const ct = (details?.contract_type || details?.contractType || "").toLowerCase();
  const type = ct === "call" ? "C" : ct === "put" ? "P" : null;

  const lastTrade = r?.last_trade || r?.lastTrade || {};
  const lastQuote = r?.last_quote || r?.lastQuote || {};
  const day = r?.day || {};

  const lastPrice = pickNumber(
    lastTrade?.price, lastTrade?.p,
    day?.close, day?.c,
    r?.last_price, r?.lastPrice
  );

  const bid = pickNumber(lastQuote?.bid, lastQuote?.bid_price, lastQuote?.p, lastQuote?.bp);
  const ask = pickNumber(lastQuote?.ask, lastQuote?.ask_price, lastQuote?.ap);

  const volume = pickNumber(day?.volume, day?.v, r?.volume);
  const openInterest = pickNumber(r?.open_interest, r?.openInterest);

  const strike = pickNumber(details?.strike_price, details?.strikePrice, r?.strike_price, r?.strike);

  const contractSymbol = details?.ticker || details?.ticker_symbol || details?.tickerSymbol || r?.ticker;

  const expiration = toIsoDateMaybe(details?.expiration_date || details?.expirationDate || r?.expiration_date);

  const greeks = r?.greeks || {};
  const impliedVolatility = pickNumber(
    r?.implied_volatility,
    r?.iv,
    greeks?.implied_volatility
  );

  return {
    contractSymbol: contractSymbol || null,
    type: type || null, // "C" or "P"
    strike: strike ?? null,
    lastPrice: lastPrice ?? null,
    bid: bid ?? null,
    ask: ask ?? null,
    volume: volume ?? null,
    openInterest: openInterest ?? null,
    expiration,
    inTheMoney: r?.in_the_money ?? r?.inTheMoney ?? null,
    impliedVolatility: impliedVolatility ?? null,
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
  if (expirationDate) qs.set("expiration_date", expirationDate); // YYYY-MM-DD

  const url = `${base}?${qs.toString()}`;

  const r = await fetch(url);
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`Polygon HTTP ${r.status}: ${txt?.slice(0, 300)}`);
  }

  return r.json();
}

export default async function handler(req, res) {
  const symbol = (req.query?.symbol || "GOOGL").toUpperCase();
  const useMock = req.query?.mock === "1"; // optional: ?mock=1 to force mock
  const expiration = req.query?.expiration || null; // optional: YYYY-MM-DD

  if (useMock) {
    return res.status(200).json(buildMockOptions(symbol));
  }

  const apiKey = process.env.POLYGON_API_KEY || "";

  if (!apiKey) {
    console.error("Missing POLYGON_API_KEY env var. Falling back to mock options.");
    return res.status(200).json(buildMockOptions(symbol));
  }

  try {
    // Grab both calls + puts (Polygon lets you filter by contract_type).
    const [callsJson, putsJson] = await Promise.all([
      fetchPolygonChain({ symbol, apiKey, contractType: "call", expirationDate: expiration, limit: 250 }),
      fetchPolygonChain({ symbol, apiKey, contractType: "put", expirationDate: expiration, limit: 250 }),
    ]);

    const calls = (callsJson?.results || []).map(mapPolygonContract).filter(o => o.contractSymbol);
    const puts = (putsJson?.results || []).map(mapPolygonContract).filter(o => o.contractSymbol);

    return res.status(200).json([...calls, ...puts]);
  } catch (err) {
    console.error("Error fetching Polygon options chain snapshot:", err);
    return res.status(200).json(buildMockOptions(symbol));
  }
}
