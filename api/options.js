// api/options.js
// Yahoo Finance options with crumb + cookie handling

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";

let yahooSession = {
  cookie: null,
  crumb: null,
  lastInit: 0,
};

async function initYahooSession() {
  // Re-use for 1 hour if already fetched
  if (
    yahooSession.cookie &&
    yahooSession.crumb &&
    Date.now() - yahooSession.lastInit < 60 * 60 * 1000
  ) {
    return;
  }

  const resp = await fetch(
    "https://query1.finance.yahoo.com/v1/test/getcrumb",
    {
      headers: {
        "User-Agent": UA,
        Accept: "text/plain,*/*",
      },
      redirect: "follow",
    }
  );

  const text = (await resp.text()).trim();
  const setCookie = resp.headers.get("set-cookie");

  if (!text || !setCookie) {
    throw new Error("Failed to init Yahoo crumb/cookie");
  }

  yahooSession = {
    cookie: setCookie.split(";")[0], // first cookie only
    crumb: text,
    lastInit: Date.now(),
  };
}

function mapOption(o, type) {
  return {
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
  };
}

export default async function handler(req, res) {
  const { symbol = "GOOGL" } = req.query ?? {};

  try {
    await initYahooSession();

    const { cookie, crumb } = yahooSession;

    const url = `https://query2.finance.yahoo.com/v7/finance/options/${encodeURIComponent(
      symbol
    )}?crumb=${encodeURIComponent(crumb)}`;

    const r = await fetch(url, {
      headers: {
        "User-Agent": UA,
        Accept: "application/json,text/plain,*/*",
        Cookie: cookie,
      },
    });

    if (!r.ok) {
      console.error("Yahoo options status", r.status);
      return res.status(200).json([]);
    }

    let json;
    try {
      json = await r.json();
    } catch (e) {
      console.error("Yahoo options JSON error", e);
      return res.status(200).json([]);
    }

    const chain = json?.optionChain?.result?.[0];
    const opt = chain?.options?.[0];
    if (!opt) {
      return res.status(200).json([]);
    }

    const calls = opt.calls || [];
    const puts = opt.puts || [];

    const options = [
      ...calls.map((o) => mapOption(o, "C")),
      ...puts.map((o) => mapOption(o, "P")),
    ];

    return res.status(200).json(options);
  } catch (e) {
    console.error("options handler error", e);
    return res.status(200).json([]); // fail soft, never 500
  }
}
