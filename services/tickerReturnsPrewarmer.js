const analyticsData = require('../analyticsData');
const { makeCacheKey, setCache } = require('../utils/cache');

const TICKER_RETURNS_CACHE_TTL_SECS = Number(process.env.TICKER_RETURNS_CACHE_TTL_SECS || 300);
const ENABLE_PREWARM = process.env.ENABLE_TICKER_RETURNS_PREWARM !== '0';
const PREWARM_INTERVAL_MS = Number(process.env.TICKER_RETURNS_PREWARM_INTERVAL_MS || 300000);
const PREWARM_TICKERS_RAW = process.env.TICKER_RETURNS_PREWARM_TICKERS || 'AAPL,MSFT,NVDA';
const ENABLE_ON_DEMAND_PREWARM = process.env.ENABLE_TICKER_RETURNS_ON_DEMAND_PREWARM !== '0';
const INCLUDE_REQUESTED_IN_ON_DEMAND = process.env.INCLUDE_REQUESTED_TICKERS_IN_PREWARM !== '0';
const ENABLE_STARTUP_BLOCK = process.env.ENABLE_TICKER_RETURNS_STARTUP_BLOCK !== '0';
const STARTUP_BLOCK_TIMEOUT_MS = Number(process.env.TICKER_RETURNS_STARTUP_BLOCK_TIMEOUT_MS || 30000);

let timer = null;
let running = false;
let queueRunning = false;
const pendingTickers = new Set();

function parsePrewarmTickers() {
  const list = String(PREWARM_TICKERS_RAW || '')
    .split(',')
    .map((t) => String(t || '').trim().toUpperCase())
    .filter(Boolean);
  return [...new Set(list)];
}

function singleTickerCacheKey(ticker) {
  return makeCacheKey('market:ticker-returns:v1', {
    tickers: ticker.toUpperCase(),
    customStartDate: '',
    customEndDate: ''
  });
}

async function warmOneTicker(ticker) {
  const startedAt = Date.now();
  const payload = await analyticsData.calculateAllReturns(ticker, true, true, null, 1980);
  const cacheKey = singleTickerCacheKey(ticker);
  await setCache(cacheKey, payload, TICKER_RETURNS_CACHE_TTL_SECS);
  return Date.now() - startedAt;
}

async function runOnce(tickersInput = null) {
  if (running) return;
  const tickers = Array.isArray(tickersInput) && tickersInput.length ? tickersInput : parsePrewarmTickers();
  if (!tickers.length) return;
  running = true;
  const batchStartedAt = Date.now();
  try {
    const results = await Promise.allSettled(tickers.map((ticker) => warmOneTicker(ticker)));
    for (let i = 0; i < results.length; i += 1) {
      const ticker = tickers[i];
      const result = results[i];
      if (result.status === 'fulfilled') {
        console.log(`[ticker-returns-prewarm] ok ticker=${ticker} ms=${result.value}`);
      } else {
        console.error(
          `[ticker-returns-prewarm] failed ticker=${ticker}:`,
          result.reason?.message || result.reason
        );
      }
    }
    console.log(`[ticker-returns-prewarm] batch done tickers=${tickers.join(',')} ms=${Date.now() - batchStartedAt}`);
  } finally {
    running = false;
  }
}

async function drainQueue() {
  if (queueRunning) return;
  queueRunning = true;
  try {
    while (pendingTickers.size > 0) {
      const tickers = [...pendingTickers];
      pendingTickers.clear();
      const startedAt = Date.now();
      const results = await Promise.allSettled(tickers.map((ticker) => warmOneTicker(ticker)));
      for (let i = 0; i < results.length; i += 1) {
        const ticker = tickers[i];
        const result = results[i];
        if (result.status === 'fulfilled') {
          console.log(`[ticker-returns-prewarm] queued ok ticker=${ticker} ms=${result.value}`);
        } else {
          console.error(
            `[ticker-returns-prewarm] queued failed ticker=${ticker}:`,
            result.reason?.message || result.reason
          );
        }
      }
      console.log(`[ticker-returns-prewarm] queued batch done tickers=${tickers.join(',')} ms=${Date.now() - startedAt}`);
    }
  } finally {
    queueRunning = false;
  }
}

function enqueueTickers(tickers) {
  for (const t of tickers) {
    const sym = String(t || '').trim().toUpperCase();
    if (sym) pendingTickers.add(sym);
  }
  if (!pendingTickers.size) return;
  void drainQueue();
}

function warmTickerReturnsInBackground(requestedTickers = []) {
  if (!ENABLE_PREWARM || !ENABLE_ON_DEMAND_PREWARM) return;
  const defaults = parsePrewarmTickers();
  const requested = INCLUDE_REQUESTED_IN_ON_DEMAND
    ? requestedTickers.map((t) => String(t || '').trim().toUpperCase()).filter(Boolean)
    : [];
  const merged = [...new Set([...defaults, ...requested])];
  enqueueTickers(merged);
}

function startTickerReturnsPrewarmer() {
  if (!ENABLE_PREWARM) {
    console.log('[ticker-returns-prewarm] disabled (ENABLE_TICKER_RETURNS_PREWARM=0)');
    return;
  }
  const tickers = parsePrewarmTickers();
  if (!tickers.length) {
    console.log('[ticker-returns-prewarm] skipped (no tickers configured)');
    return;
  }
  const intervalMs = Number.isFinite(PREWARM_INTERVAL_MS) && PREWARM_INTERVAL_MS > 0 ? PREWARM_INTERVAL_MS : 300000;
  void runOnce();
  timer = setInterval(() => {
    void runOnce();
  }, intervalMs);
  if (typeof timer?.unref === 'function') timer.unref();
  console.log(`[ticker-returns-prewarm] started tickers=${tickers.join(',')} interval_ms=${intervalMs}`);
}

async function waitForTickerReturnsWarmup() {
  if (!ENABLE_PREWARM || !ENABLE_STARTUP_BLOCK) return;
  const tickers = parsePrewarmTickers();
  if (!tickers.length) return;
  const timeoutMs = Number.isFinite(STARTUP_BLOCK_TIMEOUT_MS) && STARTUP_BLOCK_TIMEOUT_MS > 0 ? STARTUP_BLOCK_TIMEOUT_MS : 30000;
  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => resolve('timeout'), timeoutMs);
  });
  const warmPromise = runOnce(tickers).then(() => 'ok');
  const result = await Promise.race([warmPromise, timeoutPromise]);
  if (result === 'timeout') {
    console.warn(`[ticker-returns-prewarm] startup warmup timeout after ${timeoutMs}ms; continuing startup`);
  } else {
    console.log('[ticker-returns-prewarm] startup warmup completed');
  }
}

module.exports = {
  startTickerReturnsPrewarmer,
  warmTickerReturnsInBackground,
  waitForTickerReturnsWarmup
};
