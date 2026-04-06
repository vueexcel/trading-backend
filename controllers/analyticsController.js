const bigquery = require('../config/bigquery');
const supabase = require('../config/supabase');
const { makeCacheKey, getCache, setCache } = require('../utils/cache');

const ODIN_INDEX_CACHE_TTL_SECS = Number(process.env.ODIN_INDEX_CACHE_TTL_SECS || 300);
const ODIN_SUMMARY_CACHE_TTL_SECS = Number(process.env.ODIN_SUMMARY_CACHE_TTL_SECS || 300);

const PROJECT_ID = process.env.GCP_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || 'extended-byway-454621-s6';
const DATASET = process.env.BIGQUERY_DATASET || 'sp500data1';
const TABLE = process.env.BIGQUERY_TABLE || 'stock_all_data';
const TABLE_FQN = `${PROJECT_ID}.${DATASET}.${TABLE}`;
// Odin sheet: prices from StockData3Year200DA, signals from consolidated_data (same as sheet)
const ODIN_PRICE_TABLE = '`extended-byway-454621-s6.sp500data1.StockData3Year200DA`';
const ODIN_SIGNALS_TABLE = '`extended-byway-454621-s6.sp500data1.consolidated_data`';

// Dashboard main tables by category (for signal lookup) - same as watchlistUtils
const DASHBOARD_MAIN_TABLES = {
  'Dow Jones': '`extended-byway-454621-s6.sp500data1.Dashboard_dowJones`',
  'Nasdaq 100': '`extended-byway-454621-s6.sp500data1.Dashboard_Nasdaq100`',
  'SP500': '`extended-byway-454621-s6.sp500data1.Dashboard_SP500`',
  'ETF': '`extended-byway-454621-s6.sp500data1.Dashboard_ETF`',
  'Other': '`extended-byway-454621-s6.sp500data1.Dashboard_Other`'
};

const CATEGORY_ALIASES = {
  's&p 500': 'SP500', 's&p500': 'SP500', 'sp 500': 'SP500',
  'nasdaq': 'Nasdaq 100', 'nasdaq 100': 'Nasdaq 100',
  'dow jones': 'Dow Jones', 'dow': 'Dow Jones', 'etf': 'ETF', 'other': 'Other'
};

function normalizeCategory(name) {
  if (!name || typeof name !== 'string') return 'Other';
  const trimmed = name.trim();
  const key = trimmed.toLowerCase();
  if (CATEGORY_ALIASES[key]) return CATEGORY_ALIASES[key];
  return DASHBOARD_MAIN_TABLES[trimmed] !== undefined ? trimmed : 'Other';
}

/** Map signal_type string to 'long' | 'short' | null. Supports L1/S1 (sheet), above/below, long/short. */
function parseSignal(signalType) {
  if (signalType == null) return null;
  const s = String(signalType).trim().toUpperCase();
  if (s.includes('L') || s.includes('ABOVE') || s.includes('LONG') || s === 'BTO') return 'long';
  if (s.includes('S') || s.includes('BELOW') || s.includes('SHORT') || s === 'STO') return 'short';
  return null;
}

/** Get ticker category (market group name) from Supabase by symbol */
async function getCategoryBySymbol(symbol) {
  const sym = (symbol || '').toString().trim().toUpperCase();
  if (!sym) return 'Other';
  const { data: tickRows, error: tickErr } = await supabase
    .from('tickers')
    .select('id, ticker_groups(group_id)')
    .eq('symbol', sym)
    .limit(1);
  if (tickErr || !tickRows || tickRows.length === 0) return 'Other';
  const tg = tickRows[0].ticker_groups;
  const gid = Array.isArray(tg) ? tg[0]?.group_id : tg?.group_id;
  if (!gid) return 'Other';
  const { data: grp, error: grpErr } = await supabase
    .from('market_groups')
    .select('name')
    .eq('id', gid)
    .single();
  if (grpErr || !grp || !grp.name) return 'Other';
  return normalizeCategory(grp.name);
}

/**
 * Fetch daily signals for ticker in date range from a given Dashboard main table.
 * Returns array of { date: 'YYYY-MM-DD', signal: 'long'|'short' } or empty.
 */
async function fetchSignalsFromTable(tablePath, ticker, startStr, endStr) {
  const sym = ticker.toUpperCase();
  try {
    const q = `
      SELECT market_date AS dt, signal_type
      FROM ${tablePath}
      WHERE ticker = @ticker
        AND market_date BETWEEN @start AND @end
      ORDER BY market_date
    `;
    const [rows] = await bigquery.query({
      query: q,
      params: { ticker: sym, start: startStr, end: endStr }
    });
    if (rows && rows.length > 0) {
      const out = [];
      for (const r of rows) {
        const dt = r.dt && (r.dt.value || r.dt);
        const dateStr = typeof dt === 'string' ? dt : (dt && dt.toISOString ? new Date(dt).toISOString().split('T')[0] : null);
        const sig = parseSignal(r.signal_type);
        if (dateStr && sig) out.push({ date: dateStr, signal: sig });
      }
      return out;
    }
  } catch (e) {
    // Table may not have market_date or ticker; ignore
  }
  try {
    const q = `
      SELECT signal_type
      FROM ${tablePath}
      WHERE ticker = @ticker
      LIMIT 1
    `;
    const [rows] = await bigquery.query({ query: q, params: { ticker: sym } });
    if (rows && rows.length > 0) {
      const sig = parseSignal(rows[0].signal_type);
      if (sig) return [{ date: startStr, signal: sig }];
    }
  } catch (err) {
    // ignore
  }
  return [];
}

/**
 * Fetch signals from consolidated_data (same source as Odin Index sheet).
 * Schema: Ticker, Date, Signal (e.g. "L1", "S1"). Returns [{ date, signal }] sorted by date.
 */
async function fetchSignalsFromConsolidatedData(ticker, startDate, endDate) {
  const startStr = startDate.toISOString ? startDate.toISOString().split('T')[0] : startDate;
  const endStr = endDate.toISOString ? endDate.toISOString().split('T')[0] : endDate;
  const sym = ticker.toUpperCase();
  try {
    const q = `
      SELECT \`Date\` AS dt, \`Signal\` AS sig
      FROM ${ODIN_SIGNALS_TABLE}
      WHERE \`Ticker\` = @ticker
        AND \`Date\` BETWEEN @start AND @end
      ORDER BY \`Date\`
    `;
    const [rows] = await bigquery.query({
      query: q,
      params: { ticker: sym, start: startStr, end: endStr }
    });
    if (!rows || rows.length === 0) return [];
    const out = [];
    for (const r of rows) {
      const dt = r.dt && (r.dt.value || r.dt);
      const dateStr = typeof dt === 'string' ? dt : (dt && dt.toISOString ? new Date(dt).toISOString().split('T')[0] : null);
      const sig = parseSignal(r.sig);
      if (dateStr && sig) out.push({ date: dateStr, signal: sig });
    }
    return out;
  } catch (e) {
    console.warn('Odin: consolidated_data signal fetch failed', e.message);
    return [];
  }
}

/** Fallback: try Dashboard tables if consolidated_data returns no signals. */
async function fetchSignalsForOdin(ticker, startDate, endDate, category) {
  const consolidated = await fetchSignalsFromConsolidatedData(ticker, startDate, endDate);
  if (consolidated.length > 0) return consolidated;
  const startStr = startDate.toISOString ? startDate.toISOString().split('T')[0] : startDate;
  const endStr = endDate.toISOString ? endDate.toISOString().split('T')[0] : endDate;
  const tableOrder = [category, 'SP500', 'Nasdaq 100', 'Other', 'Dow Jones', 'ETF'].filter(
    (c, i, a) => c && a.indexOf(c) === i
  );
  for (const cat of tableOrder) {
    const tablePath = DASHBOARD_MAIN_TABLES[cat];
    if (!tablePath) continue;
    const signals = await fetchSignalsFromTable(tablePath, ticker, startStr, endStr);
    if (signals.length > 0) return signals;
  }
  return [];
}

/** Fetch daily OPEN prices for ticker from StockData3Year200DA (Odin sheet logic) */
async function fetchOpenPricesForOdin(ticker, startDate, endDate) {
  const startStr = startDate.toISOString ? startDate.toISOString().split('T')[0] : startDate;
  const endStr = endDate.toISOString ? endDate.toISOString().split('T')[0] : endDate;
  const sym = ticker.toUpperCase();
  const query = `
    SELECT market_date AS dt, open_price AS open_p
    FROM ${ODIN_PRICE_TABLE}
    WHERE ticker = @ticker
      AND market_date BETWEEN @start AND @end
    ORDER BY market_date
  `;
  try {
    const [rows] = await bigquery.query({
      query,
      params: { ticker: sym, start: startStr, end: endStr }
    });
    if (!rows || rows.length === 0) return new Map();
    const map = new Map();
    for (const r of rows) {
      const dt = r.dt && (r.dt.value || r.dt);
      const dateStr = typeof dt === 'string' ? dt : (dt && dt.toISOString ? new Date(dt).toISOString().split('T')[0] : null);
      if (dateStr != null) map.set(dateStr, Number(r.open_p));
    }
    return map;
  } catch (e) {
    return new Map();
  }
}

/** Fetch daily close prices for ticker (fallback / equity curve) from stock table */
async function fetchPricesForOdin(ticker, startDate, endDate) {
  const startStr = startDate.toISOString ? startDate.toISOString().split('T')[0] : startDate;
  const endStr = endDate.toISOString ? endDate.toISOString().split('T')[0] : endDate;
  const sym = ticker.toUpperCase();
  const query = `
    SELECT Date AS dt, Adj_Close AS adj_close, Close AS close
    FROM \`${TABLE_FQN}\`
    WHERE Ticker = @ticker
      AND Date BETWEEN @start AND @end
    ORDER BY Date
  `;
  const [rows] = await bigquery.query({
    query,
    params: { ticker: sym, start: startStr, end: endStr }
  });
  if (!rows || rows.length === 0) return [];
  return rows.map(r => {
    const dt = r.dt && (r.dt.value || r.dt);
    const d = typeof dt === 'string' ? new Date(dt) : (dt && dt.toISOString ? dt : new Date(dt));
    const close = r.adj_close != null ? Number(r.adj_close) : Number(r.close);
    return { date: d.toISOString().split('T')[0], dateObj: d, close };
  });
}

/**
 * Build Odin Index trade log matching the sheet script exactly:
 * - Open prices from price map; close at open price of reversal day.
 * - accumPL1 *= (1 + pnlPct); accumPL2 = accumPL1 - 1; odxValue = startPrice * accumPL1.
 * - Baseline row 0 (script: 0, startDate, "", ..., 1.0, 0.0, startPrice, startPrice, ..., initialPortfolio, 0).
 * signals = [{ date, signal }] sorted by date; openPriceByDate = Map<dateStr, openPrice>.
 */
function buildOdinSheetTradeLog(signals, openPriceByDate, startDateStr, initialPortfolio, ticker) {
  const tradeRows = [];
  const startDate = new Date(startDateStr);
  const msPerDay = 1000 * 60 * 60 * 24;

  const withPrice = signals.filter(s => openPriceByDate.has(s.date));
  if (withPrice.length === 0) return { trade_log: [], startPrice: null, accumPL1: 1, lastSamplePortfolio: initialPortfolio };

  const startPrice = openPriceByDate.get(withPrice[0].date);
  let currentDir = null;
  let tradeOpenDate = null;
  let tradeOpenPrice = 0;
  let accumPL1 = 1.0;
  let tradeId = 0;
  let lastSamplePortfolio = initialPortfolio;
  let lastSamplePortfolioPL = 0;

  for (let i = 0; i < withPrice.length; i++) {
    const { date: dateStr, signal } = withPrice[i];
    const newDir = signal;
    const currentPrice = openPriceByDate.get(dateStr);
    if (currentPrice == null) continue;

    if (currentDir === null) {
      currentDir = newDir;
      tradeOpenDate = dateStr;
      tradeOpenPrice = currentPrice;
      continue;
    }

    if (newDir !== currentDir) {
      tradeId++;
      const closeDate = dateStr;
      const closePrice = currentPrice;
      const diffVal = closePrice - tradeOpenPrice;
      // Difference (%) = (Close Value - Open Value) / Open Value (raw price return, decimal)
      const rawPct = tradeOpenPrice !== 0 ? diffVal / tradeOpenPrice : 0;
      const pnlPct = currentDir === 'long' ? rawPct : -rawPct;

      accumPL1 = accumPL1 * (1 + pnlPct);
      const accumPL2 = accumPL1 - 1.0;
      const odxValue = startPrice * accumPL1;
      const portVal = initialPortfolio * accumPL1;
      const portPL = portVal - initialPortfolio;
      lastSamplePortfolio = portVal;
      lastSamplePortfolioPL = portPL;

      const closeDateObj = new Date(closeDate);
      const openDateObj = new Date(tradeOpenDate);
      const holdDays = Math.round((closeDateObj - openDateObj) / msPerDay);
      const totalDays = Math.round((closeDateObj - startDate) / msPerDay);
      const totalYears = totalDays / 365.25;
      const avYearly = totalYears > 0.01 ? accumPL2 / totalYears : 0;

      tradeRows.push({
        '#': tradeId,
        'Open Date': tradeOpenDate,
        'BTO open long': currentDir === 'long' ? 1 : '',
        'STO open short': currentDir === 'short' ? 1 : '',
        'Type': currentDir,
        'Open Value': Math.round(tradeOpenPrice * 100) / 100,
        'Close Date': closeDate,
        'BTC close short': currentDir === 'short' ? 1 : '',
        'STC close long': currentDir === 'long' ? 1 : '',
        'Close Value': Math.round(closePrice * 100) / 100,
        'Difference ($)': Math.round(diffVal * 100) / 100,
        'Difference (%)': rawPct,
        'P&L (%)': pnlPct,
        'Holding days': holdDays,
        'Accumulated P&L 1 (%)': accumPL1,
        'Accumulated P&L 2 (%)': accumPL2,
        'ODX': Math.round(odxValue * 100) / 100,
        'Days count': totalDays,
        'Months count': parseFloat((totalDays / 30.4).toFixed(1)),
        'Years count': parseFloat(totalYears.toFixed(1)),
        'Av. Yearly return': avYearly,
        'Sample portfolio ($)': Math.round(portVal),
        'Sample portfolio P&L ($)': Math.round(portPL)
      });

      currentDir = newDir;
      tradeOpenDate = dateStr;
      tradeOpenPrice = currentPrice;
    }
  }

  const sortedTrades = tradeRows.sort((a, b) => (b['#'] || 0) - (a['#'] || 0));
  const baselineRow = {
    '#': 0,
    'Open Date': startDateStr,
    'BTO open long': '',
    'STO open short': '',
    'Type': '',
    'Open Value': '',
    'Close Date': '',
    'BTC close short': '',
    'STC close long': '',
    'Close Value': '',
    'Difference ($)': '',
    'Difference (%)': '',
    'P&L (%)': '',
    'Holding days': '',
    'Accumulated P&L 1 (%)': 1.0,
    'Accumulated P&L 2 (%)': 0.0,
    'ODX': startPrice != null ? Math.round(startPrice * 100) / 100 : '',
    'Days count': '',
    'Months count': '',
    'Years count': '',
    'Av. Yearly return': '',
    'Sample portfolio ($)': initialPortfolio,
    'Sample portfolio P&L ($)': 0
  };
  const trade_log = [...sortedTrades, baselineRow];
  return { trade_log, startPrice, accumPL1, lastSamplePortfolio, lastSamplePortfolioPL };
}

/**
 * Run backtest for equity curve (daily P&L from close prices) and summary.
 */
function runBacktest(initialPortfolio, prices, signalsByDate) {
  const sortedDates = [...new Set(prices.map(p => p.date))].sort();
  const priceByDate = new Map(prices.map(p => [p.date, p.close]));

  if (sortedDates.length === 0) {
    return {
      equity_curve: [],
      summary: { total_trades: 0, holding_days_total: 0, years: 0, avg_yearly_return_pct: null }
    };
  }

  let position = null;
  let value = initialPortfolio;
  const equityCurve = [];

  for (let i = 0; i < sortedDates.length; i++) {
    const date = sortedDates[i];
    const price = priceByDate.get(date);
    if (price == null) continue;
    const prevDate = i > 0 ? sortedDates[i - 1] : null;
    const prevPrice = prevDate != null ? priceByDate.get(prevDate) : null;
    let signal = signalsByDate.get(date);
    if (signal == null) signal = position;
    const dailyReturn = (prevPrice != null && prevPrice !== 0) ? (price - prevPrice) / prevPrice : 0;
    if (position !== null) {
      if (position === 'long') value = value * (1 + dailyReturn);
      else if (position === 'short') value = value * (1 - dailyReturn);
    }
    if (signal === 'long' || signal === 'short') {
      const wasCash = position === null;
      position = signal;
      if (wasCash && dailyReturn !== 0) {
        if (position === 'long') value = value * (1 + dailyReturn);
        else value = value * (1 - dailyReturn);
      }
    }
    equityCurve.push({ date, value: Math.round(value * 100) / 100 });
  }

  // Reduce to monthly: one point per month (last trading day of month)
  const byMonth = new Map();
  for (const p of equityCurve) {
    const ym = p.date.slice(0, 7);
    const existing = byMonth.get(ym);
    if (!existing || p.date > existing.date) byMonth.set(ym, { date: p.date, value: p.value });
  }
  const equityCurveMonthly = [...byMonth.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([, p]) => ({ date: p.date, value: p.value }));

  const finalPortfolio = value;
  const totalReturnPct = initialPortfolio !== 0 ? ((finalPortfolio - initialPortfolio) / initialPortfolio) * 100 : 0;
  const firstDate = new Date(sortedDates[0]);
  const lastDate = new Date(sortedDates[sortedDates.length - 1]);
  const years = (lastDate - firstDate) / (1000 * 60 * 60 * 24 * 365.25) || 0;

  return {
    final_portfolio: Math.round(finalPortfolio * 100) / 100,
    total_return_pct: Math.round(totalReturnPct * 100) / 100,
    equity_curve: equityCurveMonthly,
    summary: {
      years: Math.round(years * 100) / 100,
      avg_yearly_return_pct: years > 0 ? Math.round((totalReturnPct / years) * 100) / 100 : null
    }
  };
}

// Odin summary (sheet parity) source tables
const ODIN_SUMMARY_PRICE_TABLE = `\`${TABLE_FQN}\``;
const ODIN_SUMMARY_SIGNAL_TABLE = '`extended-byway-454621-s6.sp500data1.consolidated_testing_2`';
const ODIN_INDEX_PRICE_TABLE = `\`${process.env.ODIN_INDEX_PRICE_TABLE || TABLE_FQN}\``;
const ODIN_INDEX_SIGNAL_TABLE = `\`${process.env.ODIN_INDEX_SIGNAL_TABLE || `${PROJECT_ID}.${DATASET}.test`}\``;
const ODIN_MA200_DASHBOARD_TABLE = `\`${process.env.ODIN_MA200_DASHBOARD_TABLE || `${PROJECT_ID}.${DATASET}.ma200_dashboard_final`}\``;

// ─── Signal classification (mirrors Apps Script T / T+1 multi-source) ─────
// MA200_dashboard_1_signals — same codes as _MA1_SIGS in sheet script (no plain N; use N1 for MA1 neutral)
const MA1_SIGNALS = new Set(['L11', 'L21', 'L31', 'S11', 'S21', 'S31']);
// MA200_dashboard_2_signals — checked before MA1 in _sigSource (no plain N; use N2 for MA2 neutral)
const MA2_SIGNALS = new Set(['L12', 'L22', 'L32', 'S12', 'S22', 'S32']);

function _sigSource(sig) {
  if (MA2_SIGNALS.has(sig)) return 'ma2';
  if (MA1_SIGNALS.has(sig)) return 'ma1';
  return 'old';
}

function _sigFires(sig, oldSig, ma1Sig, ma2Sig) {
  // Column-specific neutral exits: data columns still store "N"; pickers N / N1 / N2 choose which column counts.
  if (sig === 'N1') return ma1Sig === 'N';
  if (sig === 'N2') return ma2Sig === 'N';
  const source = _sigSource(sig);
  if (source === 'ma2') return ma2Sig === sig;
  if (source === 'ma1') return ma1Sig === sig;
  return oldSig === sig;
}

function _anyFires(sigSet, oldSig, ma1Sig, ma2Sig) {
  for (const sig of sigSet) {
    if (_sigFires(sig, oldSig, ma1Sig, ma2Sig)) return sig;
  }
  return null;
}

function toList(input) {
  if (Array.isArray(input)) return input.map(v => String(v).trim()).filter(Boolean);
  if (typeof input === 'string') return input.split(',').map(v => v.trim()).filter(Boolean);
  return [];
}

function dateKeySimple(d) {
  return `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`;
}

function parseDateLike(v) {
  const raw = v && (v.value || v);
  const d = raw ? new Date(raw) : null;
  return d && !Number.isNaN(d.getTime()) ? d : null;
}

function nextTradeOpen(sigDate, priceMap) {
  const next = new Date(sigDate);
  for (let i = 0; i < 7; i++) {
    next.setDate(next.getDate() + 1);
    const key = dateKeySimple(next);
    if (priceMap.has(key)) return { date: new Date(next), open: priceMap.get(key) };
  }
  return null;
}

function normalizeGroupToken(token) {
  let t = String(token || '').trim().toLowerCase();
  if (!t) return '';
  t = t.replace(/^group[_\-\s]*/i, '');
  t = t.replace(/[^a-z0-9]/g, '');
  if (t === 'dowjones' || t === 'dow') return 'dowjones';
  if (t === 'nasdaq' || t === 'nasdaq100' || t === 'ndx') return 'nasdaq100';
  if (t === 'sp500' || t === 'sandp500' || t === 'snp500') return 'sp500';
  if (t === 'etf') return 'etf';
  if (t === 'other') return 'other';
  return t;
}

async function resolveTickerUniverseForSummary(inputTickers, inputGroups) {
  const explicitTickers = [...new Set(toList(inputTickers).map(t => t.toUpperCase()))];
  const requestedGroups = toList(inputGroups);
  const warnings = [];
  let groupTickers = [];

  if (requestedGroups.length > 0) {
    const { data: groups, error: grpErr } = await supabase
      .from('market_groups')
      .select('id,name,code');
    if (grpErr) throw grpErr;

    const groupMap = new Map();
    (groups || []).forEach(g => {
      const byName = normalizeGroupToken(g.name);
      if (byName) groupMap.set(byName, g.id);
      const byCode = normalizeGroupToken(g.code);
      if (byCode) groupMap.set(byCode, g.id);
    });

    const resolvedIds = [];
    for (const g of requestedGroups) {
      const key = normalizeGroupToken(g);
      const gid = groupMap.get(key);
      if (gid) resolvedIds.push(gid);
      else warnings.push(`Unknown group ignored: ${g}`);
    }

    const ids = [...new Set(resolvedIds)];
    if (ids.length > 0) {
      const { data: links, error: linkErr } = await supabase
        .from('ticker_groups')
        .select('group_id, tickers(symbol)')
        .in('group_id', ids);
      if (linkErr) throw linkErr;
      groupTickers = (links || [])
        .map(r => {
          const t = r.tickers;
          if (Array.isArray(t)) return t.map(x => x && x.symbol).filter(Boolean);
          return t && t.symbol ? [t.symbol] : [];
        })
        .flat()
        .map(s => String(s).trim().toUpperCase())
        .filter(Boolean);
    }
  }

  const resolvedTickers = [...new Set([...groupTickers, ...explicitTickers])];
  return { resolvedTickers, warnings, explicitTickers, requestedGroups };
}

async function fetchSummaryBatchPrices(tickers, startStr, endStr) {
  if (!tickers.length) return [];
  const tickerList = tickers.map(t => `'${t.replace(/'/g, "\\'")}'`).join(',');
  const query = `
    SELECT Ticker, Date, Open
    FROM ${ODIN_SUMMARY_PRICE_TABLE}
    WHERE Ticker IN (${tickerList})
      AND Date >= '${startStr}'
      AND Date <= DATE_ADD('${endStr}', INTERVAL 7 DAY)
    ORDER BY Ticker, Date ASC
  `;
  const [rows] = await bigquery.query({ query });
  return rows || [];
}

async function fetchSummaryBatchSignals(tickers, startStr, endStr) {
  if (!tickers.length) return [];
  const tickerList = tickers.map(t => `'${t.replace(/'/g, "\\'")}'`).join(',');
  const query = `
    SELECT Ticker, Date, Signal
    FROM ${ODIN_SUMMARY_SIGNAL_TABLE}
    WHERE Ticker IN (${tickerList})
      AND Date >= '${startStr}'
      AND Date <= '${endStr}'
    ORDER BY Ticker, Date ASC
  `;
  const [rows] = await bigquery.query({ query });
  return rows || [];
}

async function fetchIndexBatchPrices(tickers, startStr, endStr) {
  if (!tickers.length) return [];
  const tickerList = tickers.map(t => `'${t.replace(/'/g, "\\'")}'`).join(',');
  const query = `
    SELECT Ticker, Date, Open, Close
    FROM ${ODIN_INDEX_PRICE_TABLE}
    WHERE Ticker IN (${tickerList})
      AND Date >= '${startStr}'
      AND Date <= DATE_ADD('${endStr}', INTERVAL 7 DAY)
    ORDER BY Ticker, Date ASC
  `;
  const [rows] = await bigquery.query({ query });
  return rows || [];
}

async function fetchIndexBatchSignals(tickers, startStr, endStr) {
  if (!tickers.length) return [];
  const tickerList = tickers.map(t => `'${t.replace(/'/g, "\\'")}'`).join(',');
  const query = `
    SELECT
      t.Ticker,
      t.Date,
      t.Signal                                        AS old_signal,
      COALESCE(m.MA200_dashboard_1_signals, '')       AS ma1_signal,
      COALESCE(m.MA200_dashboard_2_signals, '')       AS ma2_signal
    FROM ${ODIN_INDEX_SIGNAL_TABLE} t
    LEFT JOIN ${ODIN_MA200_DASHBOARD_TABLE} m
      ON t.Ticker = m.Ticker AND t.Date = m.Date
    WHERE t.Ticker IN (${tickerList})
      AND t.Date >= '${startStr}'
      AND t.Date <= '${endStr}'
    ORDER BY t.Ticker ASC, t.Date ASC
  `;
  const [rows] = await bigquery.query({ query });
  return rows || [];
}

function nextTradeFromPriceData(sigDate, priceMap) {
  const next = new Date(sigDate);
  for (let i = 0; i < 7; i++) {
    next.setDate(next.getDate() + 1);
    const key = dateKeySimple(next);
    if (priceMap.has(key)) return { date: new Date(next), prices: priceMap.get(key) };
  }
  return null;
}

function toDateString(d) {
  return d && d.toISOString ? d.toISOString().split('T')[0] : '';
}

function buildSheetParityTradeRowsForTicker({
  ticker,
  priceMap,
  signals,
  startDate,
  entryLongSet,
  exitLongSet,
  entryShortSet,
  exitShortSet,
  initialPortfolio,
  includeNeutralRows
}) {
  if (!priceMap || priceMap.size === 0) return { rows: [], warning: `No prices: ${ticker}`, completedTrades: 0 };
  if (!signals || signals.length === 0) return { rows: [], warning: `No signals: ${ticker}`, completedTrades: 0 };

  const startPriceData = priceMap.get(dateKeySimple(startDate)) || Array.from(priceMap.values())[0];
  if (!startPriceData || startPriceData.open == null) {
    return { rows: [], warning: `No start price: ${ticker}`, completedTrades: 0 };
  }

  const startPrice = Number(startPriceData.open);
  const rows = [];
  let currentPos = null;
  let tradeOpenDate = null;
  let tradeOpenPrice = 0;
  let tradeSignalOpenDate = null;
  let tradeSignalOpenCloseVal = 0;
  let tradeEntrySignal = '';
  let accumPL1 = 1.0;
  let tradeID = 0;

  for (let i = 0; i < signals.length; i++) {
    const sigDate = signals[i].date;
    // Support both old {signal} shape and new {oldSig, ma1Sig, ma2Sig} shape
    const oldSig = String(signals[i].oldSig || signals[i].signal || '').trim().toUpperCase();
    const ma1Sig = String(signals[i].ma1Sig || '').trim().toUpperCase();
    const ma2Sig = String(signals[i].ma2Sig || '').trim().toUpperCase();
    const sigPriceData = priceMap.get(dateKeySimple(sigDate));

    if (!sigPriceData) continue;
    if (!oldSig && !ma1Sig && !ma2Sig) continue;
    if (oldSig === 'NULL') continue;
    if (i > 0 && signals[i - 1].date.getTime() === sigDate.getTime()) continue;

    // ── _anyFires routing: each configured signal checks its own source column ──
    const firesEntryLong  = _anyFires(entryLongSet,  oldSig, ma1Sig, ma2Sig);
    const firesExitLong   = _anyFires(exitLongSet,   oldSig, ma1Sig, ma2Sig);
    const firesEntryShort = _anyFires(entryShortSet, oldSig, ma1Sig, ma2Sig);
    const firesExitShort  = _anyFires(exitShortSet,  oldSig, ma1Sig, ma2Sig);

    let newPos = currentPos;
    let logNeutral = false;
    let firedSignal = oldSig || ma1Sig || ma2Sig;

    if (currentPos === 'Long') {
      if (firesExitLong) {
        firedSignal = firesExitLong;
        newPos = firesEntryShort ? 'Short' : 'Neutral';
        if (newPos === 'Neutral') logNeutral = true;
      }
    } else if (currentPos === 'Short') {
      if (firesExitShort) {
        firedSignal = firesExitShort;
        newPos = firesEntryLong ? 'Long' : 'Neutral';
        if (newPos === 'Neutral') logNeutral = true;
      }
    } else {
      if (firesEntryLong) {
        newPos = 'Long';
        firedSignal = firesEntryLong;
      } else if (firesEntryShort) {
        newPos = 'Short';
        firedSignal = firesEntryShort;
      }
    }

    if (newPos === currentPos) continue;
    const exec = nextTradeFromPriceData(sigDate, priceMap);

    if ((currentPos === 'Long' || currentPos === 'Short') && exec) {
      tradeID++;
      const rawDiff = Number(exec.prices.open) - tradeOpenPrice;
      const diffPct = tradeOpenPrice === 0 ? 0 : (rawDiff / tradeOpenPrice);
      const pnlPct = currentPos === 'Long' ? diffPct : -diffPct;
      accumPL1 *= (1 + pnlPct);
      const holdDays = tradeOpenDate ? Math.round((exec.date - tradeOpenDate) / 86400000) : 0;
      const odxVal = startPrice * accumPL1;
      const yearlyReturn = holdDays > 0 ? pnlPct / (holdDays / 365.25) : 0;
      const portVal = Math.round(initialPortfolio * accumPL1);
      const portPL = Math.round(initialPortfolio * accumPL1 - initialPortfolio);

      rows.push([
        tradeID,
        toDateString(tradeSignalOpenDate),
        Number(tradeSignalOpenCloseVal),
        currentPos === 'Long' ? 1 : '',
        currentPos === 'Short' ? 1 : '',
        tradeEntrySignal,
        currentPos.toLowerCase(),
        toDateString(tradeOpenDate),
        Number(tradeOpenPrice),
        toDateString(sigDate),
        Number(sigPriceData.close),
        currentPos === 'Short' ? 1 : '',
        currentPos === 'Long' ? 1 : '',
        firedSignal,
        toDateString(exec.date),
        Number(exec.prices.open),
        diffPct,
        pnlPct,
        holdDays,
        accumPL1,
        accumPL1 - 1,
        odxVal,
        odxVal,
        holdDays,
        holdDays / 30.4,
        holdDays / 365.25,
        yearlyReturn,
        '',
        portVal,
        portPL
      ]);
    }

    if (includeNeutralRows && logNeutral && currentPos !== null && exec) {
      rows.push([
        'N/A',
        toDateString(sigDate),
        Number(sigPriceData.close),
        '',
        '',
        '',
        'neutral',
        toDateString(exec.date),
        Number(exec.prices.open),
        toDateString(sigDate),
        Number(sigPriceData.close),
        '',
        '',
        firedSignal,
        toDateString(exec.date),
        Number(exec.prices.open),
        0,
        0,
        0,
        accumPL1,
        accumPL1 - 1,
        startPrice * accumPL1,
        startPrice * accumPL1,
        0,
        0,
        0,
        0,
        '',
        Math.round(initialPortfolio * accumPL1),
        0
      ]);
    }

    if ((newPos === 'Long' || newPos === 'Short') && exec) {
      tradeSignalOpenDate = sigDate;
      tradeSignalOpenCloseVal = Number(sigPriceData.close);
      tradeOpenDate = exec.date;
      tradeOpenPrice = Number(exec.prices.open);
      tradeEntrySignal = firedSignal;
    }

    currentPos = newPos;
  }

  rows.sort((a, b) => {
    const ai = Number.isFinite(Number(a[0])) ? Number(a[0]) : -1;
    const bi = Number.isFinite(Number(b[0])) ? Number(b[0]) : -1;
    return bi - ai;
  });

  const completedTrades = rows.filter(r => Number.isFinite(Number(r[0])) && Number(r[0]) > 0).length;
  const firstTradeRow = rows.find(r => Number.isFinite(Number(r[0])) && Number(r[0]) > 0);
  const stats = {
    completed_trades: completedTrades,
    final_portfolio: firstTradeRow ? Number(firstTradeRow[28]) : Number(initialPortfolio),
    portfolio_pnl: firstTradeRow ? Number(firstTradeRow[29]) : 0
  };

  return { rows, warning: null, completedTrades, stats };
}

// ── T-mode (execution = signal-day CLOSE, no T+1 lookup) ──────────────────
function buildSheetParityTradeRowsForTickerT({
  ticker,
  priceMap,
  signals,
  startDate,
  entryLongSet,
  exitLongSet,
  entryShortSet,
  exitShortSet,
  initialPortfolio,
  includeNeutralRows
}) {
  if (!priceMap || priceMap.size === 0) return { rows: [], warning: `No prices: ${ticker}`, completedTrades: 0 };
  if (!signals || signals.length === 0) return { rows: [], warning: `No signals: ${ticker}`, completedTrades: 0 };

  const startPriceData = priceMap.get(dateKeySimple(startDate)) || Array.from(priceMap.values())[0];
  if (!startPriceData || startPriceData.open == null) {
    return { rows: [], warning: `No start price: ${ticker}`, completedTrades: 0 };
  }

  const startPrice = Number(startPriceData.open);
  const rows = [];
  let currentPos = null;
  let tradeOpenDate = null;
  let tradeOpenClose = 0;
  let tradeSignalOpenDate = null;
  let tradeSignalOpenCloseVal = 0;
  let tradeEntrySignal = '';
  let accumPL1 = 1.0;
  let tradeID = 0;

  for (let i = 0; i < signals.length; i++) {
    const sigDate = signals[i].date;
    const oldSig = String(signals[i].oldSig || signals[i].signal || '').trim().toUpperCase();
    const ma1Sig = String(signals[i].ma1Sig || '').trim().toUpperCase();
    const ma2Sig = String(signals[i].ma2Sig || '').trim().toUpperCase();
    const sigPriceData = priceMap.get(dateKeySimple(sigDate));

    if (!sigPriceData) continue;
    if (!oldSig && !ma1Sig && !ma2Sig) continue;
    if (oldSig === 'NULL') continue;
    if (i > 0 && signals[i - 1].date.getTime() === sigDate.getTime()) continue;

    const firesEntryLong  = _anyFires(entryLongSet,  oldSig, ma1Sig, ma2Sig);
    const firesExitLong   = _anyFires(exitLongSet,   oldSig, ma1Sig, ma2Sig);
    const firesEntryShort = _anyFires(entryShortSet, oldSig, ma1Sig, ma2Sig);
    const firesExitShort  = _anyFires(exitShortSet,  oldSig, ma1Sig, ma2Sig);

    let newPos = currentPos;
    let logNeutral = false;
    let firedSignal = oldSig || ma1Sig || ma2Sig;

    if (currentPos === 'Long') {
      if (firesExitLong) {
        firedSignal = firesExitLong;
        newPos = firesEntryShort ? 'Short' : 'Neutral';
        if (newPos === 'Neutral') logNeutral = true;
      }
    } else if (currentPos === 'Short') {
      if (firesExitShort) {
        firedSignal = firesExitShort;
        newPos = firesEntryLong ? 'Long' : 'Neutral';
        if (newPos === 'Neutral') logNeutral = true;
      }
    } else {
      if (firesEntryLong) {
        newPos = 'Long';
        firedSignal = firesEntryLong;
      } else if (firesEntryShort) {
        newPos = 'Short';
        firedSignal = firesEntryShort;
      }
    }

    if (newPos === currentPos) continue;

    // T mode: execution price = signal-day close, no next-day lookup
    const execClose = Number(sigPriceData.close);
    const execDate  = sigDate;

    if (currentPos === 'Long' || currentPos === 'Short') {
      tradeID++;
      const rawDiff = execClose - tradeOpenClose;
      const diffPct = tradeOpenClose === 0 ? 0 : rawDiff / tradeOpenClose;
      const pnlPct  = currentPos === 'Long' ? diffPct : -diffPct;
      accumPL1 *= (1 + pnlPct);
      const holdDays = tradeOpenDate ? Math.round((execDate - tradeOpenDate) / 86400000) : 0;
      const odxVal   = startPrice * accumPL1;
      const yearlyReturn = holdDays > 0 ? pnlPct / (holdDays / 365.25) : 0;
      const portVal  = Math.round(initialPortfolio * accumPL1);
      const portPL   = Math.round(initialPortfolio * accumPL1 - initialPortfolio);

      rows.push([
        tradeID,
        toDateString(tradeSignalOpenDate),   // Signal Date (Entry-T)
        Number(tradeSignalOpenCloseVal),      // Signal Close (Entry-T)
        currentPos === 'Long'  ? 1 : '',     // BTO
        currentPos === 'Short' ? 1 : '',     // STO
        tradeEntrySignal,                    // Entry Signal
        currentPos.toLowerCase(),            // Trade Type
        Number(tradeOpenClose),              // Execution Close (Entry-T)
        toDateString(sigDate),               // Signal Date (Exit-T)
        Number(sigPriceData.close),          // Signal Close (Exit-T)
        currentPos === 'Short' ? 1 : '',     // BTC
        currentPos === 'Long'  ? 1 : '',     // STC
        firedSignal,                         // Exit Signal
        Number(execClose),                   // Execution Close (Exit-T)
        diffPct,
        pnlPct,
        holdDays,
        accumPL1,
        accumPL1 - 1,
        odxVal,
        odxVal,
        holdDays,
        holdDays / 30.4,
        holdDays / 365.25,
        yearlyReturn,
        '',
        portVal,
        portPL
      ]);
    }

    if (includeNeutralRows && logNeutral && currentPos !== null) {
      rows.push([
        'N/A',
        toDateString(sigDate),
        Number(sigPriceData.close),
        '', '', '', 'neutral',
        Number(execClose),
        toDateString(sigDate),
        Number(sigPriceData.close),
        '', '',
        firedSignal,
        Number(execClose),
        0, 0, 0,
        accumPL1, accumPL1 - 1,
        startPrice * accumPL1, startPrice * accumPL1,
        0, 0, 0, 0, '',
        Math.round(initialPortfolio * accumPL1), 0
      ]);
    }

    if (newPos === 'Long' || newPos === 'Short') {
      tradeSignalOpenDate     = sigDate;
      tradeSignalOpenCloseVal = Number(sigPriceData.close);
      tradeOpenDate           = execDate;
      tradeOpenClose          = execClose;
      tradeEntrySignal        = firedSignal;
    }

    currentPos = newPos;
  }

  rows.sort((a, b) => {
    const ai = Number.isFinite(Number(a[0])) ? Number(a[0]) : -1;
    const bi = Number.isFinite(Number(b[0])) ? Number(b[0]) : -1;
    return bi - ai;
  });

  const completedTrades = rows.filter(r => Number.isFinite(Number(r[0])) && Number(r[0]) > 0).length;
  const firstTradeRow   = rows.find(r => Number.isFinite(Number(r[0])) && Number(r[0]) > 0);
  const stats = {
    completed_trades: completedTrades,
    final_portfolio:  firstTradeRow ? Number(firstTradeRow[26]) : Number(initialPortfolio),
    portfolio_pnl:    firstTradeRow ? Number(firstTradeRow[27]) : 0
  };

  return { rows, warning: null, completedTrades, stats };
}

function processOdinSummaryForTicker(ticker, pMap, signals, startDate, entrySet, exitSet, initialPortfolio) {
  if (!pMap || pMap.size === 0) return { row: null, warning: `No prices: ${ticker}` };
  if (!signals || signals.length === 0) return { row: null, warning: `No signals: ${ticker}` };

  const startPrice = pMap.get(dateKeySimple(startDate)) || Array.from(pMap.values())[0];
  if (startPrice == null) return { row: null, warning: `No start price: ${ticker}` };

  let currentPos = null;
  let tradeOpenPrice = 0;
  let accumPL1 = 1.0;
  let firstExecOpen = null;
  let lastExecOpen = null;
  let firstODX = null;
  let lastODX = null;
  let completedTrades = 0;

  for (let i = 0; i < signals.length; i++) {
    const { date: sigDate, signal: rawSignal } = signals[i];
    let newPos;

    if (rawSignal.startsWith('L')) {
      if (currentPos === 'Short') newPos = entrySet.has(rawSignal) ? 'Long' : 'Neutral';
      else if (currentPos === 'Long') newPos = (exitSet.size > 0 && exitSet.has(rawSignal)) ? 'Neutral' : 'Long';
      else newPos = entrySet.has(rawSignal) ? 'Long' : 'Neutral';
    } else if (rawSignal.startsWith('S')) {
      if (currentPos === 'Long') newPos = entrySet.has(rawSignal) ? 'Short' : 'Neutral';
      else if (currentPos === 'Short') newPos = (exitSet.size > 0 && exitSet.has(rawSignal)) ? 'Neutral' : 'Short';
      else newPos = entrySet.has(rawSignal) ? 'Short' : 'Neutral';
    } else {
      newPos = 'Neutral';
    }

    if (newPos === currentPos) continue;
    const exec = nextTradeOpen(sigDate, pMap);

    if ((currentPos === 'Long' || currentPos === 'Short') && exec) {
      completedTrades++;
      // Keep parity with sheet summary script logic.
      const diff = exec.open - tradeOpenPrice;
      const pnlPct = tradeOpenPrice === 0 ? 0 : diff / tradeOpenPrice;
      accumPL1 *= (1 + pnlPct);
      const odxVal = startPrice * accumPL1;

      if (firstExecOpen === null) {
        firstExecOpen = tradeOpenPrice;
        firstODX = odxVal;
      }
      lastExecOpen = exec.open;
      lastODX = odxVal;
    }

    if ((newPos === 'Long' || newPos === 'Short') && exec) {
      tradeOpenPrice = exec.open;
      if (firstExecOpen === null) {
        firstExecOpen = exec.open;
        firstODX = startPrice * accumPL1;
      }
    }

    currentPos = newPos;
  }

  if (firstExecOpen === null || lastExecOpen === null) {
    return { row: null, warning: `No completed trades: ${ticker}` };
  }

  const returnPct = (lastExecOpen - firstExecOpen) / firstExecOpen;
  const returnODXPct = (firstODX && firstODX !== 0 && lastODX != null) ? (lastODX - firstODX) / firstODX : null;
  const portVal = initialPortfolio * (1 + returnPct);
  const portPL = portVal - initialPortfolio;

  return {
    row: {
      Ticker: ticker,
      'Completed Trades': completedTrades,
      'First Execution Open': Number(firstExecOpen.toFixed(2)),
      'Last Execution Open': Number(lastExecOpen.toFixed(2)),
      'Return %': returnPct,
      'First ODX': firstODX != null ? Number(firstODX.toFixed(4)) : null,
      'Last ODX': lastODX != null ? Number(lastODX.toFixed(4)) : null,
      'Return % ODX': returnODXPct,
      'Sample portfolio ($)': Math.round(portVal),
      'Sample portfolio P&L ($)': Math.round(portPL)
    },
    warning: null
  };
}

/**
 * POST /api/analytics/odin-summary
 * Body:
 * {
 *   tickers?: string[] | string,
 *   groups?: string[] | string,
 *   start_date: string,
 *   end_date: string,
 *   initial_portfolio?: number,
 *   entry_signals: string[] | string,
 *   exit_signals?: string[] | string
 * }
 */
const runOdinSummary = async (req, res) => {
  const body = req.body || {};
  const startDateStr = (body.start_date || '').toString().trim();
  const endDateStr = (body.end_date || '').toString().trim();
  const tickersIn = body.tickers;
  const groupsIn = body.groups;
  const entrySignals = toList(body.entry_signals).map(s => s.toUpperCase());
  const exitSignals = toList(body.exit_signals).map(s => s.toUpperCase());
  let initialPortfolio = body.initial_portfolio;
  if (initialPortfolio == null || initialPortfolio === '') initialPortfolio = 1000;
  initialPortfolio = Number(initialPortfolio);

  if (!startDateStr || !endDateStr) return res.status(400).json({ error: 'start_date and end_date are required' });
  if (!Number.isFinite(initialPortfolio) || initialPortfolio <= 0) return res.status(400).json({ error: 'initial_portfolio must be a positive number' });
  if (entrySignals.length === 0) return res.status(400).json({ error: 'entry_signals is required and cannot be empty' });
  if (toList(tickersIn).length === 0 && toList(groupsIn).length === 0) {
    return res.status(400).json({ error: 'Provide at least one ticker or one group' });
  }

  const startDate = new Date(startDateStr);
  const endDate = new Date(endDateStr);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return res.status(400).json({ error: 'Invalid start_date or end_date format' });
  }
  if (startDate > endDate) return res.status(400).json({ error: 'start_date must be before or equal to end_date' });

  try {
    const summaryCacheKey = makeCacheKey('analytics:odin-summary:v1', {
      startDateStr,
      endDateStr,
      tickersIn: toList(tickersIn).map((t) => String(t).toUpperCase()).sort(),
      groupsIn: toList(groupsIn).map((g) => String(g).toLowerCase()).sort(),
      entrySignals: [...entrySignals].sort(),
      exitSignals: [...exitSignals].sort(),
      initialPortfolio
    });
    const cachedSummary = await getCache(summaryCacheKey);
    if (cachedSummary) {
      res.set('X-Cache-Hit', '1');
      return res.status(200).json({ ...cachedSummary, cache_hit: true });
    }

    const { resolvedTickers, warnings: resolveWarnings, explicitTickers, requestedGroups } =
      await resolveTickerUniverseForSummary(tickersIn, groupsIn);
    if (resolvedTickers.length === 0) {
      return res.status(400).json({ error: 'No tickers resolved from provided groups/tickers', warnings: resolveWarnings });
    }

    const startSql = startDateStr;
    const endSql = endDateStr;
    const [priceRows, signalRows] = await Promise.all([
      fetchSummaryBatchPrices(resolvedTickers, startSql, endSql),
      fetchSummaryBatchSignals(resolvedTickers, startSql, endSql)
    ]);

    const priceMaps = new Map();
    for (const r of priceRows) {
      const t = String(r.Ticker || '').toUpperCase();
      const d = parseDateLike(r.Date);
      if (!t || !d || r.Open == null) continue;
      if (!priceMaps.has(t)) priceMaps.set(t, new Map());
      priceMaps.get(t).set(dateKeySimple(d), Number(r.Open));
    }

    const signalMap = new Map();
    for (const r of signalRows) {
      const t = String(r.Ticker || '').toUpperCase();
      const d = parseDateLike(r.Date);
      const s = String(r.Signal || '').trim().toUpperCase();
      if (!t || !d || !s) continue;
      if (!signalMap.has(t)) signalMap.set(t, []);
      signalMap.get(t).push({ date: d, signal: s });
    }
    for (const t of signalMap.keys()) {
      signalMap.get(t).sort((a, b) => a.date - b.date);
    }

    const entrySet = new Set(entrySignals);
    const exitSet = new Set(exitSignals);
    const summaryRows = [];
    const warnings = [...resolveWarnings];

    for (const ticker of resolvedTickers) {
      const { row, warning } = processOdinSummaryForTicker(
        ticker,
        priceMaps.get(ticker),
        signalMap.get(ticker),
        startDate,
        entrySet,
        exitSet,
        initialPortfolio
      );
      if (warning) warnings.push(warning);
      if (row) {
        summaryRows.push({
          Ticker: row.Ticker,
          'Start Date': startSql,
          'End Date': endSql,
          'Entry Signals': entrySignals.join(','),
          'Exit Signals': exitSignals.length ? exitSignals.join(',') : 'direction-change',
          'Completed Trades': row['Completed Trades'],
          'First Execution Open': row['First Execution Open'],
          'Last Execution Open': row['Last Execution Open'],
          'Return %': row['Return %'],
          'First ODX': row['First ODX'],
          'Last ODX': row['Last ODX'],
          'Return % ODX': row['Return % ODX'],
          'Sample portfolio ($)': row['Sample portfolio ($)'],
          'Sample portfolio P&L ($)': row['Sample portfolio P&L ($)']
        });
      }
    }

    const summaryHeaders = [
      'Ticker', 'Start Date', 'End Date', 'Entry Signals', 'Exit Signals',
      'Completed Trades', 'First Execution Open', 'Last Execution Open', 'Return %',
      'First ODX', 'Last ODX', 'Return % ODX', 'Sample portfolio ($)', 'Sample portfolio P&L ($)'
    ];

    const responsePayload = {
      success: true,
      cache_hit: false,
      config: {
        start_date: startSql,
        end_date: endSql,
        initial_portfolio: initialPortfolio,
        entry_signals: entrySignals,
        exit_signals: exitSignals,
        resolved_ticker_count: resolvedTickers.length
      },
      summary_headers: summaryHeaders,
      summary_rows: summaryRows,
      universe: {
        input_tickers: explicitTickers,
        input_groups: requestedGroups,
        resolved_tickers: resolvedTickers
      },
      warnings
    };
    await setCache(summaryCacheKey, responsePayload, ODIN_SUMMARY_CACHE_TTL_SECS);
    res.set('X-Cache-Hit', '0');
    res.status(200).json(responsePayload);
  } catch (err) {
    console.error('Odin Summary error:', err);
    res.status(500).json({ error: 'Failed to generate Odin summary', message: err.message });
  }
};

const runOdinIndex = async (req, res) => {
  const body = req.body || {};
  const startDateStr = (body.start_date || '').toString().trim();
  const endDateStr = (body.end_date || '').toString().trim();
  const groupsIn = body.groups;
  const includeNeutralRows = body.include_neutral_rows !== false;
  const chunkSize = Math.max(1, Math.min(Number(body.chunk_size) || 30, 100));
  // 'T' = execution at signal-day close; 'T+1' = execution at next-day open (default)
  const executionMode = String(body.execution_mode || 'T+1').trim().toUpperCase() === 'T' ? 'T' : 'T+1';
  const legacyEntrySignals = toList(body.entry_signals).map(s => s.toUpperCase());
  const legacyExitSignals = toList(body.exit_signals).map(s => s.toUpperCase());
  let entryLongSignals = toList(body.entry_long_signals).map(s => s.toUpperCase());
  let exitLongSignals = toList(body.exit_long_signals).map(s => s.toUpperCase());
  let entryShortSignals = toList(body.entry_short_signals).map(s => s.toUpperCase());
  let exitShortSignals = toList(body.exit_short_signals).map(s => s.toUpperCase());
  if (entryLongSignals.length === 0 && entryShortSignals.length === 0 && legacyEntrySignals.length > 0) {
    entryLongSignals = [...legacyEntrySignals];
    entryShortSignals = [...legacyEntrySignals];
  }
  if (exitLongSignals.length === 0 && exitShortSignals.length === 0 && legacyExitSignals.length > 0) {
    exitLongSignals = [...legacyExitSignals];
    exitShortSignals = [...legacyExitSignals];
  }
  const explicitTickerFromLegacyField = (body.ticker || '').toString().trim().toUpperCase();
  const tickersIn = body.tickers != null
    ? body.tickers
    : (explicitTickerFromLegacyField ? [explicitTickerFromLegacyField] : []);

  let initialPortfolio = body.initial_portfolio;
  if (initialPortfolio == null || initialPortfolio === '') initialPortfolio = 1000;
  initialPortfolio = Number(initialPortfolio);
  if (!Number.isFinite(initialPortfolio) || initialPortfolio <= 0) {
    return res.status(400).json({ error: 'initial_portfolio must be a positive number' });
  }
  if (!startDateStr || !endDateStr) {
    return res.status(400).json({ error: 'start_date and end_date are required' });
  }
  if (entryLongSignals.length === 0 && entryShortSignals.length === 0) {
    return res.status(400).json({
      error: 'Provide entry_long_signals and/or entry_short_signals (or legacy entry_signals)'
    });
  }
  if (toList(tickersIn).length === 0 && toList(groupsIn).length === 0) {
    return res.status(400).json({ error: 'Provide at least one ticker or one group' });
  }

  let startDate;
  let endDate;
  try {
    startDate = new Date(startDateStr);
    endDate = new Date(endDateStr);
    if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) throw new Error('Invalid date');
    if (startDate > endDate) {
      return res.status(400).json({ error: 'start_date must be before or equal to end_date' });
    }
  } catch (e) {
    return res.status(400).json({ error: 'Invalid start_date or end_date format' });
  }

  try {
    const indexCacheKey = makeCacheKey('analytics:odin-index:v5', {
      startDateStr,
      endDateStr,
      tickersIn: toList(tickersIn).map((t) => String(t).toUpperCase()).sort(),
      groupsIn: toList(groupsIn).map((g) => String(g).toLowerCase()).sort(),
      entryLongSignals: [...entryLongSignals].sort(),
      exitLongSignals: [...exitLongSignals].sort(),
      entryShortSignals: [...entryShortSignals].sort(),
      exitShortSignals: [...exitShortSignals].sort(),
      initialPortfolio,
      includeNeutralRows,
      chunkSize,
      executionMode
    });
    const cachedIndex = await getCache(indexCacheKey);
    if (cachedIndex) {
      res.set('X-Cache-Hit', '1');
      return res.status(200).json({ ...cachedIndex, cache_hit: true });
    }

    const { resolvedTickers, warnings: resolveWarnings, explicitTickers, requestedGroups } =
      await resolveTickerUniverseForSummary(tickersIn, groupsIn);
    if (resolvedTickers.length === 0) {
      return res.status(400).json({ error: 'No tickers resolved from provided groups/tickers', warnings: resolveWarnings });
    }

    const headers = executionMode === 'T'
      ? [
          '#', 'Signal Date (Entry-T)', 'Signal Close (Entry-T)', 'BTO', 'STO', 'Entry Signal', 'Trade Type',
          'Execution Close (Entry-T)',
          'Signal Date (Exit-T)', 'Signal Close (Exit-T)', 'BTC', 'STC', 'Exit Signal',
          'Execution Close (Exit-T)',
          'Difference (%)', 'P&L (%)', 'Hold Days', 'Accum P&L 1', 'Accum P&L 2',
          'ODX', 'ODX', 'Days Count', 'Months Count', 'Years Count',
          'Av. Yearly Return', '', 'Port Val ($)', 'Port P&L ($)'
        ]
      : [
          '#', 'Signal Date (Entry-T)', 'Signal Close (Entry-T)', 'BTO', 'STO', 'Entry Signal', 'Trade Type',
          'Execution Date (Entry-T+1)', 'Execution Open (Entry-T+1)',
          'Signal Date (Exit-T)', 'Signal Close (Exit-T)', 'BTC', 'STC', 'Exit Signal',
          'Execution Date (Exit-T+1)', 'Execution Open (Exit-T+1)',
          'Difference (%)', 'P&L (%)', 'Hold Days', 'Accum P&L 1', 'Accum P&L 2',
          'ODX', 'ODX', 'Days Count', 'Months Count', 'Years Count',
          'Av. Yearly Return', '', 'Port Val ($)', 'Port P&L ($)'
        ];

    const entryLongSet = new Set(entryLongSignals);
    const exitLongSet = new Set(exitLongSignals);
    const entryShortSet = new Set(entryShortSignals);
    const exitShortSet = new Set(exitShortSignals);
    const warnings = [...resolveWarnings];
    const resultsByTicker = [];

    for (let i = 0; i < resolvedTickers.length; i += chunkSize) {
      const chunk = resolvedTickers.slice(i, i + chunkSize);
      const [priceRows, signalRows] = await Promise.all([
        fetchIndexBatchPrices(chunk, startDateStr, endDateStr),
        fetchIndexBatchSignals(chunk, startDateStr, endDateStr)
      ]);

      const priceMaps = new Map();
      for (const r of priceRows) {
        const t = String(r.Ticker || '').toUpperCase();
        const d = parseDateLike(r.Date);
        if (!t || !d || r.Open == null) continue;
        if (!priceMaps.has(t)) priceMaps.set(t, new Map());
        priceMaps.get(t).set(dateKeySimple(d), {
          open: Number(r.Open),
          close: r.Close != null ? Number(r.Close) : Number(r.Open)
        });
      }

      const signalMap = new Map();
      for (const r of signalRows) {
        const t = String(r.Ticker || '').toUpperCase();
        const d = parseDateLike(r.Date);
        if (!t || !d) continue;
        const oldSig = String(r.old_signal || r.Signal || '').trim().toUpperCase();
        const ma1Sig = String(r.ma1_signal || '').trim().toUpperCase();
        const ma2Sig = String(r.ma2_signal || '').trim().toUpperCase();
        if (!oldSig && !ma1Sig && !ma2Sig) continue;
        if (!signalMap.has(t)) signalMap.set(t, []);
        signalMap.get(t).push({ date: d, oldSig, ma1Sig, ma2Sig });
      }
      for (const t of signalMap.keys()) {
        signalMap.get(t).sort((a, b) => a.date - b.date);
      }

      for (const ticker of chunk) {
        const buildFn = executionMode === 'T'
          ? buildSheetParityTradeRowsForTickerT
          : buildSheetParityTradeRowsForTicker;
        const built = buildFn({
          ticker,
          priceMap: priceMaps.get(ticker),
          signals: signalMap.get(ticker),
          startDate,
          entryLongSet,
          exitLongSet,
          entryShortSet,
          exitShortSet,
          initialPortfolio,
          includeNeutralRows
        });
        if (built.warning) warnings.push(built.warning);
        resultsByTicker.push({
          ticker,
          row_count: built.rows.length,
          rows: built.rows,
          stats: built.stats || {
            completed_trades: 0,
            final_portfolio: initialPortfolio,
            portfolio_pnl: 0
          }
        });
      }
    }

    const responsePayload = {
      success: true,
      cache_hit: false,
      config: {
        start_date: startDateStr,
        end_date: endDateStr,
        initial_portfolio: initialPortfolio,
        entry_long_signals: entryLongSignals,
        exit_long_signals: exitLongSignals,
        entry_short_signals: entryShortSignals,
        exit_short_signals: exitShortSignals,
        entry_signals: [...new Set([...entryLongSignals, ...entryShortSignals])],
        exit_signals: [...new Set([...exitLongSignals, ...exitShortSignals])],
        include_neutral_rows: includeNeutralRows,
        chunk_size: chunkSize,
        execution_mode: executionMode,
        resolved_ticker_count: resolvedTickers.length
      },
      headers,
      results_by_ticker: resultsByTicker,
      universe: {
        input_tickers: explicitTickers,
        input_groups: requestedGroups,
        resolved_tickers: resolvedTickers
      },
      warnings
    };
    await setCache(indexCacheKey, responsePayload, ODIN_INDEX_CACHE_TTL_SECS);
    res.set('X-Cache-Hit', '0');
    res.status(200).json(responsePayload);
  } catch (err) {
    console.error('Odin Index error:', err);
    res.status(500).json({
      error: 'Failed to run Odin Index',
      message: err.message
    });
  }
};

module.exports = { runOdinIndex, runOdinSummary };
