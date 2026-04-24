const bigquery = require('./config/bigquery');
const supabase = require('./config/supabase');
const fs = require('fs');
const path = require('path');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || 'extended-byway-454621-s6';
const BIGQUERY_DATASET = process.env.BIGQUERY_DATASET || 'sp500data1';
const TICKER_DETAILS_TABLE = process.env.TICKER_DETAILS_TABLE || 'TickerDetails';
const BIGQUERY_TABLE = process.env.BIGQUERY_TABLE || 'stock_all_data';
const SIGNALS_TABLE = process.env.BIGQUERY_SIGNALS_TABLE || 'Test';
const TICKER_DETAILS_SYMBOL_COLUMN =
  (process.env.TICKER_DETAILS_SYMBOL_COLUMN || 'Symbol').trim() || 'Symbol';
const ENABLE_RELATIVE_VOLUME_QUERY = process.env.ENABLE_RELATIVE_VOLUME_QUERY === '1';
const SNAPSHOT_DATASET = process.env.BIGQUERY_SNAPSHOT_DATASET || BIGQUERY_DATASET;
const TICKER_DETAILS_SNAPSHOT_TABLE =
  process.env.TICKER_DETAILS_SNAPSHOT_TABLE || 'ticker_details_snapshot';
const INDEX_MOVERS_SNAPSHOT_TABLE =
  process.env.INDEX_MARKET_MOVERS_SNAPSHOT_TABLE || 'index_market_movers_snapshot';
const INDEX_RETURNS_SNAPSHOT_TABLE =
  process.env.INDEX_RETURNS_SNAPSHOT_TABLE || 'index_returns_snapshot';
const WEIGHTS_JSON_PATH = path.resolve(__dirname, 'data', 'index-weights.json');

const TICKER_DETAILS_FQN = `${PROJECT_ID}.${BIGQUERY_DATASET}.${TICKER_DETAILS_TABLE}`;
const TABLE_FQN = `${PROJECT_ID}.${BIGQUERY_DATASET}.${BIGQUERY_TABLE}`;
const SIGNALS_TABLE_FQN = `${PROJECT_ID}.${BIGQUERY_DATASET}.${SIGNALS_TABLE}`;
const TICKER_DETAILS_SNAPSHOT_FQN =
  `${PROJECT_ID}.${SNAPSHOT_DATASET}.${TICKER_DETAILS_SNAPSHOT_TABLE}`;
const INDEX_MOVERS_SNAPSHOT_FQN =
  `${PROJECT_ID}.${SNAPSHOT_DATASET}.${INDEX_MOVERS_SNAPSHOT_TABLE}`;
const INDEX_RETURNS_SNAPSHOT_FQN =
  `${PROJECT_ID}.${SNAPSHOT_DATASET}.${INDEX_RETURNS_SNAPSHOT_TABLE}`;
/** API `index` body values for POST /index-returns (same as IndexPage). */
const SNAPSHOT_INDEX_RETURNS_KEYS = ['sp500', 'Dow Jones', 'Nasdaq 100'];
const SNAPSHOT_INDEX_RETURNS_KEY_SET = new Set(
  SNAPSHOT_INDEX_RETURNS_KEYS.map((k) => String(k).trim().toLowerCase())
);
const SNAPSHOT_SUPPORTED_PERIODS = ['last-date', 'last-5-days', 'mtd'];
const SNAPSHOT_SUPPORTED_INDICES = ['SP500', 'Dow Jones', 'Nasdaq 100'];

const DAYS_IN_YEAR = 365.25;
const MAX_DATE_CACHE_TTL_MS = Number(process.env.MAX_DATE_CACHE_TTL_MS || 60000);
let maxDateCache = { value: null, ts: 0 };
let indexWeightsCache = null;
let indexWeightsCacheMtime = 0;

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function parseBqDate(v) {
  const raw = v && (v.value || v);
  const dt = raw ? new Date(raw) : null;
  return dt && !Number.isNaN(dt.getTime()) ? dt : null;
}

async function getMaxDate() {
  const now = Date.now();
  if (
    maxDateCache.value &&
    Number.isFinite(maxDateCache.ts) &&
    now - maxDateCache.ts < MAX_DATE_CACHE_TTL_MS
  ) {
    return new Date(maxDateCache.value);
  }
  const query = `SELECT MAX(Date) AS max_date FROM \`${TABLE_FQN}\``;
  const [rows] = await bigquery.query({ query });
  const dt = rows && rows[0] ? parseBqDate(rows[0].max_date) : null;
  if (!dt) throw new Error('No dates found in price table.');
  maxDateCache = { value: dt.toISOString(), ts: now };
  return dt;
}

function getPeriodOptions() {
  const periodDefs = [
    { name: 'Last date', value: 'last-date' },
    { name: 'Week', value: 'week' },
    { name: 'Last Month', value: 'last-month' },
    { name: 'Last 3 months', value: 'last-3-months' },
    { name: 'Last 6 months', value: 'last-6-months' },
    { name: 'Year to Date (YTD)', value: 'ytd' },
    { name: 'Last 1 year', value: 'last-1-year' },
    { name: 'Last 2 years', value: 'last-2-years' },
    { name: 'Last 3 years', value: 'last-3-years' },
    { name: 'Last 5 years', value: 'last-5-years' },
    { name: 'Last 10 years', value: 'last-10-years' },
    { name: 'Last 20 years', value: 'last-20-years' },
  ];
  return periodDefs.map(p => ({ value: p.value, label: p.name }));
}

/**
 * Start of rolling window for `periodValue`, relative to `endDate` (same semantics as ticker-details).
 * Exported for index constituent leaderboards (single BQ fetch, many windows).
 */
function computeRollingWindowStart(endDate, periodValue) {
  const startDate = new Date(endDate);
  switch ((periodValue || '').toLowerCase()) {
    case 'last-date':
      startDate.setDate(endDate.getDate() - 1);
      break;
    /** ~5 calendar days ending at endDate (distinct from calendar week). */
    case 'last-5-days':
      startDate.setDate(endDate.getDate() - 5);
      break;
    /** First calendar day of month containing endDate. */
    case 'mtd':
      startDate.setFullYear(endDate.getFullYear(), endDate.getMonth(), 1);
      startDate.setHours(0, 0, 0, 0);
      break;
    /** First calendar day of quarter containing endDate. */
    case 'qtd': {
      const m = endDate.getMonth();
      const qStartMonth = Math.floor(m / 3) * 3;
      startDate.setFullYear(endDate.getFullYear(), qStartMonth, 1);
      startDate.setHours(0, 0, 0, 0);
      break;
    }
    /** Long history cap (~25y) so BigQuery stays tractable for full-index fetch. */
    case 'all-available':
      startDate.setFullYear(endDate.getFullYear() - 25);
      break;
    case 'week':
      startDate.setDate(endDate.getDate() - 7);
      break;
    case 'last-month':
      startDate.setDate(endDate.getDate() - 30);
      break;
    case 'last-3-months':
      startDate.setDate(endDate.getDate() - 91);
      break;
    case 'last-6-months':
      startDate.setDate(endDate.getDate() - 183);
      break;
    case 'ytd':
      startDate.setMonth(0, 1);
      startDate.setHours(0, 0, 0, 0);
      break;
    case 'last-2-years':
      startDate.setDate(endDate.getDate() - Math.floor(2 * 365));
      break;
    case 'last-3-years':
      startDate.setDate(endDate.getDate() - Math.floor(3 * 365));
      break;
    case 'last-5-years':
      startDate.setDate(endDate.getDate() - Math.floor(5 * 365));
      break;
    case 'last-10-years':
      startDate.setDate(endDate.getDate() - Math.floor(10 * 365));
      break;
    case 'last-20-years':
      startDate.setDate(endDate.getDate() - Math.floor(20 * 365));
      break;
    case 'last-1-year':
    default:
      startDate.setDate(endDate.getDate() - Math.floor(1 * 365));
      break;
  }
  return startDate;
}

async function calculatePeriodDates(periodValue) {
  const endDate = await getMaxDate();
  const startDate = computeRollingWindowStart(endDate, periodValue);
  return [startDate, endDate];
}

async function getUniqueIndices() {
  const { data, error } = await supabase
    .from('market_groups')
    .select('name')
    .order('id', { ascending: true });
  if (error) throw error;
  return (data || []).map((r) => r.name).filter(Boolean);
}

async function fetchMarketGroupsWithCodes() {
  const { data, error } = await supabase
    .from('market_groups')
    .select('id, name, code')
    .order('id', { ascending: true });
  if (error) throw error;
  return data || [];
}

function normIndexKey(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeTickerFromWeightCell(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  const md = s.match(/^\[([A-Za-z0-9.\-]+)\]\s*\(/);
  if (md) return md[1].toUpperCase();
  return s.replace(/[^A-Za-z0-9.\-]/g, '').toUpperCase() || s.toUpperCase().trim();
}

function indexToWeightSource(indexValue) {
  const k = normIndexKey(indexValue);
  if (k === 'dow jones' || k === 'dow' || k === 'dow jones 30' || k === 'djia') return 'dowjones';
  if (k === 'sp500' || k === 's&p 500' || k === 's&p500' || k === 'sp 500') return 'sp500';
  if (k === 'nasdaq 100' || k === 'nasdaq100' || k === 'ndx' || k === 'nasdaq-100') return 'nasdaq100';
  if (k === 'all stocks') return 'sp500';
  return null;
}

function loadIndexWeights() {
  try {
    const st = fs.statSync(WEIGHTS_JSON_PATH);
    if (indexWeightsCache && indexWeightsCacheMtime === st.mtimeMs) return indexWeightsCache;
    const raw = fs.readFileSync(WEIGHTS_JSON_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    indexWeightsCache = parsed;
    indexWeightsCacheMtime = st.mtimeMs;
    return parsed;
  } catch {
    return null;
  }
}

function buildWeightMapForIndex(indexValue, tickerSymbols) {
  const syms = [...new Set((tickerSymbols || []).map((s) => String(s || '').toUpperCase().trim()).filter(Boolean))];
  const sourceKey = indexToWeightSource(indexValue);
  const allWeights = loadIndexWeights();
  const out = new Map();
  if (!allWeights?.sources || !sourceKey || !Array.isArray(allWeights.sources[sourceKey])) {
    for (const s of syms) out.set(s, 1);
    return out;
  }
  const bySymbol = new Map();
  for (const r of allWeights.sources[sourceKey]) {
    const sym = normalizeTickerFromWeightCell(r.symbol);
    const w = Number(r.weight);
    if (!sym || !Number.isFinite(w) || w <= 0) continue;
    bySymbol.set(sym, w);
  }
  for (const s of syms) {
    const w = Number(bySymbol.get(s));
    out.set(s, Number.isFinite(w) && w > 0 ? w : 1);
  }
  return out;
}

function resolveMarketGroupFromIndexValue(indexValue, groups) {
  const groupsArr = groups || [];
  if (!groupsArr.length) return null;
  const raw = String(indexValue || '').trim();
  if (!raw) return null;
  const n = normIndexKey(raw);
  const up = raw.toUpperCase();

  for (const g of groupsArr) {
    if (g.code && String(g.code).toUpperCase() === up) return g;
  }

  const aliasToCode = {
    's&p 500': 'SP',
    sp500: 'SP',
    'sp 500': 'SP',
    's&p500': 'SP',
    'dow jones 30': 'DJ',
    'dow jones': 'DJ',
    dow: 'DJ',
    djia: 'DJ',
    'nasdaq 100': 'ND',
    nasdaq100: 'ND',
    ndx: 'ND',
    etf: 'ETF',
    other: 'OTH'
  };
  const code = aliasToCode[n] || aliasToCode[raw.toLowerCase().replace(/\s+/g, ' ')];
  if (code) {
    const found = groupsArr.find((x) => (x.code || '').toUpperCase() === code);
    if (found) return found;
  }

  for (const g of groupsArr) {
    if (normIndexKey(g.name) === n) return g;
  }
  const compact = (s) => s.replace(/[^a-z0-9]/gi, '');
  const nc = compact(n);
  for (const g of groupsArr) {
    if (compact(normIndexKey(g.name)) === nc) return g;
  }
  return null;
}

/**
 * Tickers in a market group from Supabase (symbol + company_name).
 * Uses ticker_id → tickers first; falls back to embedded tickers(...) with array-shaped rows.
 */
async function getMembersForMarketGroupId(groupId) {
  const { data: links, error: linkErr } = await supabase
    .from('ticker_groups')
    .select('ticker_id')
    .eq('group_id', groupId);

  if (!linkErr && links?.length) {
    const ids = [...new Set(links.map((l) => l.ticker_id).filter(Boolean))];
    if (ids.length) {
      const { data: ticks, error: tickErr } = await supabase
        .from('tickers')
        .select('symbol, company_name')
        .in('id', ids);
      if (tickErr) {
        console.error('getMembersForMarketGroupId tickers:', tickErr);
      } else if (ticks?.length) {
        const seen = new Set();
        const out = [];
        for (const t of ticks) {
          const sym = String(t.symbol || '').trim();
          if (!sym) continue;
          const u = sym.toUpperCase();
          if (seen.has(u)) continue;
          seen.add(u);
          out.push({ symbol: sym, company_name: t.company_name || '' });
        }
        return out;
      }
    }
  } else if (linkErr) {
    console.error('getMembersForMarketGroupId ticker_groups:', linkErr);
  }

  const { data: nested, error: nestErr } = await supabase
    .from('ticker_groups')
    .select('tickers(symbol, company_name)')
    .eq('group_id', groupId);
  if (nestErr) {
    console.error('getMembersForMarketGroupId nested:', nestErr);
    return [];
  }
  const seen = new Set();
  const out = [];
  for (const row of nested || []) {
    const t = row.tickers;
    const pushOne = (x) => {
      const sym = x && x.symbol ? String(x.symbol).trim() : '';
      if (!sym) return;
      const u = sym.toUpperCase();
      if (seen.has(u)) return;
      seen.add(u);
      out.push({ symbol: sym, company_name: (x && x.company_name) || '' });
    };
    if (Array.isArray(t)) {
      for (const x of t) pushOne(x);
    } else {
      pushOne(t);
    }
  }
  return out;
}

/** BigQuery row shape varies (Symbol vs symbol); normalize for lookups. */
function normalizeTickerDetailRow(row) {
  if (!row || typeof row !== 'object') return null;
  const sym = row.Symbol ?? row.symbol ?? row.Ticker ?? row.ticker;
  if (sym == null || String(sym).trim() === '') return null;
  return {
    Symbol: String(sym).trim(),
    Security: String(row.Security ?? row.security ?? '').trim(),
    Sector: String(row.Sector ?? row.sector ?? '').trim(),
    Industry: String(row.Industry ?? row.industry ?? '').trim(),
    Index: String(row.Index ?? row.index ?? '').trim()
  };
}

function detailRowScore(d) {
  if (!d) return 0;
  let n = 0;
  if (d.Security) n += 4;
  if (d.Sector) n += 2;
  if (d.Industry) n += 2;
  if (d.Index) n += 1;
  return n;
}

/** One best row per symbol (TickerDetails can have multiple Index rows per ticker). */
function mergeTickerDetailRowsBySymbol(rawRows) {
  const map = new Map();
  for (const r of rawRows || []) {
    const d = normalizeTickerDetailRow(r);
    if (!d) continue;
    const key = d.Symbol.toUpperCase();
    const prev = map.get(key);
    if (!prev || detailRowScore(d) > detailRowScore(prev)) {
      map.set(key, d);
    }
  }
  return map;
}

function buildTickerDetailsByColumnQuery(symbolColumn) {
  const col = /^[A-Za-z_][A-Za-z0-9_]*$/.test(symbolColumn) ? symbolColumn : 'Symbol';
  return (tickersParam) => `
    SELECT \`${col}\` AS Symbol, Security, Sector, Industry, \`Index\`
    FROM \`${TICKER_DETAILS_FQN}\`
    WHERE UPPER(TRIM(CAST(\`${col}\` AS STRING))) IN (${tickersParam})
  `;
}

/**
 * TickerDetails may use Symbol or Ticker (like stock_all_data); ETFs are often keyed one way or missing.
 * Query both columns when possible and merge rows.
 */
async function fetchTickerDetailsRowsBySymbols(symbols) {
  const unique = [
    ...new Set(
      symbols
        .map((s) => String(s || '').toUpperCase().trim())
        .filter(Boolean)
    )
  ];
  if (!unique.length) return [];
  const tickersParam = unique.map((t) => `'${t.replace(/'/g, "\\'")}'`).join(', ');
  const columnsToTry = [TICKER_DETAILS_SYMBOL_COLUMN];

  let combined = [];
  for (const col of columnsToTry) {
    try {
      const sql = buildTickerDetailsByColumnQuery(col)(tickersParam);
      const [chunk] = await bigquery.query({ query: sql });
      if (chunk && chunk.length) {
        combined = combined.concat(chunk);
      }
    } catch (e) {
      console.warn(`TickerDetails lookup by column "${col}":`, e.message || e);
    }
  }

  const merged = mergeTickerDetailRowsBySymbol(combined);
  return [...merged.values()].sort((a, b) =>
    a.Symbol.localeCompare(b.Symbol, undefined, { sensitivity: 'base' })
  );
}

/** Fill empty Security from Supabase `tickers.company_name`. */
async function backfillSecurityFromSupabase(rows) {
  const need = rows.filter((r) => !String(r.Security || '').trim());
  if (!need.length) return rows;
  const syms = [...new Set(need.map((r) => String(r.Symbol || '').toUpperCase().trim()).filter(Boolean))];
  if (!syms.length) return rows;

  const variants = [...new Set(syms.flatMap((s) => [s, s.toLowerCase()]))];

  const byUpper = new Map();
  const chunkSize = 120;
  for (let i = 0; i < variants.length; i += chunkSize) {
    const slice = variants.slice(i, i + chunkSize);
    const { data, error } = await supabase
      .from('tickers')
      .select('symbol, company_name')
      .in('symbol', slice);
    if (error) {
      console.error('backfillSecurityFromSupabase:', error);
      return rows;
    }
    for (const t of data || []) {
      const u = String(t.symbol || '')
        .toUpperCase()
        .trim();
      if (!u) continue;
      const name = String(t.company_name || '').trim();
      if (name) byUpper.set(u, name);
    }
  }

  return rows.map((r) => {
    const u = String(r.Symbol || '')
      .toUpperCase()
      .trim();
    const cn = byUpper.get(u);
    if (!cn || String(r.Security || '').trim()) return r;
    return { ...r, Security: cn };
  });
}

/** When group is ETF and BigQuery has no sector/industry, use neutral labels so UI/heatmap still groups. */
function applyEtfSectorIndustryDefaults(rows, groupMeta) {
  const code = String(groupMeta?.code || '').toUpperCase();
  const name = String(groupMeta?.name || '').toUpperCase();
  if (code !== 'ETF' && name !== 'ETF') return rows;
  return rows.map((r) => ({
    ...r,
    Sector: String(r.Sector || '').trim() || 'ETF',
    Industry: String(r.Industry || '').trim() || 'Exchange Traded Fund'
  }));
}

/**
 * Fill Security / Sector / Industry from TickerDetails for every row (by symbol).
 */
function applyTickerDetailsToRows(rows, detailMap) {
  return rows.map((r) => {
    const sym = String(r.Symbol ?? r.symbol ?? '')
      .toUpperCase()
      .trim();
    if (!sym) return r;
    const d = detailMap.get(sym);
    if (!d) return r;
    const cur = normalizeTickerDetailRow(r) || {
      Symbol: r.Symbol ?? r.symbol,
      Security: '',
      Sector: '',
      Industry: '',
      Index: ''
    };
    return {
      Symbol: cur.Symbol || d.Symbol,
      Security: d.Security || cur.Security || r.Security || r.security || '',
      Sector: d.Sector || cur.Sector || r.Sector || r.sector || '',
      Industry: d.Industry || cur.Industry || r.Industry || r.industry || '',
      Index: cur.Index || d.Index || r.Index || r.index || ''
    };
  });
}

async function fetchTickerDetailsRowsByIndexColumn(dbIndex) {
  const query = `
    SELECT Symbol, Security, Sector, Industry, \`Index\`
    FROM \`${TICKER_DETAILS_FQN}\`
    WHERE \`Index\` = @idx
    ORDER BY Symbol
  `;
  const [rows] = await bigquery.query({ query, params: { idx: dbIndex } });
  const merged = mergeTickerDetailRowsBySymbol(rows || []);
  return [...merged.values()].sort((a, b) =>
    a.Symbol.localeCompare(b.Symbol, undefined, { sensitivity: 'base' })
  );
}

async function mapIndexValueToDbIndex(indexValue) {
  const val = (indexValue || '').toString().trim().toLowerCase();
  if (!val) return 'S&P 500';
  if (val === 'sp500' || val === 's&p 500' || val === 's&p500' || val === 'sp 500') return 'S&P 500';
  if (val === 'nasdaq 100' || val === 'nasdaq100' || val === 'ndx') return 'Nasdaq 100';
  if (val === 'dow jones 30' || val === 'dow jones' || val === 'dow' || val === 'djia') return 'Dow Jones 30';
  if (val === 'etf') return 'ETF';
  return indexValue;
}

async function fetchAllTickerCloses(tickerSymbols, extendedStart, endDate) {
  if (!tickerSymbols.length) return [];
  // Note: this builds an IN (...) list; works with current codebase style but could be parameterized later.
  const tickersParam = tickerSymbols.map(t => `'${String(t).replace(/'/g, "\\'")}'`).join(', ');
  const query = `
    SELECT
      DATE(Date) AS TradeDate,
      Ticker,
      ANY_VALUE(Close) AS Close_raw
    FROM \`${TABLE_FQN}\`
    WHERE Ticker IN (${tickersParam})
      AND Date BETWEEN '${isoDate(extendedStart)}' AND '${isoDate(endDate)}'
    GROUP BY Ticker, TradeDate
    ORDER BY Ticker, TradeDate
  `;
  const [rows] = await bigquery.query({ query });
  if (!rows || rows.length === 0) return [];
  const out = [];
  for (const row of rows) {
    const dt = parseBqDate(row.TradeDate ?? row.Date);
    if (!dt) continue;
    out.push({
      Ticker: row.Ticker,
      Date: dt,
      Close: row.Close_raw != null ? Number(row.Close_raw) : null
    });
  }
  out.sort((a, b) => a.Ticker.localeCompare(b.Ticker) || a.Date - b.Date);
  return out;
}

function normalizeSignalCode(sig) {
  const s = String(sig || '').trim().toUpperCase();
  if (!s) return 'N';
  const allowed = new Set(['L1', 'L2', 'L3', 'S1', 'S2', 'S3', 'N']);
  return allowed.has(s) ? s : 'N';
}

async function fetchLatestSignalsForTickers(tickerSymbols, endDate) {
  if (!tickerSymbols.length) return new Map();
  const tickersParam = tickerSymbols.map((t) => `'${String(t).replace(/'/g, "\\'")}'`).join(', ');
  const query = `
    WITH ranked AS (
      SELECT
        UPPER(TRIM(CAST(Ticker AS STRING))) AS ticker,
        CAST(Signal AS STRING) AS signal,
        DATE(Date) AS trade_date,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(CAST(Ticker AS STRING)))
          ORDER BY DATE(Date) DESC
        ) AS rn
      FROM \`${SIGNALS_TABLE_FQN}\`
      WHERE UPPER(TRIM(CAST(Ticker AS STRING))) IN (${tickersParam})
        AND DATE(Date) <= DATE('${isoDate(endDate)}')
    )
    SELECT ticker, signal
    FROM ranked
    WHERE rn = 1
  `;
  const [rows] = await bigquery.query({ query });
  const out = new Map();
  for (const row of rows || []) {
    const ticker = String(row.ticker || '').toUpperCase().trim();
    if (!ticker) continue;
    out.set(ticker, normalizeSignalCode(row.signal));
  }
  return out;
}

function getStartEndClose(priceData, startDate, endDate) {
  const tickerData = Array.isArray(priceData) ? priceData : [];
  if (!tickerData.length) return [null, null];

  const startPrices = tickerData.filter(row => row.Date >= startDate && row.Close != null);
  const endPrices = tickerData.filter(row => row.Date <= endDate && row.Close != null);

  // Start uses first trading day on/after requested start date.
  const startClose = startPrices.length ? startPrices[0].Close : null;
  // End uses last trading day on/before requested end date.
  const endClose = endPrices.length ? endPrices[endPrices.length - 1].Close : null;

  return [startClose, endClose];
}

/**
 * For "last-date" performance: use the most recent two trading closes on/before `endDate`.
 * This avoids calendar-day gaps (weekends/holidays) collapsing returns to 0.00% for most symbols.
 */
function getLatestTwoCloses(priceData, endDate) {
  const tickerData = (Array.isArray(priceData) ? priceData : []).filter(
    (row) => row.Date <= endDate && row.Close != null
  );
  if (!tickerData.length) return [null, null];
  // Defensive: ensure prior close comes from a prior trading date (not duplicate same-day rows).
  const byDay = new Map();
  for (const r of tickerData) {
    const d = isoDate(r.Date);
    byDay.set(d, r.Close);
  }
  const days = [...byDay.keys()].sort();
  const endClose = byDay.get(days[days.length - 1]);
  const startClose = days.length > 1 ? byDay.get(days[days.length - 2]) : null;
  return [startClose, endClose];
}

function buildPriceDataByTicker(priceData) {
  const map = new Map();
  for (const row of priceData || []) {
    const t = String(row.Ticker || '').toUpperCase().trim();
    if (!t) continue;
    const cur = map.get(t);
    if (cur) cur.push(row);
    else map.set(t, [row]);
  }
  return map;
}

function calculateTotalReturnPercentage(startPrice, endPrice) {
  if (startPrice == null || startPrice === 0 || endPrice == null) return null;
  return Math.round(((endPrice - startPrice) / startPrice) * 100 * 100) / 100;
}

async function getTickerDetailsByIndex(indexValue, periodValue) {
  const [startDate, endDate] = await calculatePeriodDates(periodValue);
  const isLastDate = String(periodValue || '').toLowerCase() === 'last-date';

  let rows = [];
  let groupMeta = null;

  try {
    const groups = await fetchMarketGroupsWithCodes();
    const resolved = resolveMarketGroupFromIndexValue(indexValue, groups);
    if (resolved) {
      const members = await getMembersForMarketGroupId(resolved.id);
      if (!members.length) {
        return [];
      }
      rows = members.map((m) => ({
        Symbol: m.symbol,
        Security: m.company_name || '',
        Sector: '',
        Industry: '',
        Index: ''
      }));
      groupMeta = resolved;
    } else {
      const dbIndex = await mapIndexValueToDbIndex(indexValue);
      rows = await fetchTickerDetailsRowsByIndexColumn(dbIndex);
    }
  } catch (e) {
    console.error('getTickerDetailsByIndex:', e);
    try {
      const dbIndex = await mapIndexValueToDbIndex(indexValue);
      rows = await fetchTickerDetailsRowsByIndexColumn(dbIndex);
    } catch (e2) {
      console.error('getTickerDetailsByIndex fallback:', e2);
      return [];
    }
  }

  if (!rows.length) return [];

  const allSyms = [
    ...new Set(
      rows
        .map((r) => String(r.Symbol ?? r.symbol ?? '').toUpperCase().trim())
        .filter(Boolean)
    )
  ];
  const detailMap = mergeTickerDetailRowsBySymbol(
    await fetchTickerDetailsRowsBySymbols(allSyms)
  );
  rows = applyTickerDetailsToRows(rows, detailMap);
  rows = await backfillSecurityFromSupabase(rows);
  if (groupMeta) {
    rows = applyEtfSectorIndustryDefaults(rows, groupMeta);
  }

  const tickerSymbols = rows.map((r) => r.Symbol).filter(Boolean);
  if (!tickerSymbols.length) return [];

  const extendedStart = new Date(startDate);
  extendedStart.setDate(startDate.getDate() - 30);
  const priceData = await fetchAllTickerCloses(tickerSymbols, extendedStart, endDate);
  const priceDataByTicker = buildPriceDataByTicker(priceData);
  const signalByTicker = await fetchLatestSignalsForTickers(tickerSymbols, endDate);

  return rows
    .map((row, idx) => {
      const symbol = row.Symbol ? String(row.Symbol).toUpperCase() : '';
      if (!symbol) return null;
      const symbolPrices = priceDataByTicker.get(symbol) || [];

      const [startClose, endClose] = isLastDate
        ? getLatestTwoCloses(symbolPrices, endDate)
        : getStartEndClose(symbolPrices, startDate, endDate);
      const totalReturnPct = calculateTotalReturnPercentage(startClose, endClose);

      return {
        row: idx + 1,
        symbol,
        security: row.Security || '',
        sector: row.Sector || '',
        industry: row.Industry || '',
        index: groupMeta ? groupMeta.name : row.Index || '',
        totalReturnPercentage: totalReturnPct,
        price: endClose,
        signal: signalByTicker.get(symbol) || 'N'
      };
    })
    .filter(Boolean);
}

async function getIndexConstituentSymbols(indexValue) {
  let rows = [];
  try {
    const groups = await fetchMarketGroupsWithCodes();
    const resolved = resolveMarketGroupFromIndexValue(indexValue, groups);
    if (resolved) {
      const members = await getMembersForMarketGroupId(resolved.id);
      rows = (members || []).map((m) => String(m.symbol || '').toUpperCase().trim()).filter(Boolean);
    } else {
      const dbIndex = await mapIndexValueToDbIndex(indexValue);
      const fallbackRows = await fetchTickerDetailsRowsByIndexColumn(dbIndex);
      rows = (fallbackRows || []).map((r) => String(r.Symbol || '').toUpperCase().trim()).filter(Boolean);
    }
  } catch (e) {
    console.error('getIndexConstituentSymbols:', e);
    const dbIndex = await mapIndexValueToDbIndex(indexValue);
    const fallbackRows = await fetchTickerDetailsRowsByIndexColumn(dbIndex);
    rows = (fallbackRows || []).map((r) => String(r.Symbol || '').toUpperCase().trim()).filter(Boolean);
  }
  return [...new Set(rows)];
}

function yearsBetween(start, end) {
  return Math.round(((end - start) / (1000 * 60 * 60 * 24)) / DAYS_IN_YEAR * 1000) / 1000;
}

async function fetchCloseSeries(ticker, startDate, endDate) {
  const t = (ticker || '').toString().trim().toUpperCase();
  const query = `
    SELECT Date, Close as Close_raw
    FROM \`${TABLE_FQN}\`
    WHERE Ticker = @ticker
      AND Date BETWEEN @start AND @end
    ORDER BY Date
  `;
  const [rows] = await bigquery.query({
    query,
    params: { ticker: t, start: isoDate(startDate), end: isoDate(endDate) }
  });
  if (!rows || rows.length === 0) return [];
  const out = [];
  for (const row of rows) {
    const dt = parseBqDate(row.Date);
    if (!dt) continue;
    const close = row.Close_raw != null ? Number(row.Close_raw) : null;
    if (close == null) continue;
    out.push({ Date: dt, Close: close });
  }
  return out;
}

function closeOnOrBefore(prices, target) {
  const before = prices.filter(p => p.Date <= target);
  if (!before.length) return [null, null];
  const latest = before[before.length - 1];
  return [latest.Date, latest.Close];
}

function closeOnOrAfter(prices, target) {
  const after = prices.filter(p => p.Date >= target);
  if (!after.length) return [null, null];
  const first = after[0];
  return [first.Date, first.Close];
}

function calcTotalReturnPct(startClose, endClose) {
  return calculateTotalReturnPercentage(startClose, endClose);
}

function formatPeriods(rows, labelKey = 'period', labelPrefix = null) {
  return rows.map(row => {
    let period = row[labelKey];
    if (labelPrefix) period = `${labelPrefix}: ${period}`;

    const startDate = row.start_date_found || row.start_date_requested;
    const endDate = row.end_date_found || row.end_date_requested;
    const years = parseFloat(row.years || 0);
    const startPrice = row.start_price != null ? Number(row.start_price) : null;
    const endPrice = row.end_price != null ? Number(row.end_price) : null;
    const priceDiff = (startPrice != null && endPrice != null) ? (endPrice - startPrice) : null;
    const totalReturn = row.total_return_pct != null
      ? Number(row.total_return_pct)
      : (startPrice == null || startPrice === 0 || endPrice == null ? null : ((endPrice - startPrice) / startPrice) * 100);

    const simpleAnnualReturn = (years > 0 && totalReturn != null) ? totalReturn / years : 0;
    const cagrPercent = (years > 0 && startPrice != null && startPrice > 0 && endPrice != null)
      ? (((endPrice / startPrice) ** (1 / years)) - 1) * 100
      : 0;

    return {
      period,
      startDate,
      endDate,
      years,
      startPrice,
      endPrice,
      priceDifference: priceDiff,
      totalReturn,
      simpleAnnualReturn: Math.round(simpleAnnualReturn * 100) / 100,
      cagrPercent: Math.round(cagrPercent * 100) / 100
    };
  });
}

const DYNAMIC_PERIOD_DEFS = [
  { name: 'Last date', type: 'days', days: 1 },
  { name: 'Week', type: 'days', days: 7 },
  { name: 'Last Month', type: 'days', days: 30 },
  { name: 'Last 3 months', type: 'days', days: 91 },
  { name: 'Last 6 months', type: 'days', days: 183 },
  { name: 'Year to Date (YTD)', type: 'ytd' },
  { name: 'Last 1 year', type: 'years', years: 1 },
  { name: 'Last 2 years', type: 'years', years: 2 },
  { name: 'Last 3 years', type: 'years', years: 3 },
  { name: 'Last 5 years', type: 'years', years: 5 },
  { name: 'Last 10 years', type: 'years', years: 10 },
  { name: 'Last 15 years', type: 'years', years: 15 },
  { name: 'Last 20 years', type: 'years', years: 20 },
  { name: 'Last 25 years', type: 'years', years: 25 },
  { name: 'Last 50 years', type: 'years', years: 50 },
];

const PREDEFINED_START_YEARS = [2024, 2023, 2022, 2020, 2015, 2010, 2005, 2000, 1975];

async function getMinDateForTicker(ticker) {
  const query = `
    SELECT MIN(Date) AS min_date
    FROM \`${TABLE_FQN}\`
    WHERE Ticker = @ticker
  `;
  const [rows] = await bigquery.query({ query, params: { ticker: String(ticker).toUpperCase() } });
  const dt = rows && rows[0] ? parseBqDate(rows[0].min_date) : null;
  if (!dt) throw new Error(`No start date found for ticker ${ticker}`);
  return dt;
}

async function calculateDynamicPeriods(ticker, endDate, prices) {
  const results = [];
  for (const def of DYNAMIC_PERIOD_DEFS) {
    let start = new Date(endDate);
    if (def.type === 'days') start.setDate(endDate.getDate() - def.days);
    else if (def.type === 'years') start.setDate(endDate.getDate() - Math.floor(def.years * 365));
    else if (def.type === 'ytd') start = new Date(endDate.getFullYear(), 0, 1);

    const [startFoundDate, startPrice] = closeOnOrAfter(prices, start);
    const [endFoundDate, endPrice] = closeOnOrBefore(prices, endDate);
    results.push({
      period: def.name,
      start_date_requested: isoDate(start),
      end_date_requested: isoDate(endDate),
      start_date_found: startFoundDate ? isoDate(startFoundDate) : null,
      end_date_found: endFoundDate ? isoDate(endFoundDate) : null,
      start_price: startPrice,
      end_price: endPrice,
      years: (startFoundDate && endFoundDate) ? yearsBetween(startFoundDate, endFoundDate) : 0,
      total_return_pct: calcTotalReturnPct(startPrice, endPrice)
    });
  }
  return results;
}

async function calculatePredefinedPeriods(ticker, endDate, prices) {
  const out = [];
  for (const yr of PREDEFINED_START_YEARS) {
    const start = new Date(yr, 0, 1);
    if (start > endDate) continue;
    const [startFoundDate, startPrice] = closeOnOrAfter(prices, start);
    const [endFoundDate, endPrice] = closeOnOrBefore(prices, endDate);
    out.push({
      period: String(yr),
      start_date_requested: isoDate(start),
      end_date_requested: isoDate(endDate),
      start_date_found: startFoundDate ? isoDate(startFoundDate) : null,
      end_date_found: endFoundDate ? isoDate(endFoundDate) : null,
      start_price: startPrice,
      end_price: endPrice,
      years: (startFoundDate && endFoundDate) ? yearsBetween(startFoundDate, endFoundDate) : 0,
      total_return_pct: calcTotalReturnPct(startPrice, endPrice)
    });
  }
  return out;
}

async function calculateAnnualReturns(ticker, endDate, minYear, prices) {
  const out = [];
  const endYear = endDate.getFullYear();
  for (let y = minYear; y <= endYear; y++) {
    const start = new Date(y, 0, 1);
    const end = (y === endYear) ? endDate : new Date(y, 11, 31);
    const [startFoundDate, startPrice] = closeOnOrAfter(prices, start);
    const [endFoundDate, endPrice] = closeOnOrBefore(prices, end);
    if (!startFoundDate || !endFoundDate) continue;
    out.push({
      year: String(y),
      start_date_requested: isoDate(start),
      end_date_requested: isoDate(end),
      start_date_found: isoDate(startFoundDate),
      end_date_found: isoDate(endFoundDate),
      start_price: startPrice,
      end_price: endPrice,
      years: yearsBetween(startFoundDate, endFoundDate),
      total_return_pct: calcTotalReturnPct(startPrice, endPrice)
    });
  }
  return out;
}

async function calculateMonthlyReturns(ticker, endDate, fromYear, prices) {
  const out = [];
  const endYear = endDate.getFullYear();
  for (let y = fromYear; y <= endYear; y++) {
    for (let m = 0; m < 12; m++) {
      const start = new Date(y, m, 1);
      const end = (y === endYear && m === endDate.getMonth()) ? endDate : new Date(y, m + 1, 0);
      if (start > endDate) continue;
      const [startFoundDate, startPrice] = closeOnOrAfter(prices, start);
      const [endFoundDate, endPrice] = closeOnOrBefore(prices, end);
      if (!startFoundDate || !endFoundDate) continue;
      out.push({
        month: `${y}-${String(m + 1).padStart(2, '0')}`,
        start_date_requested: isoDate(start),
        end_date_requested: isoDate(end),
        start_date_found: isoDate(startFoundDate),
        end_date_found: isoDate(endFoundDate),
        start_price: startPrice,
        end_price: endPrice,
        years: yearsBetween(startFoundDate, endFoundDate),
        total_return_pct: calcTotalReturnPct(startPrice, endPrice)
      });
    }
  }
  return out;
}

async function calculateQuarterlyReturns(ticker, endDate, fromYear, prices) {
  const out = [];
  const endYear = endDate.getFullYear();
  for (let y = fromYear; y <= endYear; y++) {
    for (let q = 1; q <= 4; q++) {
      const startMonth = (q - 1) * 3;
      const start = new Date(y, startMonth, 1);
      const end = (y === endYear && endDate.getMonth() <= startMonth + 2)
        ? endDate
        : new Date(y, startMonth + 3, 0);
      if (start > endDate) continue;
      const [startFoundDate, startPrice] = closeOnOrAfter(prices, start);
      const [endFoundDate, endPrice] = closeOnOrBefore(prices, end);
      if (!startFoundDate || !endFoundDate) continue;
      out.push({
        quarter: `${y}-Q${q}`,
        start_date_requested: isoDate(start),
        end_date_requested: isoDate(end),
        start_date_found: isoDate(startFoundDate),
        end_date_found: isoDate(endFoundDate),
        start_price: startPrice,
        end_price: endPrice,
        years: yearsBetween(startFoundDate, endFoundDate),
        total_return_pct: calcTotalReturnPct(startPrice, endPrice)
      });
    }
  }
  return out;
}

async function calculateCustomRange(ticker, startDate, endDate, prices) {
  const [startFoundDate, startPrice] = closeOnOrAfter(prices, startDate);
  const [endFoundDate, endPrice] = closeOnOrBefore(prices, endDate);
  return {
    period: 'Selected dates',
    start_date_requested: isoDate(startDate),
    end_date_requested: isoDate(endDate),
    start_date_found: startFoundDate ? isoDate(startFoundDate) : null,
    end_date_found: endFoundDate ? isoDate(endFoundDate) : null,
    start_price: startPrice,
    end_price: endPrice,
    years: (startFoundDate && endFoundDate) ? yearsBetween(startFoundDate, endFoundDate) : 0,
    total_return_pct: calcTotalReturnPct(startPrice, endPrice)
  };
}

async function buildAllReturnsPayloadFromPrices(
  ticker,
  endDate,
  prices,
  includePredefined,
  includeAnnual,
  customRange,
  annualFromYear,
  options = {}
) {
  const t = (ticker || '').toString().trim().toUpperCase();
  const includeDynamic = options.includeDynamic !== false;
  const includeMonthly = options.includeMonthly !== false;
  const includeQuarterly = options.includeQuarterly !== false;
  const includeCustom = options.includeCustom !== false;
  if (!prices || !prices.length) {
    return {
      success: true,
      ticker: t,
      asOfDate: isoDate(endDate),
      performance: {
        dynamicPeriods: [],
        predefinedPeriods: [],
        annualReturns: [],
        customRange: [],
        quarterlyReturns: [],
        monthlyReturns: []
      }
    };
  }
  const minDt = prices[0].Date;
  const startYear = Math.max(minDt.getFullYear(), annualFromYear);

  const dynamic = includeDynamic ? await calculateDynamicPeriods(t, endDate, prices) : [];
  const predefined = includePredefined ? await calculatePredefinedPeriods(t, endDate, prices) : [];
  const annual = includeAnnual ? await calculateAnnualReturns(t, endDate, minDt.getFullYear(), prices) : [];
  const monthly = includeMonthly ? await calculateMonthlyReturns(t, endDate, startYear, prices) : [];
  const quarterly = includeQuarterly ? await calculateQuarterlyReturns(t, endDate, startYear, prices) : [];

  let custom = null;
  if (includeCustom && customRange && customRange.length === 2) {
    custom = await calculateCustomRange(t, new Date(customRange[0]), new Date(customRange[1]), prices);
  }

  return {
    success: true,
    ticker: t,
    asOfDate: isoDate(endDate),
    performance: {
      dynamicPeriods: formatPeriods(dynamic),
      predefinedPeriods: formatPeriods(predefined),
      annualReturns: formatPeriods(annual, 'year'),
      customRange: custom ? formatPeriods([custom]) : [],
      quarterlyReturns: formatPeriods(quarterly, 'quarter'),
      monthlyReturns: formatPeriods(monthly, 'month')
    }
  };
}

async function calculateAllReturns(ticker, includePredefined = true, includeAnnual = true, customRange = null, annualFromYear = 1970) {
  const endDate = await getMaxDate();
  const t = (ticker || '').toString().trim().toUpperCase();

  const minDt = await getMinDateForTicker(t);
  const startYear = Math.max(minDt.getFullYear(), annualFromYear);
  const earliest = new Date(startYear, 0, 1);
  const prices = await fetchCloseSeries(t, earliest, endDate);

  return buildAllReturnsPayloadFromPrices(t, endDate, prices, includePredefined, includeAnnual, customRange, annualFromYear);
}

async function calculateReturnsSections(ticker, sections = {}, customRange = null, annualFromYear = 1970) {
  const includeDynamic = sections.includeDynamic !== false;
  const includePredefined = sections.includePredefined !== false;
  const includeAnnual = sections.includeAnnual !== false;
  const includeMonthly = sections.includeMonthly !== false;
  const includeQuarterly = sections.includeQuarterly !== false;
  const includeCustom = sections.includeCustom !== false;

  const endDate = await getMaxDate();
  const t = (ticker || '').toString().trim().toUpperCase();
  const minDt = await getMinDateForTicker(t);
  const startYear = Math.max(minDt.getFullYear(), annualFromYear);
  const earliest = new Date(startYear, 0, 1);
  const prices = await fetchCloseSeries(t, earliest, endDate);

  return buildAllReturnsPayloadFromPrices(
    t,
    endDate,
    prices,
    includePredefined,
    includeAnnual,
    customRange,
    annualFromYear,
    { includeDynamic, includeMonthly, includeQuarterly, includeCustom }
  );
}

function buildSyntheticIndexCloseSeries(priceData, weightByTicker) {
  const byDate = new Map();
  const baseCloseByTicker = new Map();
  for (const row of priceData || []) {
    const t = String(row.Ticker || '').toUpperCase().trim();
    if (!t) continue;
    const close = row.Close != null ? Number(row.Close) : null;
    if (!Number.isFinite(close) || close <= 0) continue;
    if (!baseCloseByTicker.has(t)) baseCloseByTicker.set(t, close);
    const base = baseCloseByTicker.get(t);
    if (!Number.isFinite(base) || base <= 0) continue;
    const ratio = close / base;
    const w = Number(weightByTicker.get(t));
    const weight = Number.isFinite(w) && w > 0 ? w : 1;
    const d = isoDate(row.Date);
    const cur = byDate.get(d) || { num: 0, den: 0, dt: new Date(row.Date) };
    cur.num += weight * ratio;
    cur.den += weight;
    if (row.Date > cur.dt) cur.dt = new Date(row.Date);
    byDate.set(d, cur);
  }

  const out = [];
  for (const [d, v] of byDate.entries()) {
    if (!Number.isFinite(v.num) || !Number.isFinite(v.den) || v.den <= 0) continue;
    out.push({ Date: new Date(`${d}T12:00:00`), Close: (v.num / v.den) * 100 });
  }
  out.sort((a, b) => a.Date - b.Date);
  return out;
}

/**
 * When your BigQuery `stock_all_data` stores major US indices as plain tickers
 * (e.g. SPX, DJI, IXIC), use that single series for published-index returns.
 * Falls back to synthetic constituents when no mapping applies.
 */
function resolveOfficialIndexTicker(indexValue) {
  const raw = String(indexValue || '').trim();
  if (!raw) return null;
  const envMap = (process.env.OFFICIAL_INDEX_TICKER_MAP || '').trim();
  if (envMap) {
    try {
      const obj = JSON.parse(envMap);
      if (obj && typeof obj === 'object') {
        const keys = [raw, normIndexKey(raw), raw.toLowerCase(), raw.toUpperCase()];
        for (const k of keys) {
          if (k != null && obj[k] != null && String(obj[k]).trim()) {
            return String(obj[k]).trim().toUpperCase();
          }
        }
      }
    } catch {
      /* ignore bad JSON */
    }
  }

  const k = normIndexKey(raw);
  const up = raw.toUpperCase();

  if (k === 'sp500' || k === 's&p 500' || k === 's&p500' || k === 'sp 500' || up === 'SP') return 'SPX';
  if (
    k === 'dow jones' ||
    k === 'dow jones 30' ||
    k === 'dow' ||
    k === 'djia' ||
    up === 'DJ'
  ) {
    return 'DJI';
  }
  if (
    k === 'nasdaq composite' ||
    k === 'nasdaq comp' ||
    k === 'ixic' ||
    k === 'comp' ||
    up === 'COMP'
  ) {
    return 'IXIC';
  }
  return null;
}

async function calculateIndexReturns(indexValue, customRange = null, annualFromYear = 1970) {
  const endDate = await getMaxDate();
  const officialTicker = resolveOfficialIndexTicker(indexValue);
  if (officialTicker) {
    const minDt = await getMinDateForTicker(officialTicker);
    const startYear = Math.max(minDt.getFullYear(), annualFromYear);
    const earliest = new Date(startYear, 0, 1);
    const prices = await fetchCloseSeries(officialTicker, earliest, endDate);
    const base = await buildAllReturnsPayloadFromPrices(
      officialTicker,
      endDate,
      prices,
      true,
      true,
      customRange,
      annualFromYear
    );
    return {
      ...base,
      index: String(indexValue || '').trim(),
      seriesMode: 'official-index-ticker',
      officialIndexTicker: officialTicker,
      constituentsCount: 1,
      constituentSymbolsUsed: [officialTicker],
      symbolsWithPriceData: prices.length ? [officialTicker] : [],
      constituentsMissingPriceData: prices.length ? [] : [officialTicker],
      syntheticCloseSeries: prices.map((r) => ({
        date: isoDate(r.Date),
        close: Math.round(Number(r.Close) * 10000) / 10000
      }))
    };
  }

  const tickerSymbols = await getIndexConstituentSymbols(indexValue);
  if (!Array.isArray(tickerSymbols) || !tickerSymbols.length) {
    throw new Error(`No constituents found for index "${indexValue}"`);
  }

  const weightByTicker = buildWeightMapForIndex(indexValue, tickerSymbols);

  const earliest = new Date(Math.max(1900, Number(annualFromYear) || 1970), 0, 1);
  const priceData = await fetchAllTickerCloses(tickerSymbols, earliest, endDate);
  const symbolsWithPriceData = [
    ...new Set(
      (priceData || [])
        .map((r) => String(r.Ticker || '').toUpperCase().trim())
        .filter(Boolean)
    )
  ].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  const symbolsWithPriceSet = new Set(symbolsWithPriceData);
  const constituentSymbolsUsed = [...tickerSymbols].sort((a, b) =>
    a.localeCompare(b, undefined, { sensitivity: 'base' })
  );
  const constituentsMissingPriceData = constituentSymbolsUsed.filter((s) => !symbolsWithPriceSet.has(s));
  const synthetic = buildSyntheticIndexCloseSeries(priceData, weightByTicker);
  if (!synthetic.length) {
    throw new Error(`No synthetic close series could be built for index "${indexValue}"`);
  }

  const syntheticEnd = synthetic[synthetic.length - 1].Date;
  const minDt = synthetic[0].Date;
  const startYear = Math.max(minDt.getFullYear(), annualFromYear);

  const dynamic = await calculateDynamicPeriods(String(indexValue || 'INDEX'), syntheticEnd, synthetic);
  const predefined = await calculatePredefinedPeriods(String(indexValue || 'INDEX'), syntheticEnd, synthetic);
  const annual = await calculateAnnualReturns(String(indexValue || 'INDEX'), syntheticEnd, minDt.getFullYear(), synthetic);
  const monthly = await calculateMonthlyReturns(String(indexValue || 'INDEX'), syntheticEnd, startYear, synthetic);
  const quarterly = await calculateQuarterlyReturns(String(indexValue || 'INDEX'), syntheticEnd, startYear, synthetic);

  let custom = null;
  if (customRange && customRange.length === 2) {
    custom = await calculateCustomRange(String(indexValue || 'INDEX'), new Date(customRange[0]), new Date(customRange[1]), synthetic);
  }

  return {
    success: true,
    index: String(indexValue || '').trim(),
    seriesMode: 'synthetic-constituents',
    officialIndexTicker: null,
    asOfDate: isoDate(syntheticEnd),
    constituentsCount: constituentSymbolsUsed.length,
    constituentSymbolsUsed,
    symbolsWithPriceData,
    constituentsMissingPriceData,
    syntheticCloseSeries: synthetic.map((r) => ({
      date: isoDate(r.Date),
      close: Math.round(Number(r.Close) * 10000) / 10000
    })),
    performance: {
      dynamicPeriods: formatPeriods(dynamic),
      predefinedPeriods: formatPeriods(predefined),
      annualReturns: formatPeriods(annual, 'year'),
      customRange: custom ? formatPeriods([custom]) : [],
      quarterlyReturns: formatPeriods(quarterly, 'quarter'),
      monthlyReturns: formatPeriods(monthly, 'month')
    }
  };
}

const DEFAULT_LEADERBOARD_INTERVALS = ['1d', '1m', '1y', '3y', '5y', '10y', '20y'];

/** Map API shorthand to internal rolling keys (see computeRollingWindowStart). */
const LEADER_PERIOD_ALIASES = {
  '1d': 'last-date',
  'last-day': 'last-date',
  '1m': 'last-month',
  '30d': 'last-month',
  '1y': 'last-1-year',
  '12m': 'last-1-year',
  '3y': 'last-3-years',
  '5y': 'last-5-years',
  '10y': 'last-10-years',
  '20y': 'last-20-years'
};

function normalizeLeaderPeriodKey(raw) {
  const k = String(raw || '')
    .toLowerCase()
    .trim();
  return LEADER_PERIOD_ALIASES[k] || k;
}

/** S&P 500 → 20/20; Dow & Nasdaq (100) → 10/10. */
function leaderBoardDepthForIndex(indexValue) {
  const k = normIndexKey(String(indexValue || ''));
  if (k === 'sp500' || k === 's&p 500' || k === 's&p500' || k === 'sp 500') {
    return { top: 20, bottom: 20 };
  }
  if (
    k === 'dow jones' ||
    k === 'dow jones 30' ||
    k === 'dow' ||
    k === 'djia' ||
    k === 'nasdaq 100' ||
    k === 'nasdaq100' ||
    k === 'ndx' ||
    k === 'nasdaq-100' ||
    k === 'nasdaq'
  ) {
    return { top: 10, bottom: 10 };
  }
  return { top: 10, bottom: 10 };
}

function calendarQuarterBounds(year, quarter) {
  const q = Number(quarter);
  const y = Number(year);
  if (!Number.isFinite(q) || q < 1 || q > 4 || !Number.isFinite(y)) return [null, null];
  const parts = [
    [0, 1, 2, 31],
    [3, 1, 5, 30],
    [6, 1, 8, 30],
    [9, 1, 11, 31]
  ];
  const [sm, sd, em, ed] = parts[q - 1];
  const start = new Date(y, sm, sd, 0, 0, 0, 0);
  const end = new Date(y, em, ed, 23, 59, 59, 999);
  return [start, end];
}

async function buildConstituentNameBySymbol(symbols) {
  const rows = await fetchTickerDetailsRowsBySymbols(symbols);
  const merged = mergeTickerDetailRowsBySymbol(rows);
  const map = new Map();
  for (const sym of symbols) {
    const up = String(sym || '').toUpperCase().trim();
    const row = merged.get(up);
    map.set(up, row?.Security ? String(row.Security).trim() : '');
  }
  const missing = [...map.entries()].filter(([, n]) => !n).map(([s]) => s);
  if (missing.length) {
    const stubRows = missing.map((Symbol) => ({ Symbol, Security: '' }));
    const filled = await backfillSecurityFromSupabase(stubRows);
    for (const r of filled) {
      const u = String(r.Symbol || '').toUpperCase().trim();
      const nm = String(r.Security || '').trim();
      if (nm) map.set(u, nm);
    }
  }
  return map;
}

function rankLeaderSlice(byTicker, symbols, nameBySym, startDate, endDate, topN, bottomN, useLastTwoTradingDays, endClamp) {
  const scores = [];
  const endD = endClamp || endDate;
  for (const sym of symbols) {
    const rows = byTicker.get(sym) || [];
    let pct = null;
    if (useLastTwoTradingDays) {
      const [sc, ec] = getLatestTwoCloses(rows, endD);
      pct = calculateTotalReturnPercentage(sc, ec);
    } else {
      const [sc, ec] = getStartEndClose(rows, startDate, endDate);
      pct = calculateTotalReturnPercentage(sc, ec);
    }
    scores.push({
      symbol: sym,
      companyName: nameBySym.get(sym) || '',
      totalReturnPct: pct
    });
  }
  const valid = scores.filter((s) => s.totalReturnPct != null && Number.isFinite(s.totalReturnPct));
  const sortedDesc = [...valid].sort((a, b) => b.totalReturnPct - a.totalReturnPct);
  const sortedAsc = [...valid].sort((a, b) => a.totalReturnPct - b.totalReturnPct);
  return {
    top: sortedDesc.slice(0, topN),
    bottom: sortedAsc.slice(0, bottomN)
  };
}

/**
 * Rank index constituents by total return over multiple rolling windows, calendar quarters, and optional custom range.
 * Uses one BigQuery fetch for all closes in the union of requested windows.
 *
 * @param {string} indexValue — e.g. sp500, dow jones, nasdaq 100
 * @param {object} [options]
 * @param {string[]} [options.intervals] — default 1d,1m,1y,3y,5y,10y,20y (aliases mapped internally)
 * @param {number[]} [options.quarterYears] — calendar years for Q1–Q4 blocks (default: latest year & prior)
 * @param {string} [options.customStartDate] — ISO date with options.customEndDate
 * @param {string} [options.customEndDate]
 */
async function calculateIndexConstituentLeaderboards(indexValue, options = {}) {
  const endDate = await getMaxDate();
  const constituents = await getIndexConstituentSymbols(indexValue);
  if (!constituents.length) {
    throw new Error(`No constituents found for index "${indexValue}"`);
  }

  const depths = leaderBoardDepthForIndex(indexValue);
  const rawIntervals =
    Array.isArray(options.intervals) && options.intervals.length ? options.intervals : DEFAULT_LEADERBOARD_INTERVALS;

  const quarterYears =
    Array.isArray(options.quarterYears) && options.quarterYears.length
      ? options.quarterYears.map((y) => Number(y)).filter((y) => Number.isFinite(y))
      : [endDate.getFullYear(), endDate.getFullYear() - 1];

  let customStart = null;
  let customEnd = null;
  const csRaw = (options.customStartDate || '').trim();
  const ceRaw = (options.customEndDate || '').trim();
  if (csRaw && ceRaw) {
    const cs = new Date(csRaw);
    const ce = new Date(ceRaw);
    if (!Number.isNaN(cs.getTime()) && !Number.isNaN(ce.getTime()) && cs <= ce) {
      customStart = cs;
      customEnd = ce;
    }
  }

  let earliest = endDate;
  for (const raw of rawIntervals) {
    const nk = normalizeLeaderPeriodKey(raw);
    if (nk === 'last-date') continue;
    const s = computeRollingWindowStart(endDate, nk);
    if (s < earliest) earliest = s;
  }
  if (customStart && customStart < earliest) earliest = customStart;
  for (const y of quarterYears) {
    for (let q = 1; q <= 4; q++) {
      const [qs] = calendarQuarterBounds(y, q);
      if (qs && qs < earliest) earliest = qs;
    }
  }

  const buffered = new Date(earliest);
  buffered.setDate(buffered.getDate() - 45);

  const priceData = await fetchAllTickerCloses(constituents, buffered, endDate);
  const byTicker = buildPriceDataByTicker(priceData);
  const symbolsWithPriceData = [...byTicker.keys()].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));

  const nameBySym = await buildConstituentNameBySymbol(constituents);

  const intervalsOut = {};
  for (const rawInterval of rawIntervals) {
    const nk = normalizeLeaderPeriodKey(rawInterval);
    const useLastTwo = nk === 'last-date';
    const winStart = useLastTwo ? null : computeRollingWindowStart(endDate, nk);
    const slice = rankLeaderSlice(
      byTicker,
      constituents,
      nameBySym,
      winStart || endDate,
      endDate,
      depths.top,
      depths.bottom,
      useLastTwo,
      endDate
    );
    const keyOut = String(rawInterval);
    intervalsOut[keyOut] = {
      canonicalPeriod: nk,
      startDateRequested: winStart ? isoDate(winStart) : null,
      endDateRequested: isoDate(endDate),
      methodologyNote: useLastTwo ? 'Return from prior trading session close to latest close (matches last-date).' : null,
      ...slice
    };
  }

  const quartersByYear = {};
  for (const y of quarterYears) {
    quartersByYear[String(y)] = {};
    for (let q = 1; q <= 4; q++) {
      const [qs, qe] = calendarQuarterBounds(y, q);
      if (!qs || !qe) continue;
      const slice = rankLeaderSlice(
        byTicker,
        constituents,
        nameBySym,
        qs,
        qe,
        depths.top,
        depths.bottom,
        false,
        qe
      );
      quartersByYear[String(y)][`Q${q}`] = {
        startDate: isoDate(qs),
        endDate: isoDate(qe),
        ...slice
      };
    }
  }

  let customRange = null;
  if (customStart && customEnd) {
    const slice = rankLeaderSlice(
      byTicker,
      constituents,
      nameBySym,
      customStart,
      customEnd,
      depths.top,
      depths.bottom,
      false,
      customEnd
    );
    customRange = {
      startDate: isoDate(customStart),
      endDate: isoDate(customEnd),
      ...slice
    };
  }

  return {
    success: true,
    index: String(indexValue || '').trim(),
    asOfDate: isoDate(endDate),
    constituentsCount: constituents.length,
    symbolsWithPriceDataCount: symbolsWithPriceData.length,
    symbolsMissingPriceData: constituents.filter((s) => !byTicker.has(s)),
    topN: depths.top,
    bottomN: depths.bottom,
    intervals: intervalsOut,
    quartersByYear,
    customRange
  };
}

/**
 * Latest daily volume vs trailing 10-session average volume (same-trading-day definition as ticker-details).
 * Requires a numeric `Volume` column on BIGQUERY_TABLE; otherwise returns an empty Map.
 */
async function fetchRelativeVolume10dByTicker(tickerSymbols, endDate) {
  const out = new Map();
  if (!Array.isArray(tickerSymbols) || !tickerSymbols.length) return out;
  if (!ENABLE_RELATIVE_VOLUME_QUERY) return out;

  const start = new Date(endDate);
  start.setDate(start.getDate() - 120);

  const tickersParam = tickerSymbols.map((t) => `'${String(t).replace(/'/g, "\\'")}'`).join(', ');
  const query = `
    WITH ranked AS (
      SELECT
        Ticker,
        SAFE_CAST(Volume AS FLOAT64) AS vol,
        ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS rn
      FROM \`${TABLE_FQN}\`
      WHERE Ticker IN (${tickersParam})
        AND Date BETWEEN '${isoDate(start)}' AND '${isoDate(endDate)}'
        AND Volume IS NOT NULL
        AND SAFE_CAST(Volume AS FLOAT64) > 0
    ),
    agg AS (
      SELECT
        Ticker,
        MAX(CASE WHEN rn = 1 THEN vol END) AS v_last,
        AVG(CASE WHEN rn BETWEEN 2 AND 11 THEN vol END) AS v_avg_prior
      FROM ranked
      WHERE rn <= 11
      GROUP BY Ticker
    )
    SELECT Ticker, v_last, v_avg_prior
    FROM agg
    WHERE v_last IS NOT NULL AND v_avg_prior IS NOT NULL AND v_avg_prior > 0
  `;

  try {
    const [rows] = await bigquery.query({ query });
    for (const row of rows || []) {
      const t = String(row.Ticker || '')
        .toUpperCase()
        .trim();
      const vl = Number(row.v_last);
      const va = Number(row.v_avg_prior);
      const rel = vl / va;
      if (t && Number.isFinite(rel) && rel > 0) {
        out.set(t, {
          relativeVolume10d: Math.round(rel * 1000) / 1000,
          volume: Number.isFinite(vl) ? vl : null
        });
      }
    }
  } catch (e) {
    console.warn('fetchRelativeVolume10dByTicker:', e.message || e);
  }
  return out;
}

/**
 * Whitelist for POST /api/market/index-market-movers `period` (maps to getTickerDetailsByIndex / computeRollingWindowStart).
 */
function normalizeMarketMoversPeriod(raw) {
  const k = String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[\s_]+/g, '-');
  const map = {
    '1d': 'last-date',
    'last-date': 'last-date',
    '5d': 'last-5-days',
    'last-5-days': 'last-5-days',
    mtd: 'mtd',
    '1m': 'last-month',
    'last-month': 'last-month',
    qtd: 'qtd',
    '3m': 'last-3-months',
    'last-3-months': 'last-3-months',
    '6m': 'last-6-months',
    'last-6-months': 'last-6-months',
    ytd: 'ytd',
    '1y': 'last-1-year',
    'last-1-year': 'last-1-year',
    '3y': 'last-3-years',
    'last-3-years': 'last-3-years',
    '5y': 'last-5-years',
    'last-5-years': 'last-5-years',
    '10y': 'last-10-years',
    'last-10-years': 'last-10-years',
    '20y': 'last-20-years',
    'last-20-years': 'last-20-years',
    all: 'all-available',
    'all-available': 'all-available'
  };
  const v = map[k];
  return v || 'last-date';
}

function marketMoversSessionCopy(period) {
  switch (period) {
    case 'last-date':
      return 'Returns use the latest daily close vs the prior trading session close (same methodology as ticker-details period last-date). Relative volume uses the latest session vs trailing 10 sessions.';
    case 'last-5-days':
      return 'Returns use total % change from approximately 5 calendar days before the latest close through the latest close.';
    case 'mtd':
      return 'Returns use total % change from the first calendar day of the month through the latest close.';
    case 'qtd':
      return 'Returns use total % change from the first calendar day of the quarter through the latest close.';
    case 'all-available':
      return 'Returns use total % change over ~25 years of history through the latest close (capped for performance).';
    default:
      return 'Returns use total % change from the start of the selected rolling/calendar window through the latest close. Relative volume always uses the latest session vs trailing 10 sessions.';
  }
}

/**
 * Scatter payload: period % change + relative volume (10d, latest session) per constituent.
 */
async function calculateIndexMarketMovers(indexValue, periodValue = 'last-date') {
  const period = normalizeMarketMoversPeriod(periodValue);
  const endDate = await getMaxDate();
  const rows = await getTickerDetailsByIndex(indexValue, period);
  if (!rows.length) {
    return {
      success: true,
      index: String(indexValue || '').trim(),
      period,
      asOfDate: isoDate(endDate),
      sessionNote: marketMoversSessionCopy(period),
      volumeNote: 'No constituents returned for this index.',
      points: []
    };
  }

  const symbols = rows.map((r) => String(r.symbol || '').toUpperCase().trim()).filter(Boolean);
  const volMap = await fetchRelativeVolume10dByTicker(symbols, endDate);

  const points = rows.map((r) => {
    const sym = String(r.symbol || '').toUpperCase().trim();
    const vm = volMap.get(sym);
    const hasVol = vm && Number.isFinite(vm.relativeVolume10d) && vm.relativeVolume10d > 0;
    const last = r.price;
    const pct = r.totalReturnPercentage;
    let priceChange = null;
    if (Number.isFinite(pct) && Number.isFinite(last) && last > 0) {
      const prior = last / (1 + Number(pct) / 100);
      priceChange = Math.round((last - prior) * 100) / 100;
    }
    return {
      symbol: sym,
      companyName: String(r.security || '').trim(),
      sector: String(r.sector || '').trim(),
      industry: String(r.industry || '').trim(),
      dayReturnPct: r.totalReturnPercentage,
      lastPrice: Number.isFinite(last) ? Math.round(last * 100) / 100 : null,
      priceChange,
      volume: hasVol && vm.volume != null ? vm.volume : null,
      relativeVolume10d: hasVol ? vm.relativeVolume10d : 1,
      relativeVolumeIsEstimated: !hasVol
    };
  });

  return {
    success: true,
    index: String(indexValue || '').trim(),
    period,
    asOfDate: isoDate(endDate),
    sessionNote:
      marketMoversSessionCopy(period) +
      ' Pre / Market / Post tabs are layout-only until extended-hours bars exist.',
    volumeNote:
      volMap.size > 0
        ? 'Relative volume = latest session volume ÷ average volume of the prior 10 sessions (with volume).'
        : 'Volume column missing or empty in price table; points use 1.0× on the X-axis until BigQuery exposes Volume.',
    volumeRowsResolved: volMap.size,
    points
  };
}

function normalizeSnapshotPeriod(periodValue) {
  return normalizeMarketMoversPeriod(periodValue || 'last-date');
}

function nowIso() {
  return new Date().toISOString();
}

async function ensureSnapshotTables() {
  const createTickerDetailsSnapshot = `
    CREATE TABLE IF NOT EXISTS \`${TICKER_DETAILS_SNAPSHOT_FQN}\` (
      snapshot_ts TIMESTAMP NOT NULL,
      as_of_date DATE NOT NULL,
      index_name STRING NOT NULL,
      period STRING NOT NULL,
      row_num INT64,
      symbol STRING NOT NULL,
      security STRING,
      sector STRING,
      industry STRING,
      total_return_percentage FLOAT64,
      price FLOAT64,
      signal STRING,
      weight FLOAT64
    )
    PARTITION BY DATE(snapshot_ts)
    CLUSTER BY index_name, period, symbol
  `;
  const createIndexMoversSnapshot = `
    CREATE TABLE IF NOT EXISTS \`${INDEX_MOVERS_SNAPSHOT_FQN}\` (
      snapshot_ts TIMESTAMP NOT NULL,
      as_of_date DATE NOT NULL,
      index_name STRING NOT NULL,
      period STRING NOT NULL,
      symbol STRING NOT NULL,
      company_name STRING,
      sector STRING,
      industry STRING,
      day_return_pct FLOAT64,
      last_price FLOAT64,
      price_change FLOAT64,
      volume FLOAT64,
      relative_volume_10d FLOAT64,
      relative_volume_is_estimated BOOL
    )
    PARTITION BY DATE(snapshot_ts)
    CLUSTER BY index_name, period, symbol
  `;
  await bigquery.query({ query: createTickerDetailsSnapshot });
  await bigquery.query({ query: createIndexMoversSnapshot });
  const createIndexReturnsSnapshot = `
    CREATE TABLE IF NOT EXISTS \`${INDEX_RETURNS_SNAPSHOT_FQN}\` (
      snapshot_ts TIMESTAMP NOT NULL,
      as_of_date DATE NOT NULL,
      index_key STRING NOT NULL,
      payload_json STRING NOT NULL
    )
    PARTITION BY DATE(snapshot_ts)
    CLUSTER BY index_key
  `;
  await bigquery.query({ query: createIndexReturnsSnapshot });
}

function normalizeIndexReturnsSnapshotKey(indexValue) {
  const low = String(indexValue || '')
    .trim()
    .toLowerCase();
  if (!low || !SNAPSHOT_INDEX_RETURNS_KEY_SET.has(low)) return null;
  return low;
}

async function writeIndexReturnsSnapshot(indexKeyLower, payload, asOfDate, snapshotTs) {
  const indexKey = String(indexKeyLower || '')
    .trim()
    .toLowerCase();
  if (!indexKey) return;
  const deleteQuery = `
    DELETE FROM \`${INDEX_RETURNS_SNAPSHOT_FQN}\`
    WHERE index_key = @indexKey
  `;
  await bigquery.query({ query: deleteQuery, params: { indexKey } });
  const payloadJson = JSON.stringify(payload || {});
  const tsEsc = String(snapshotTs || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const dateEsc = String(asOfDate || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const insertQuery = `
    INSERT INTO \`${INDEX_RETURNS_SNAPSHOT_FQN}\`
    (snapshot_ts, as_of_date, index_key, payload_json)
    VALUES (TIMESTAMP('${tsEsc}'), DATE('${dateEsc}'), @indexKey, @payloadJson)
  `;
  await bigquery.query({
    query: insertQuery,
    params: {
      indexKey,
      payloadJson
    }
  });
}

async function writeTickerDetailsSnapshot(indexValue, periodValue, rows, asOfDate, snapshotTs) {
  const indexName = String(indexValue || '').trim();
  const period = normalizeSnapshotPeriod(periodValue);
  const deleteQuery = `
    DELETE FROM \`${TICKER_DETAILS_SNAPSHOT_FQN}\`
    WHERE index_name = @indexName AND period = @period
  `;
  await bigquery.query({ query: deleteQuery, params: { indexName, period } });
  if (!Array.isArray(rows) || !rows.length) return;
  const values = rows
    .map((r) => `(
      TIMESTAMP('${snapshotTs}'),
      DATE('${asOfDate}'),
      '${String(indexName).replace(/'/g, "\\'")}',
      '${String(period).replace(/'/g, "\\'")}',
      ${Number.isFinite(Number(r.row)) ? Number(r.row) : 'NULL'},
      '${String(r.symbol || '').replace(/'/g, "\\'")}',
      '${String(r.security || '').replace(/'/g, "\\'")}',
      '${String(r.sector || '').replace(/'/g, "\\'")}',
      '${String(r.industry || '').replace(/'/g, "\\'")}',
      ${Number.isFinite(Number(r.totalReturnPercentage)) ? Number(r.totalReturnPercentage) : 'NULL'},
      ${Number.isFinite(Number(r.price)) ? Number(r.price) : 'NULL'},
      '${String(r.signal || 'N').replace(/'/g, "\\'")}',
      ${Number.isFinite(Number(r.weight)) ? Number(r.weight) : 'NULL'}
    )`)
    .join(',\n');
  const insertQuery = `
    INSERT INTO \`${TICKER_DETAILS_SNAPSHOT_FQN}\`
    (snapshot_ts, as_of_date, index_name, period, row_num, symbol, security, sector, industry, total_return_percentage, price, signal, weight)
    VALUES ${values}
  `;
  await bigquery.query({ query: insertQuery });
}

async function writeIndexMoversSnapshot(indexValue, periodValue, points, asOfDate, snapshotTs) {
  const indexName = String(indexValue || '').trim();
  const period = normalizeSnapshotPeriod(periodValue);
  const deleteQuery = `
    DELETE FROM \`${INDEX_MOVERS_SNAPSHOT_FQN}\`
    WHERE index_name = @indexName AND period = @period
  `;
  await bigquery.query({ query: deleteQuery, params: { indexName, period } });
  if (!Array.isArray(points) || !points.length) return;
  const values = points
    .map((p) => `(
      TIMESTAMP('${snapshotTs}'),
      DATE('${asOfDate}'),
      '${String(indexName).replace(/'/g, "\\'")}',
      '${String(period).replace(/'/g, "\\'")}',
      '${String(p.symbol || '').replace(/'/g, "\\'")}',
      '${String(p.companyName || '').replace(/'/g, "\\'")}',
      '${String(p.sector || '').replace(/'/g, "\\'")}',
      '${String(p.industry || '').replace(/'/g, "\\'")}',
      ${Number.isFinite(Number(p.dayReturnPct)) ? Number(p.dayReturnPct) : 'NULL'},
      ${Number.isFinite(Number(p.lastPrice)) ? Number(p.lastPrice) : 'NULL'},
      ${Number.isFinite(Number(p.priceChange)) ? Number(p.priceChange) : 'NULL'},
      ${Number.isFinite(Number(p.volume)) ? Number(p.volume) : 'NULL'},
      ${Number.isFinite(Number(p.relativeVolume10d)) ? Number(p.relativeVolume10d) : 'NULL'},
      ${p.relativeVolumeIsEstimated ? 'TRUE' : 'FALSE'}
    )`)
    .join(',\n');
  const insertQuery = `
    INSERT INTO \`${INDEX_MOVERS_SNAPSHOT_FQN}\`
    (snapshot_ts, as_of_date, index_name, period, symbol, company_name, sector, industry, day_return_pct, last_price, price_change, volume, relative_volume_10d, relative_volume_is_estimated)
    VALUES ${values}
  `;
  await bigquery.query({ query: insertQuery });
}

async function buildMarketSnapshots() {
  await ensureSnapshotTables();
  const snapshotTs = nowIso();
  const asOfDate = isoDate(await getMaxDate());
  for (const indexName of SNAPSHOT_SUPPORTED_INDICES) {
    for (const period of SNAPSHOT_SUPPORTED_PERIODS) {
      const details = await getTickerDetailsByIndex(indexName, period);
      await writeTickerDetailsSnapshot(indexName, period, details, asOfDate, snapshotTs);
      const movers = await calculateIndexMarketMovers(indexName, period);
      await writeIndexMoversSnapshot(indexName, period, movers.points || [], asOfDate, snapshotTs);
    }
  }
  for (const indexApi of SNAPSHOT_INDEX_RETURNS_KEYS) {
    const keyLower = normalizeIndexReturnsSnapshotKey(indexApi);
    if (!keyLower) continue;
    try {
      const payload = await calculateIndexReturns(indexApi, null, 1980);
      await writeIndexReturnsSnapshot(keyLower, payload, asOfDate, snapshotTs);
    } catch (err) {
      console.error(
        `[snapshot-refresher] index-returns snapshot failed index="${indexApi}":`,
        err?.message || err
      );
    }
  }
  return {
    success: true,
    snapshotTs,
    asOfDate,
    indices: SNAPSHOT_SUPPORTED_INDICES.length,
    periods: SNAPSHOT_SUPPORTED_PERIODS.length
  };
}

async function readTickerDetailsSnapshot(indexValue, periodValue) {
  const indexName = String(indexValue || '').trim();
  const period = normalizeSnapshotPeriod(periodValue);
  const q = `
    SELECT snapshot_ts, as_of_date, row_num, symbol, security, sector, industry, total_return_percentage, price, signal, weight
    FROM \`${TICKER_DETAILS_SNAPSHOT_FQN}\`
    WHERE index_name = @indexName
      AND period = @period
    ORDER BY row_num ASC, symbol ASC
  `;
  const [rows] = await bigquery.query({ query: q, params: { indexName, period } });
  if (!rows || !rows.length) return null;
  const snapshotTs = rows[0].snapshot_ts?.value || rows[0].snapshot_ts || null;
  const asOfDate = rows[0].as_of_date?.value || rows[0].as_of_date || null;
  const data = rows.map((r, i) => ({
    row: Number(r.row_num) || i + 1,
    symbol: String(r.symbol || ''),
    security: String(r.security || ''),
    sector: String(r.sector || ''),
    industry: String(r.industry || ''),
    index: indexName,
    totalReturnPercentage: r.total_return_percentage != null ? Number(r.total_return_percentage) : null,
    price: r.price != null ? Number(r.price) : null,
    signal: String(r.signal || 'N'),
    weight: r.weight != null ? Number(r.weight) : null
  }));
  return { snapshotTs, asOfDate, data };
}

async function readIndexMarketMoversSnapshot(indexValue, periodValue) {
  const indexName = String(indexValue || '').trim();
  const period = normalizeSnapshotPeriod(periodValue);
  const q = `
    SELECT snapshot_ts, as_of_date, symbol, company_name, sector, industry, day_return_pct, last_price, price_change, volume, relative_volume_10d, relative_volume_is_estimated
    FROM \`${INDEX_MOVERS_SNAPSHOT_FQN}\`
    WHERE index_name = @indexName
      AND period = @period
    ORDER BY symbol ASC
  `;
  const [rows] = await bigquery.query({ query: q, params: { indexName, period } });
  if (!rows || !rows.length) return null;
  const snapshotTs = rows[0].snapshot_ts?.value || rows[0].snapshot_ts || null;
  const asOfDate = rows[0].as_of_date?.value || rows[0].as_of_date || null;
  const points = rows.map((r) => ({
    symbol: String(r.symbol || ''),
    companyName: String(r.company_name || ''),
    sector: String(r.sector || ''),
    industry: String(r.industry || ''),
    dayReturnPct: r.day_return_pct != null ? Number(r.day_return_pct) : null,
    lastPrice: r.last_price != null ? Number(r.last_price) : null,
    priceChange: r.price_change != null ? Number(r.price_change) : null,
    volume: r.volume != null ? Number(r.volume) : null,
    relativeVolume10d: r.relative_volume_10d != null ? Number(r.relative_volume_10d) : 1,
    relativeVolumeIsEstimated: Boolean(r.relative_volume_is_estimated)
  }));
  return { snapshotTs, asOfDate, points };
}

async function readIndexReturnsSnapshot(indexValue) {
  const indexKey = normalizeIndexReturnsSnapshotKey(indexValue);
  if (!indexKey) return null;
  const q = `
    SELECT snapshot_ts, as_of_date, payload_json
    FROM \`${INDEX_RETURNS_SNAPSHOT_FQN}\`
    WHERE index_key = @indexKey
    ORDER BY snapshot_ts DESC
    LIMIT 1
  `;
  const [rows] = await bigquery.query({ query: q, params: { indexKey } });
  if (!rows || !rows.length) return null;
  const snapshotTs = rows[0].snapshot_ts?.value || rows[0].snapshot_ts || null;
  const asOfDate = rows[0].as_of_date?.value || rows[0].as_of_date || null;
  let payload = null;
  try {
    const raw = rows[0].payload_json;
    const s = raw != null && typeof raw === 'object' && raw.value != null ? String(raw.value) : String(raw || '');
    payload = s ? JSON.parse(s) : null;
  } catch {
    return null;
  }
  if (!payload || typeof payload !== 'object') return null;
  return { snapshotTs, asOfDate, payload };
}

module.exports = {
  getUniqueIndices,
  getPeriodOptions,
  calculatePeriodDates,
  computeRollingWindowStart,
  mapIndexValueToDbIndex,
  getTickerDetailsByIndex,
  calculateAllReturns,
  calculateReturnsSections,
  calculateIndexReturns,
  calculateTotalReturnPercentage,
  calculateIndexConstituentLeaderboards,
  calculateIndexMarketMovers,
  normalizeMarketMoversPeriod,
  normalizeSnapshotPeriod,
  ensureSnapshotTables,
  buildMarketSnapshots,
  readTickerDetailsSnapshot,
  readIndexMarketMoversSnapshot,
  readIndexReturnsSnapshot
};

