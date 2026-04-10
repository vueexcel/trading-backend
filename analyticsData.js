const bigquery = require('./config/bigquery');
const supabase = require('./config/supabase');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || 'extended-byway-454621-s6';
const BIGQUERY_DATASET = process.env.BIGQUERY_DATASET || 'sp500data1';
const TICKER_DETAILS_TABLE = process.env.TICKER_DETAILS_TABLE || 'TickerDetails';
const BIGQUERY_TABLE = process.env.BIGQUERY_TABLE || 'stock_all_data';

const TICKER_DETAILS_FQN = `${PROJECT_ID}.${BIGQUERY_DATASET}.${TICKER_DETAILS_TABLE}`;
const TABLE_FQN = `${PROJECT_ID}.${BIGQUERY_DATASET}.${BIGQUERY_TABLE}`;

const DAYS_IN_YEAR = 365.25;

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function parseBqDate(v) {
  const raw = v && (v.value || v);
  const dt = raw ? new Date(raw) : null;
  return dt && !Number.isNaN(dt.getTime()) ? dt : null;
}

async function getMaxDate() {
  const query = `SELECT MAX(Date) AS max_date FROM \`${TABLE_FQN}\``;
  const [rows] = await bigquery.query({ query });
  const dt = rows && rows[0] ? parseBqDate(rows[0].max_date) : null;
  if (!dt) throw new Error('No dates found in price table.');
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
  ];
  return periodDefs.map(p => ({ value: p.value, label: p.name }));
}

async function calculatePeriodDates(periodValue) {
  const endDate = await getMaxDate();
  const startDate = new Date(endDate);

  switch ((periodValue || '').toLowerCase()) {
    case 'last-date':
      startDate.setDate(endDate.getDate() - 1);
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
    case 'last-1-year':
    default:
      startDate.setDate(endDate.getDate() - Math.floor(1 * 365));
      break;
  }

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
  const envCol = (process.env.TICKER_DETAILS_SYMBOL_COLUMN || '').trim();
  const columnsToTry = envCol
    ? [envCol]
    : ['Symbol', 'Ticker'];

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
    SELECT Date, Ticker, Close as Close_raw
    FROM \`${TABLE_FQN}\`
    WHERE Ticker IN (${tickersParam})
      AND Date BETWEEN '${isoDate(extendedStart)}' AND '${isoDate(endDate)}'
    ORDER BY Ticker, Date
  `;
  const [rows] = await bigquery.query({ query });
  if (!rows || rows.length === 0) return [];
  const out = [];
  for (const row of rows) {
    const dt = parseBqDate(row.Date);
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

function getStartEndClose(priceData, ticker, startDate, endDate) {
  const t = ticker.toUpperCase();
  const tickerData = priceData.filter(row => String(row.Ticker || '').toUpperCase() === t);
  if (!tickerData.length) return [null, null];

  const startPrices = tickerData.filter(row => row.Date >= startDate && row.Close != null);
  const endPrices = tickerData.filter(row => row.Date <= endDate && row.Close != null);

  // Start uses first trading day on/after requested start date.
  const startClose = startPrices.length ? startPrices[0].Close : null;
  // End uses last trading day on/before requested end date.
  const endClose = endPrices.length ? endPrices[endPrices.length - 1].Close : null;

  return [startClose, endClose];
}

function calculateTotalReturnPercentage(startPrice, endPrice) {
  if (startPrice == null || startPrice === 0 || endPrice == null) return null;
  return Math.round(((endPrice - startPrice) / startPrice) * 100 * 100) / 100;
}

async function getTickerDetailsByIndex(indexValue, periodValue) {
  const [startDate, endDate] = await calculatePeriodDates(periodValue);

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

  return rows
    .map((row, idx) => {
      const symbol = row.Symbol ? String(row.Symbol).toUpperCase() : '';
      if (!symbol) return null;

      const [startClose, endClose] = getStartEndClose(priceData, symbol, startDate, endDate);
      const totalReturnPct = calculateTotalReturnPercentage(startClose, endClose);

      return {
        row: idx + 1,
        symbol,
        security: row.Security || '',
        sector: row.Sector || '',
        industry: row.Industry || '',
        index: groupMeta ? groupMeta.name : row.Index || '',
        totalReturnPercentage: totalReturnPct,
        price: endClose
      };
    })
    .filter(Boolean);
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

async function calculateAllReturns(ticker, includePredefined = true, includeAnnual = true, customRange = null, annualFromYear = 1970) {
  const endDate = await getMaxDate();
  const t = (ticker || '').toString().trim().toUpperCase();

  // Pull enough data once for all calculations.
  const minDt = await getMinDateForTicker(t);
  const startYear = Math.max(minDt.getFullYear(), annualFromYear);
  const earliest = new Date(startYear, 0, 1);
  const prices = await fetchCloseSeries(t, earliest, endDate);

  const dynamic = await calculateDynamicPeriods(t, endDate, prices);
  const predefined = includePredefined ? await calculatePredefinedPeriods(t, endDate, prices) : [];
  const annual = includeAnnual ? await calculateAnnualReturns(t, endDate, minDt.getFullYear(), prices) : [];
  const monthly = await calculateMonthlyReturns(t, endDate, startYear, prices);
  const quarterly = await calculateQuarterlyReturns(t, endDate, startYear, prices);

  let custom = null;
  if (customRange && customRange.length === 2) {
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

module.exports = {
  getUniqueIndices,
  getPeriodOptions,
  calculatePeriodDates,
  mapIndexValueToDbIndex,
  getTickerDetailsByIndex,
  calculateAllReturns,
  calculateTotalReturnPercentage
};

