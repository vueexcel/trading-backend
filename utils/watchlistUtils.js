const bigquery = require('../config/bigquery');
const supabase = require('../config/supabase');

// === BigQuery table constants ===
const OHLC_TABLE = '`extended-byway-454621-s6.sp500data1.DailyOHLC200MAData`';
const HISTORY_TABLE = '`extended-byway-454621-s6.sp500data1.StockData3Year200DA`';

const DASHBOARD_TABLES = {
  'Dow Jones': {
    main: '`extended-byway-454621-s6.sp500data1.Dashboard_dowJones`',
    ma1: '`extended-byway-454621-s6.sp500data1.DowJones_200MA_Dashboard1`',
    ma2: '`extended-byway-454621-s6.sp500data1.DowJones_200MA_Dashboard2`'
  },
  'Nasdaq 100': {
    main: '`extended-byway-454621-s6.sp500data1.Dashboard_Nasdaq100`',
    ma1: '`extended-byway-454621-s6.sp500data1.Nasdaq_200MA_Dashboard1`',
    ma2: '`extended-byway-454621-s6.sp500data1.Nasdaq_200MA_Dashboard2`'
  },
  SP500: {
    main: '`extended-byway-454621-s6.sp500data1.Dashboard_SP500`',
    ma1: '`extended-byway-454621-s6.sp500data1.SP500_200MA_Dashboard1`',
    ma2: '`extended-byway-454621-s6.sp500data1.SP500_200MA_Dashboard2`'
  },
  ETF: {
    main: '`extended-byway-454621-s6.sp500data1.Dashboard_ETF`',
    ma1: '`extended-byway-454621-s6.sp500data1.ETF_200MA_Dashboard1`',
    ma2: '`extended-byway-454621-s6.sp500data1.ETF_200MA_Dashboard2`'
  },
  Other: {
    main: '`extended-byway-454621-s6.sp500data1.Dashboard_Other`',
    ma1: '`extended-byway-454621-s6.sp500data1.Other_200MA_Dashboard1`',
    ma2: '`extended-byway-454621-s6.sp500data1.Other_200MA_Dashboard2`'
  }
};

// Map common market_groups.name variations to DASHBOARD_TABLES keys
const CATEGORY_ALIASES = {
  's&p 500': 'SP500',
  's&p500': 'SP500',
  'sp 500': 'SP500',
  'nasdaq': 'Nasdaq 100',
  'nasdaq 100': 'Nasdaq 100',
  'dow jones': 'Dow Jones',
  'dow': 'Dow Jones',
  'etf': 'ETF',
  'other': 'Other'
};

function normalizeCategory(name) {
  if (!name || typeof name !== 'string') return 'Other';
  const trimmed = name.trim();
  const key = trimmed.toLowerCase();
  if (CATEGORY_ALIASES[key]) return CATEGORY_ALIASES[key];
  return DASHBOARD_TABLES[trimmed] !== undefined ? trimmed : 'Other';
}

// helper to convert symbol list to BigQuery array literal
function bqArray(arr) {
  return arr.map(s => `'${s.replace(/'/g, "\\'")}'`).join(',');
}

// 1. fetch tickers with metadata including category
async function getTickersInfoByIds(tickerIds) {
  if (!tickerIds || tickerIds.length === 0) return [];

  // fetch tickers along with their group_id
  const { data: tickRows, error: tickErr } = await supabase
    .from('tickers')
    .select(`
      id,
      symbol,
      company_name,
      ticker_groups(group_id)
    `)
    .in('id', tickerIds);
  if (tickErr) throw tickErr;

  // fetch all relevant market_groups once
  const { data: groups, error: grpErr } = await supabase
    .from('market_groups')
    .select('id,name,code');
  if (grpErr) throw grpErr;
  const groupMap = new Map(groups.map(g => [g.id, g]));

  return tickRows.map(row => {
    // Supabase may return ticker_groups as array (e.g. [{ group_id: '...' }]) or single object
    const tg = row.ticker_groups;
    const gid = Array.isArray(tg) ? tg[0]?.group_id : tg?.group_id;
    const grp = groupMap.get(gid) || {};
    const rawCategory = grp.name || 'Other';
    return {
      ticker_id: row.id,
      symbol: row.symbol,
      company_name: row.company_name,
      category: normalizeCategory(rawCategory),
      category_code: grp.code || ''
    };
  });
} 

// 2. fetch tickers belonging to a default group
async function getTickersByGroupId(groupId) {
  // fetch tickers with their groups, then filter by the specific group
  const { data: tickRows, error: tickErr } = await supabase
    .from('tickers')
    .select(`
      id,
      symbol,
      company_name,
      ticker_groups(group_id)
    `);

  if (tickErr) throw tickErr;

  // filter to only tickers that have this group_id
  const filteredTicks = tickRows.filter(row => {
    return row.ticker_groups && row.ticker_groups.some(tg => tg.group_id === groupId);
  });

  // get the group metadata
  const { data: groups, error: grpErr } = await supabase
    .from('market_groups')
    .select('id,name,code')
    .eq('id', groupId)
    .single();
  if (grpErr) throw grpErr;
  const grp = groups || {};

  return filteredTicks.map(row => ({
    ticker_id: row.id,
    symbol: row.symbol,
    company_name: row.company_name,
    category: grp.name || 'Other',
    category_code: grp.code || ''
  }));
}

// 3. Query OHLC latest for symbols
async function fetchOHLC(symbols) {
  if (!symbols || symbols.length === 0) return {};
  // table columns: ticker, market_date, open_price, high_price, low_price, close_price, adj_close, dma_200
  const q = `
    SELECT ticker, market_date AS date,
           open_price AS open,
           high_price AS high,
           low_price AS low,
           close_price AS close,
           adj_close AS adjclose,
           dma_200 AS dma200
    FROM ${OHLC_TABLE}
    WHERE ticker IN (${bqArray(symbols)})
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY market_date DESC) = 1
  `;
  const [job] = await bigquery.createQueryJob({ query: q });
  const [rows] = await job.getQueryResults();
  const map = new Map();
  rows.forEach(r => {
    map.set(r.ticker.toUpperCase(), r);
  });
  return map;
}

// 4. Query history changes
async function fetchHistoryChanges(symbols) {
  if (!symbols || symbols.length === 0) return new Map();
  // history table has same structure as OHLC; use close_price for price
  const q = `
    WITH ranked AS (
      SELECT ticker, CAST(close_price AS FLOAT64) AS price, market_date AS date,
             ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY market_date DESC) AS rn
      FROM ${HISTORY_TABLE}
      WHERE ticker IN (${bqArray(symbols)})
    )
    SELECT r1.ticker, r1.price AS latest_price, r2.price AS prev_price
    FROM ranked r1
    LEFT JOIN ranked r2 ON r1.ticker = r2.ticker AND r2.rn = 2
    WHERE r1.rn = 1
  `;
  const [job] = await bigquery.createQueryJob({ query: q });
  const [rows] = await job.getQueryResults();
  const map = new Map();
  rows.forEach(r => {
    const diff = (r.prev_price || 0) === 0 ? 0 : r.latest_price - r.prev_price;
    const pct = r.prev_price ? diff / r.prev_price : 0;
    map.set(r.ticker.toUpperCase(), { diff, pct });
  });
  return map;
}

// 5. fetch signals for a category
async function fetchSignals(symbols, category) {
  const result = { main: new Map(), ma1: new Map(), ma2: new Map() };
  if (!symbols || symbols.length === 0) return result;
  const normalizedCat = normalizeCategory(category);
  const tbl = DASHBOARD_TABLES[normalizedCat] || DASHBOARD_TABLES.Other;
  
  // dashboard tables have signal_type and signal_days columns
  for (const key of ['main', 'ma1', 'ma2']) {
    const tablePath = tbl[key];
    const q = `
      SELECT DISTINCT ticker, signal_type
      FROM ${tablePath}
      WHERE ticker IN (${bqArray(symbols)})
      ORDER BY ticker, signal_type
    `;
    try {
      const [job] = await bigquery.createQueryJob({ query: q });
      const [rows] = await job.getQueryResults();
      // map tickers to their signals (concatenate if multiple)
      const sigMap = new Map();
      rows.forEach(r => {
        if (!sigMap.has(r.ticker)) sigMap.set(r.ticker, []);
        if (r.signal_type) sigMap.get(r.ticker).push(r.signal_type);
      });
      sigMap.forEach((sigs, ticker) => {
        result[key].set(ticker.toUpperCase(), sigs.length > 0 ? sigs.join(', ') : '-');
      });
      // ensure all input symbols have an entry
      symbols.forEach(s => {
        if (!result[key].has(s.toUpperCase())) result[key].set(s.toUpperCase(), '-');
      });
    } catch (e) {
      console.warn(`signal query error for category="${category}" table=${key}:`, e.message);
      // fallback: set all inputs to '-'
      symbols.forEach(s => result[key].set(s.toUpperCase(), '-'));
    }
  }
  return result;
}

// 6. build watchlist rows given an array of ticker metadata objects
async function buildWatchlistRows(tickerEntities) {
  const symbols = tickerEntities.map(t => t.symbol.toUpperCase());
  const ohlcMap = await fetchOHLC(symbols);
  const changeMap = await fetchHistoryChanges(symbols);

  // group by category for signal queries
  const byCategory = {};
  tickerEntities.forEach(t => {
    if (!byCategory[t.category]) byCategory[t.category] = [];
    byCategory[t.category].push(t.symbol.toUpperCase());
  });

  const signalMaps = {};
  for (const cat of Object.keys(byCategory)) {
    signalMaps[cat] = await fetchSignals(byCategory[cat], cat);
  }

  // hyperlink templates
  const LINK_TEMPLATES = {
    tv: 'https://www.tradingview.com/chart/?symbol=',
    sc: 'https://stockcharts.com/h-sc/ui?s=',
    yf: 'https://finance.yahoo.com/quote/'
  };

  return tickerEntities.map(t => {
    const ticker = t.symbol.toUpperCase();
    const ohlc = ohlcMap.get(ticker) || {};
    const change = changeMap.get(ticker) || { diff: 0, pct: 0 };
    const sigs = signalMaps[t.category] || { main: new Map(), ma1: new Map(), ma2: new Map() };
    const s1 = sigs.main.get(ticker) || '-';
    const s2 = sigs.ma1.get(ticker) || '-';
    const s3 = sigs.ma2.get(ticker) || '-';

    const tvLink = `${LINK_TEMPLATES.tv}${ticker}`;
    const scLink = `${LINK_TEMPLATES.sc}${ticker}`;
    const yfLink = `${LINK_TEMPLATES.yf}${ticker}`;

    return {
      ticker_id: t.ticker_id,
      symbol: ticker,
      category: t.category,
      ohlc_date: ohlc.date || null,
      open: ohlc.open || null,
      high: ohlc.high || null,
      low: ohlc.low || null,
      close: ohlc.close || null,
      adj_close: ohlc.adjclose || null,
      dma200: ohlc.dma200 || null,
      signal_main: s1,
      signal_ma1: s2,
      signal_ma2: s3,
      change_diff: change.diff,
      change_pct: change.pct,
      link_tv: tvLink,
      link_sc: scLink,
      link_yf: yfLink
    };
  });
}

module.exports = {
  getTickersInfoByIds,
  getTickersByGroupId,
  buildWatchlistRows
};
