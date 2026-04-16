// const bigquery = require('../config/bigquery');

// // GET /api/market/ohlc?symbol=AAPL
// const getStockData = async (req, res) => {
//     const { symbol } = req.query; // Get symbol from URL

//     if (!symbol) {
//         return res.status(400).json({ error: 'Symbol is required' });
//     }

//     try {
//         // SQL Query to run inside BigQuery
//         // Note: We use `LIMIT 100` to save costs during testing
//         const query = `
//             SELECT * 
//             FROM \`market_data.ohlc_data\`
//             ORDER BY trade_date DESC
//             LIMIT 100
//         `;

//         // Run the query
//         const [rows] = await bigquery.query({ query });

//         res.status(200).json({ 
//             symbol: symbol.toUpperCase(), 
//             data: rows 
//         });
//     } catch (error) {
//         console.error('BigQuery Error:', error);
//         res.status(500).json({ error: 'Failed to fetch market data' });
//     }
// };

// module.exports = { getStockData };
const bigquery = require('../config/bigquery');
const analyticsData = require('../analyticsData');
const { makeCacheKey, getCache, setCache } = require('../utils/cache');
const fs = require('fs');
const path = require('path');

// you can override these with environment variables if you need a different
// project / dataset / table for testing or production.  leave them hard- coded
// if you only ever query the single dataset we ship with the sample data.
const PROJECT_ID = process.env.GCP_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || 'extended-byway-454621-s6';
const DATASET = process.env.BIGQUERY_DATASET || 'sp500data1';
const TABLE = process.env.BIGQUERY_TABLE || 'stock_all_data';
const TABLE_FQN = `${PROJECT_ID}.${DATASET}.${TABLE}`; // used in backticks below
// Same signal source as Odin summary; override with OHLC_SIGNALS_TABLE_FQN if needed (full `project.dataset.table` in backticks).
const OHLC_SIGNALS_TABLE_FQN =
    process.env.OHLC_SIGNALS_TABLE_FQN || '`extended-byway-454621-s6.sp500data1.consolidated_testing_2`';
const MA200_TABLE_FQN =
    process.env.MA200_TABLE_FQN || '`extended-byway-454621-s6.sp500data1.200MA_consolidated`';

const OHLC_MAX_LIMIT = 500;
const OHLC_DEFAULT_LIMIT = 100;
const OHLC_CACHE_TTL_SECS = Number(process.env.OHLC_CACHE_TTL_SECS || 120);
const OHLC_SIGNALS_INDICATOR_CACHE_TTL_SECS = Number(process.env.OHLC_SIGNALS_INDICATOR_CACHE_TTL_SECS || 120);
const TICKER_DETAILS_CACHE_TTL_SECS = Number(process.env.TICKER_DETAILS_CACHE_TTL_SECS || 300);
/** Max inclusive calendar span for ohlc-signals-indicator (raise via OHLC_SIGNALS_MAX_RANGE_DAYS). */
const OHLC_SIGNALS_MAX_RANGE_DAYS = Number(process.env.OHLC_SIGNALS_MAX_RANGE_DAYS || 40000);
const WEIGHTS_JSON_PATH = path.resolve(__dirname, '..', 'data', 'index-weights.json');

let weightsCache = null;
let weightsCacheMtime = 0;

function normalizeKey(s) {
    return String(s || '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
}

/**
 * `index-weights.json` rows from Slickcharts sometimes store symbols as markdown links:
 * `[AAPL](https://...)`. API ticker rows use plain `AAPL`. Normalize for lookup.
 */
function normalizeTickerFromWeightCell(raw) {
    const s = String(raw || '').trim();
    if (!s) return '';
    const md = s.match(/^\[([A-Za-z0-9.\-]+)\]\s*\(/);
    if (md) return md[1].toUpperCase();
    return s.replace(/[^A-Za-z0-9.\-]/g, '').toUpperCase() || s.toUpperCase().trim();
}

function indexToWeightSource(indexValue) {
    const k = normalizeKey(indexValue);
    if (k === 'dow jones' || k === 'dow' || k === 'dow jones 30' || k === 'djia') return 'dowjones';
    if (k === 'sp500' || k === 's&p 500' || k === 's&p500' || k === 'sp 500') return 'sp500';
    if (k === 'nasdaq 100' || k === 'nasdaq100' || k === 'ndx' || k === 'nasdaq-100') return 'nasdaq100';
    if (k === 'all stocks') return 'sp500';
    return null;
}

function fallbackWeightFromRow(row) {
    const p = Number(row?.price ?? row?.Price);
    if (Number.isFinite(p) && p > 0) {
        return Math.max(Math.pow(p, 0.82), 6);
    }
    return 6;
}

function loadIndexWeights() {
    try {
        const st = fs.statSync(WEIGHTS_JSON_PATH);
        if (weightsCache && weightsCacheMtime === st.mtimeMs) return weightsCache;
        const raw = fs.readFileSync(WEIGHTS_JSON_PATH, 'utf8');
        const parsed = JSON.parse(raw);
        weightsCache = parsed;
        weightsCacheMtime = st.mtimeMs;
        return parsed;
    } catch {
        return null;
    }
}

function buildWeightMaps(allWeights, sourceKey) {
    const bySource = new Map();
    const byAnySource = new Map();
    const sources = allWeights?.sources && typeof allWeights.sources === 'object' ? allWeights.sources : {};
    const keys = Object.keys(sources);
    for (const k of keys) {
        const srcRows = sources[k];
        if (!Array.isArray(srcRows) || !srcRows.length) continue;
        const m = new Map();
        for (const r of srcRows) {
            const sym = normalizeTickerFromWeightCell(r.symbol);
            const w = Number(r.weight);
            if (!sym || !Number.isFinite(w) || w <= 0) continue;
            m.set(sym, w);
            // Keep largest weight seen for overlapping symbols across indices.
            const prev = byAnySource.get(sym);
            if (!Number.isFinite(prev) || w > prev) byAnySource.set(sym, w);
        }
        bySource.set(k, m);
    }
    return {
        bySymbol: sourceKey ? bySource.get(sourceKey) || new Map() : new Map(),
        byAnySource
    };
}

function enrichRowsWithIndexWeights(indexValue, rows) {
    if (!Array.isArray(rows) || !rows.length) return rows;
    const sourceKey = indexToWeightSource(indexValue);
    const allWeights = loadIndexWeights();
    const { bySymbol, byAnySource } = buildWeightMaps(allWeights, sourceKey);

    // Always emit a `weight` for treemap sizing; if index weight is missing, return 0 (no backend fallback).
    let matchedSource = 0;
    let matchedAny = 0;
    let zeroWeight = 0;
    const verbose = process.env.DEBUG_INDEX_WEIGHTS_VERBOSE === '1';
    const out = rows.map((r) => {
        const sym = String(r.symbol || r.Symbol || '').toUpperCase().trim();
        const wSource = bySymbol.get(sym);
        const wAny = byAnySource.get(sym);
        let wf = null;
        let sourceUsed = 'none';
        if (Number.isFinite(wSource) && wSource > 0) {
            wf = wSource;
            matchedSource += 1;
            sourceUsed = sourceKey || 'source';
        } else if (Number.isFinite(wAny) && wAny > 0) {
            wf = wAny;
            matchedAny += 1;
            sourceUsed = 'any-source';
        } else {
            wf = 0;
            zeroWeight += 1;
            sourceUsed = 'zero';
        }
        if (verbose) {
            // eslint-disable-next-line no-console
            console.log(
                `[index-weights:ticker] index="${indexValue}" symbol=${sym} ` +
                    `sourceWeight=${Number.isFinite(wSource) ? wSource : 'na'} ` +
                    `anySourceWeight=${Number.isFinite(wAny) ? wAny : 'na'} ` +
                    `finalWeight=${wf} sourceUsed=${sourceUsed}`
            );
        }
        return { ...r, weight: wf };
    });

    if (process.env.DEBUG_INDEX_WEIGHTS === '1') {
        // eslint-disable-next-line no-console
        console.log(
            `[index-weights] index="${indexValue}" source=${sourceKey || 'none'} ` +
                `sourceMap=${bySymbol.size} anySourceMap=${byAnySource.size} rows=${rows.length} ` +
                `matchedSource=${matchedSource} matchedAnySource=${matchedAny} zeroWeight=${zeroWeight}`
        );
    }

    return out;
}

function bqCellToPlain(v) {
    if (v == null) return null;
    if (typeof v === 'object' && v.value !== undefined) return v.value;
    if (v instanceof Date) return v.toISOString().split('T')[0];
    return v;
}

function rowDateKeyFromBQ(rowDate) {
    const raw = bqCellToPlain(rowDate);
    if (raw == null) return '';
    if (typeof raw === 'string') return raw.slice(0, 10);
    const d = new Date(raw);
    return Number.isNaN(d.getTime()) ? '' : d.toISOString().split('T')[0];
}

function normalizeIndicatorSignal(sig) {
    if (sig == null || sig === '') return 'N';
    const s = String(bqCellToPlain(sig) ?? sig).trim().toUpperCase();
    if (!s || s === 'NULL') return 'N';
    return s;
}

/**
 * POST /api/market/ohlc-signals-indicator
 * Body: { ticker, start_date, end_date }
 * OHLC from stock_all_data (TABLE_FQN), signals from consolidated_testing_2.
 */
const getOhlcSignalsIndicator = async (req, res) => {
    const data = req.body || {};
    const ticker = (data.ticker || '').toString().trim();
    const startStr = (data.start_date || '').toString().trim();
    const endStr = (data.end_date || '').toString().trim();

    if (!ticker) {
        return res.status(400).json({ success: false, error: 'Missing required field: ticker' });
    }
    if (!startStr || !endStr) {
        return res.status(400).json({ success: false, error: 'start_date and end_date are required' });
    }

    const startDt = new Date(startStr);
    const endDt = new Date(endStr);
    if (Number.isNaN(startDt.getTime()) || Number.isNaN(endDt.getTime())) {
        return res.status(400).json({ success: false, error: 'Invalid start_date or end_date' });
    }
    if (startDt > endDt) {
        return res.status(400).json({ success: false, error: 'start_date must be before or equal to end_date' });
    }

    // Inclusive calendar-day span (matches YYYY-MM-DD + T12:00:00 style ranges from the app).
    // NOTE: ceil(deltaMs/day)+1 was one day too large vs the frontend cap and rejected full 10Y windows as 400.
    const msPerDay = 86400000;
    const inclusiveDays = Math.floor((endDt.getTime() - startDt.getTime()) / msPerDay) + 1;
    if (inclusiveDays > OHLC_SIGNALS_MAX_RANGE_DAYS) {
        return res.status(400).json({
            success: false,
            error: `Date range too large (max ${OHLC_SIGNALS_MAX_RANGE_DAYS} calendar days inclusive)`
        });
    }

    const sym = ticker.toUpperCase();

    try {
        const cacheKey = makeCacheKey('market:ohlc-signals-indicator:v2', {
            ticker: sym,
            startStr,
            endStr
        });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json({ ...cached, cache_hit: true });
        }

        const ohlcQuery = `
            SELECT *
            FROM \`${TABLE_FQN}\`
            WHERE Ticker = @ticker
              AND Date BETWEEN @start AND @end
            ORDER BY Date ASC
        `;
        const signalQuery = `
            SELECT \`Date\` AS dt, \`Signal\` AS sig
            FROM ${OHLC_SIGNALS_TABLE_FQN}
            WHERE \`Ticker\` = @ticker
              AND \`Date\` BETWEEN @start AND @end
            ORDER BY \`Date\` ASC
        `;
        const ma200Query = `
            SELECT \`Date\` AS dt, \`DMA_200\` AS dma200
            FROM ${MA200_TABLE_FQN}
            WHERE \`Ticker\` = @ticker
              AND \`Date\` BETWEEN @start AND @end
            ORDER BY \`Date\` ASC
        `;

        const [priceRows, signalRows, ma200Rows] = await Promise.all([
            bigquery.query({
                query: ohlcQuery,
                params: { ticker: sym, start: startStr, end: endStr }
            }).then((r) => r[0] || []),
            bigquery.query({
                query: signalQuery,
                params: { ticker: sym, start: startStr, end: endStr }
            }).then((r) => r[0] || []),
            bigquery.query({
                query: ma200Query,
                params: { ticker: sym, start: startStr, end: endStr }
            }).then((r) => r[0] || [])
        ]);

        const sigByDate = new Map();
        for (const r of signalRows) {
            const key = rowDateKeyFromBQ(r.dt);
            if (!key) continue;
            sigByDate.set(key, normalizeIndicatorSignal(r.sig));
        }

        const rows = (priceRows || []).map((row) => {
            const dateKey = rowDateKeyFromBQ(row.Date);
            const flat = {};
            for (const [k, v] of Object.entries(row)) {
                flat[k] = bqCellToPlain(v);
            }
            return {
                ...flat,
                signal: sigByDate.get(dateKey) || 'N'
            };
        });

        const ma200 = (ma200Rows || [])
            .map((r) => {
                const date = rowDateKeyFromBQ(r.dt);
                const raw = bqCellToPlain(r.dma200);
                const value = raw != null ? Number(raw) : null;
                return { date, value };
            })
            .filter((r) => r.date && r.value != null && !Number.isNaN(r.value));

        const payload = {
            success: true,
            cache_hit: false,
            ticker: sym,
            start_date: startStr,
            end_date: endStr,
            row_count: rows.length,
            data: rows,
            ma200
        };

        await setCache(cacheKey, payload, OHLC_SIGNALS_INDICATOR_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(payload);
    } catch (error) {
        console.error('OHLC signals indicator error:', error);
        res.status(500).json({ success: false, error: 'Failed to fetch OHLC and signals', message: error.message });
    }
};

// GET /api/market/ohlc?symbol=AAPL  or  ?symbol=AAPL&start_date=2024-01-01&end_date=2024-12-31&limit=250
const getStockData = async (req, res) => {
    const { symbol, start_date, end_date, limit: limitParam } = req.query;

    if (!symbol) {
        return res.status(400).json({ error: 'Symbol is required' });
    }

    const sym = symbol.toString().trim().toUpperCase();
    if (!sym) {
        return res.status(400).json({ error: 'Symbol is required' });
    }

    let startDate = (start_date && typeof start_date === 'string') ? start_date.trim() : null;
    let endDate = (end_date && typeof end_date === 'string') ? end_date.trim() : null;
    let limit = OHLC_DEFAULT_LIMIT;
    if (limitParam != null && limitParam !== '') {
        const n = parseInt(limitParam, 10);
        if (!Number.isNaN(n) && n > 0) {
            limit = Math.min(n, OHLC_MAX_LIMIT);
        }
    }

    if (startDate && endDate) {
        const startDt = new Date(startDate);
        const endDt = new Date(endDate);
        if (Number.isNaN(startDt.getTime()) || Number.isNaN(endDt.getTime())) {
            return res.status(400).json({ error: 'Invalid start_date or end_date' });
        }
        if (startDt > endDt) {
            return res.status(400).json({ error: 'start_date must be before or equal to end_date' });
        }
    } else if (startDate || endDate) {
        return res.status(400).json({ error: 'Both start_date and end_date are required when using a date range' });
    }

    try {
        const cacheKey = makeCacheKey('market:ohlc:v1', {
            sym,
            startDate: startDate || '',
            endDate: endDate || '',
            limit
        });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json({ ...cached, cache_hit: true });
        }

        let query;
        const params = { symbol: sym, limit };

        if (startDate && endDate) {
            query = `
                SELECT *
                FROM \`${TABLE_FQN}\`
                WHERE Ticker = @symbol
                  AND Date BETWEEN @start AND @end
                ORDER BY Date DESC
                LIMIT @limit
            `;
            params.start = startDate;
            params.end = endDate;
        } else {
            query = `
                SELECT *
                FROM \`${TABLE_FQN}\`
                WHERE Ticker = @symbol
                ORDER BY Date DESC
                LIMIT @limit
            `;
        }

        const [rows] = await bigquery.query({ query, params });

        const payload = {
            symbol: sym,
            data: rows,
            cache_hit: false
        };
        await setCache(cacheKey, payload, OHLC_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(payload);
    } catch (error) {
        console.error('BigQuery Error:', error);
        res.status(500).json({ error: 'Failed to fetch market data' });
    }
};

/**
 * GET /api/market/ohlc-ticker-bounds?symbol=AAPL
 * First / last trade dates in stock_all_data for chart "ALL" range.
 */
const getOhlcTickerBounds = async (req, res) => {
    const symbol = (req.query.symbol || '').toString().trim();
    if (!symbol) {
        return res.status(400).json({ success: false, error: 'Query parameter symbol is required' });
    }
    const sym = symbol.toUpperCase();
    try {
        const cacheKey = makeCacheKey('market:ohlc-ticker-bounds:v1', { sym });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json({ ...cached, cache_hit: true });
        }
        const row = await _fetchDateRange(sym);
        const min_date = row?.min_date ? rowDateKeyFromBQ(row.min_date) : '';
        const max_date = row?.max_date ? rowDateKeyFromBQ(row.max_date) : '';
        if (!min_date || !max_date) {
            return res.status(404).json({ success: false, error: 'No OHLC data for ticker' });
        }
        const payload = {
            success: true,
            symbol: sym,
            min_date,
            max_date,
            cache_hit: false
        };
        await setCache(cacheKey, payload, OHLC_CACHE_TTL_SECS * 10);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(payload);
    } catch (error) {
        console.error('getOhlcTickerBounds error:', error);
        res.status(500).json({ success: false, error: 'Failed to load ticker date bounds' });
    }
};

// helper used by both endpoints
async function _fetchDateRange(symbol) {
    const dateQuery = `
        SELECT MIN(Date) AS min_date, MAX(Date) AS max_date
        FROM \`${TABLE_FQN}\`
        WHERE Ticker = @symbol
    `;

    const [rows] = await bigquery.query({
        query: dateQuery,
        params: { symbol: symbol.toUpperCase() }
    });
    return rows[0];
}

// POST /api/market/monthly-ohlc
const getMonthlyOHLC = async (req, res) => {
    const data = req.body || {};
    const ticker = (data.ticker || '').trim();
    if (!ticker) {
        return res.status(400).json({ success: false, error: 'Missing required field: ticker' });
    }

    let startDate = (data.start_date || '').trim() || null;
    let endDate = (data.end_date || '').trim() || null;

    try {
        const cacheKey = makeCacheKey('market:monthly-ohlc:v1', {
            ticker: ticker.toUpperCase(),
            startDate: startDate || '',
            endDate: endDate || ''
        });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json({ ...cached, cache_hit: true });
        }

        if (!startDate || !endDate) {
            const { min_date, max_date } = await _fetchDateRange(ticker);
            if (!min_date || !max_date) {
                return res.status(404).json({ success: false, error: 'No data found for the given ticker/date range' });
            }
            if (!startDate) startDate = min_date.toISOString().slice(0, 10);
            if (!endDate) endDate = max_date.toISOString().slice(0, 10);
        }

        const query = `
            WITH stock_data AS (
                SELECT
                    Ticker,
                    Date,
                    Open,
                    High,
                    Low,
                    Close,
                    EXTRACT(YEAR FROM Date) AS year,
                    EXTRACT(MONTH FROM Date) AS month
                FROM \`${TABLE_FQN}\`
                WHERE Ticker = @symbol
                  AND Date BETWEEN @start AND @end
            ),
            monthly_summary AS (
                SELECT
                    Ticker,
                    year,
                    month,
                    ARRAY_AGG(Open ORDER BY Date ASC LIMIT 1)[OFFSET(0)] AS Open,
                    MAX(High) AS High,
                    MIN(Low) AS Low,
                    ARRAY_AGG(Close ORDER BY Date DESC LIMIT 1)[OFFSET(0)] AS Close
                FROM stock_data
                GROUP BY Ticker, year, month
            )
            SELECT *
            FROM monthly_summary
            ORDER BY year, month
        `;

        const [rows] = await bigquery.query({
            query,
            params: {
                symbol: ticker.toUpperCase(),
                start: startDate,
                end: endDate
            }
        });

        // convert BigQuery types to plain JS
        const monthlyOHLC = rows.map(r => {
            const year = Number(r.year);
            const month = Number(r.month);
            const lastDay = new Date(year, month, 0).getDate();
            return {
                ticker: r.Ticker,
                year,
                month,
                open: Number(r.Open),
                high: Number(r.High),
                low: Number(r.Low),
                close: Number(r.Close),
                adj_close: Number(r.Close),
                start_date: `${year}-${month.toString().padStart(2, '0')}-01`,
                end_date: `${year}-${month.toString().padStart(2, '0')}-${lastDay}`
            };
        });

        const payload = { success: true, ticker: ticker.toUpperCase(), monthlyOHLC, cache_hit: false };
        await setCache(cacheKey, payload, OHLC_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(payload);
    } catch (error) {
        console.error('BigQuery Error:', error);
        res.status(500).json({ success: false, error: 'Internal server error' });
    }
};

// POST /api/market/weekly-ohlc  body: { ticker, start_date?, end_date? }
const getWeeklyOHLC = async (req, res) => {
    const data = req.body || {};
    const ticker = (data.ticker || '').trim();
    if (!ticker) {
        return res.status(400).json({ success: false, error: 'Missing required field: ticker' });
    }

    let startDate = (data.start_date || '').trim() || null;
    let endDate = (data.end_date || '').trim() || null;

    try {
        const cacheKey = makeCacheKey('market:weekly-ohlc:v1', {
            ticker: ticker.toUpperCase(),
            startDate: startDate || '',
            endDate: endDate || ''
        });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json({ ...cached, cache_hit: true });
        }

        if (!startDate || !endDate) {
            const { min_date, max_date } = await _fetchDateRange(ticker);
            if (!min_date || !max_date) {
                return res.status(404).json({ success: false, error: 'No data found for the given ticker/date range' });
            }
            if (!startDate) startDate = min_date.toISOString ? min_date.toISOString().slice(0, 10) : String(min_date).slice(0, 10);
            if (!endDate) endDate = max_date.toISOString ? max_date.toISOString().slice(0, 10) : String(max_date).slice(0, 10);
        }

        const query = `
            WITH stock_data AS (
                SELECT
                    Ticker,
                    Date,
                    Open,
                    High,
                    Low,
                    Close,
                    EXTRACT(YEAR FROM Date) AS year,
                    EXTRACT(ISOWEEK FROM Date) AS week
                FROM \`${TABLE_FQN}\`
                WHERE Ticker = @symbol
                  AND Date BETWEEN @start AND @end
            ),
            weekly_summary AS (
                SELECT
                    Ticker,
                    year,
                    week,
                    ARRAY_AGG(Open ORDER BY Date ASC LIMIT 1)[OFFSET(0)] AS Open,
                    MAX(High) AS High,
                    MIN(Low) AS Low,
                    ARRAY_AGG(Close ORDER BY Date DESC LIMIT 1)[OFFSET(0)] AS Close,
                    MIN(Date) AS week_start,
                    MAX(Date) AS week_end
                FROM stock_data
                GROUP BY Ticker, year, week
            )
            SELECT *
            FROM weekly_summary
            ORDER BY year, week
        `;

        const [rows] = await bigquery.query({
            query,
            params: {
                symbol: ticker.toUpperCase(),
                start: startDate,
                end: endDate
            }
        });

        const weeklyOHLC = rows.map(r => {
            const year = Number(r.year);
            const week = Number(r.week);
            const ws = r.week_start && (r.week_start.value || r.week_start);
            const we = r.week_end && (r.week_end.value || r.week_end);
            const startStr = ws ? (typeof ws === 'string' ? ws : new Date(ws).toISOString().slice(0, 10)) : '';
            const endStr = we ? (typeof we === 'string' ? we : new Date(we).toISOString().slice(0, 10)) : '';
            return {
                ticker: r.Ticker,
                year,
                week,
                open: Number(r.Open),
                high: Number(r.High),
                low: Number(r.Low),
                close: Number(r.Close),
                adj_close: Number(r.Close),
                start_date: startStr,
                end_date: endStr
            };
        });

        const payload = { success: true, ticker: ticker.toUpperCase(), weeklyOHLC, cache_hit: false };
        await setCache(cacheKey, payload, OHLC_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(payload);
    } catch (error) {
        console.error('BigQuery Error:', error);
        res.status(500).json({ success: false, error: 'Internal server error' });
    }
};

const getUniqueIndices = async (req, res) => {
    try {
        const indices = await analyticsData.getUniqueIndices();
        res.status(200).json({ success: true, indices });
    } catch (error) {
        console.error('Error fetching unique indices:', error);
        res.status(500).json({ success: false, error: 'Failed to fetch indices' });
    }
};

const getPeriodOptions = async (req, res) => {
    try {
        const options = analyticsData.getPeriodOptions();
        res.status(200).json({ success: true, periods: options });
    } catch (error) {
        console.error('Error fetching period options:', error);
        res.status(500).json({ success: false, error: 'Failed to fetch period options' });
    }
};

const getTickerDetailsByIndex = async (req, res) => {
    const data = req.body || {};
    const indexValue = (data.index || '').trim();
    const periodValue = (data.period || '').trim();
    if (!indexValue) {
        return res.status(400).json({ success: false, error: 'Missing required field: index' });
    }
    if (!periodValue) {
        return res.status(400).json({ success: false, error: 'Missing required field: period' });
    }
    try {
        const cacheKey = makeCacheKey('market:ticker-details:v1', {
            index: indexValue.toLowerCase(),
            period: periodValue.toLowerCase()
        });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json({ ...cached, cache_hit: true });
        }

        const details = await analyticsData.getTickerDetailsByIndex(indexValue, periodValue);
        const weighted = enrichRowsWithIndexWeights(indexValue, details);
        const payload = {
            success: true,
            index: indexValue,
            period: periodValue,
            data: weighted,
            cache_hit: false
        };
        await setCache(cacheKey, payload, TICKER_DETAILS_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(payload);
    } catch (error) {
        console.error('Error fetching ticker details by index:', error);
        res.status(500).json({ success: false, error: 'Failed to fetch ticker details' });
    }
};

const getTickerReturns = async (req, res) => {
    const data = req.body || {};
    const ticker = (data.ticker || '').trim();
    if (!ticker) {
        return res.status(400).json({ success: false, error: 'Missing required field: ticker' });
    }

    const customStartDate = (data.customStartDate || '').trim();
    const customEndDate = (data.customEndDate || '').trim();
    let customRange = null;
    if (customStartDate && customEndDate) {
        try {
            const startDt = new Date(customStartDate);
            const endDt = new Date(customEndDate);
            if (startDt <= endDt) {
                customRange = [customStartDate, customEndDate];
            }
        } catch (e) {
            // Ignore invalid dates
        }
    }

    try {
        const returns = await analyticsData.calculateAllReturns(ticker, true, true, customRange, 2018);
        res.status(200).json(returns);
    } catch (error) {
        console.error('Error calculating ticker returns:', error);
        res.status(500).json({ success: false, error: 'Failed to calculate returns' });
    }
};

module.exports = {
    getStockData,
    getOhlcTickerBounds,
    getMonthlyOHLC,
    getWeeklyOHLC,
    getOhlcSignalsIndicator,
    getUniqueIndices,
    getPeriodOptions,
    getTickerDetailsByIndex,
    getTickerReturns
};