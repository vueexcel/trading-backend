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

const OHLC_MAX_LIMIT = 500;
const OHLC_DEFAULT_LIMIT = 100;
const OHLC_CACHE_TTL_SECS = Number(process.env.OHLC_CACHE_TTL_SECS || 120);
const OHLC_SIGNALS_INDICATOR_CACHE_TTL_SECS = Number(process.env.OHLC_SIGNALS_INDICATOR_CACHE_TTL_SECS || 120);
const OHLC_SIGNALS_MAX_RANGE_DAYS = Number(process.env.OHLC_SIGNALS_MAX_RANGE_DAYS || 3650);

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

    const rangeDays = Math.ceil((endDt - startDt) / (1000 * 60 * 60 * 24)) + 1;
    if (rangeDays > OHLC_SIGNALS_MAX_RANGE_DAYS) {
        return res.status(400).json({
            success: false,
            error: `Date range too large (max ${OHLC_SIGNALS_MAX_RANGE_DAYS} days)`
        });
    }

    const sym = ticker.toUpperCase();

    try {
        const cacheKey = makeCacheKey('market:ohlc-signals-indicator:v1', {
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

        const [priceRows, signalRows] = await Promise.all([
            bigquery.query({
                query: ohlcQuery,
                params: { ticker: sym, start: startStr, end: endStr }
            }).then((r) => r[0] || []),
            bigquery.query({
                query: signalQuery,
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

        const payload = {
            success: true,
            cache_hit: false,
            ticker: sym,
            start_date: startStr,
            end_date: endStr,
            row_count: rows.length,
            data: rows
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
        const details = await analyticsData.getTickerDetailsByIndex(indexValue, periodValue);
        res.status(200).json({ success: true, index: indexValue, period: periodValue, data: details });
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
    getMonthlyOHLC,
    getWeeklyOHLC,
    getOhlcSignalsIndicator,
    getUniqueIndices,
    getPeriodOptions,
    getTickerDetailsByIndex,
    getTickerReturns
};