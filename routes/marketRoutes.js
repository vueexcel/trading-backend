const express = require('express');
const router = express.Router();
const {
    getStockData,
    getOhlcTickerBounds,
    getMonthlyOHLC,
    getWeeklyOHLC,
    getOhlcSignalsIndicator,
    getUniqueIndices,
    getPeriodOptions,
    getTickerDetailsByIndex,
    getTickerReturns,
    getTickerAnnualReturns,
    getTickerQuarterlyReturns,
    getTickerMonthlyReturns,
    getTickerCoreReturns,
    getIndexReturns,
    getIndexMarketMovers,
    getIndexConstituentLeaders
} = require('../controllers/marketController');
const requireAuth = require('../middleware/authMiddleware'); // Protect this route!

// Only logged-in users can see stock data
router.get('/ohlc', requireAuth, getStockData);
router.get('/ohlc-ticker-bounds', requireAuth, getOhlcTickerBounds);

// monthly aggregated OHLC (used by frontend screen)
// params: ticker, start_date, end_date (optional)
router.post('/monthly-ohlc', requireAuth, getMonthlyOHLC);

// weekly aggregated OHLC; body: { ticker, start_date?, end_date? }
router.post('/weekly-ohlc', requireAuth, getWeeklyOHLC);

// Daily OHLC + indicator signal (L1–L3, S1–S3, N) per date from consolidated_testing_2
router.post('/ohlc-signals-indicator', requireAuth, getOhlcSignalsIndicator);

// New analytics routes
router.get('/indices', requireAuth, getUniqueIndices);
router.get('/period-options', requireAuth, getPeriodOptions);
router.post('/ticker-details', requireAuth, getTickerDetailsByIndex);
router.post('/ticker-returns', requireAuth, getTickerReturns);
router.post('/ticker-annual-returns', requireAuth, getTickerAnnualReturns);
router.post('/ticker-quarterly-returns', requireAuth, getTickerQuarterlyReturns);
router.post('/ticker-monthly-returns', requireAuth, getTickerMonthlyReturns);
router.post('/ticker-core-returns', requireAuth, getTickerCoreReturns);
router.post('/index-returns', requireAuth, getIndexReturns);

/** 1-day return % vs relative volume (10d) for index constituents (scatter chart). */
router.post('/index-market-movers', requireAuth, getIndexMarketMovers);

/** Best / worst constituents by total return across rolling windows, calendar quarters, and optional custom range. */
router.post('/index-constituent-leaders', requireAuth, getIndexConstituentLeaders);

module.exports = router;