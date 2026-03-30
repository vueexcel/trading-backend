const express = require('express');
const router = express.Router();
const { 
    createWatchlist, 
    getMyWatchlists, 
    addTickerToWatchlist, 
    removeTickerFromWatchlist,
    getDefaultWatchlists
} = require('../controllers/watchlistController');
const requireAuth = require('../middleware/authMiddleware'); // Must be logged in!

// defaults are public
router.get('/defaults', getDefaultWatchlists); // Fetch the five built-in watchlists

// Apply auth middleware to all other watchlist routes
router.use(requireAuth); 

router.post('/', createWatchlist);             // Create new watchlist
router.get('/', getMyWatchlists);              // Get all my watchlists
router.post('/add', addTickerToWatchlist);     // Add stock to watchlist
router.delete('/:watchlist_id/remove/:ticker_id', removeTickerFromWatchlist); // Remove stock

module.exports = router;