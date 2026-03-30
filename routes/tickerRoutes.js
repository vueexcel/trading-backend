const express = require('express');
const router = express.Router();
const { getGroups, getTickersByGroup, searchTickers } = require('../controllers/tickerController');
const requireAuth = require('../middleware/authMiddleware'); // Protect your APIs!

// All these routes require the user to be logged in
router.get('/groups', requireAuth, getGroups);
router.get('/search', requireAuth, searchTickers);
router.get('/group/:code', requireAuth, getTickersByGroup); // :code is a variable (e.g., ND, DJ)

module.exports = router;