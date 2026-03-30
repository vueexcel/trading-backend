const express = require('express');
const router = express.Router();
const requireAuth = require('../middleware/authMiddleware');
const { runOdinIndex, runOdinSummary } = require('../controllers/analyticsController');

router.use(requireAuth);

router.post('/odin-index', runOdinIndex);
router.post('/odin-summary', runOdinSummary);

module.exports = router;
