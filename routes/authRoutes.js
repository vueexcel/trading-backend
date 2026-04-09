const express = require('express');
const router = express.Router();
const { signUp, login, logout, refresh } = require('../controllers/authController');
const { loginLimiter } = require('../middleware/rateLimitMiddleware'); // <-- Import this

router.post('/signup', signUp);
router.post('/login', loginLimiter, login);
router.post('/refresh', refresh);
router.post('/logout', logout);

module.exports = router;