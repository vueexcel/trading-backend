const express = require('express');
const router = express.Router();
const {
  signUp,
  login,
  logout,
  refresh,
  updateDisplayName,
  verifySignUpOtp,
  resendSignUpOtp,
  startForgotPassword
} = require('../controllers/authController');
const { loginLimiter } = require('../middleware/rateLimitMiddleware'); // <-- Import this
const requireAuth = require('../middleware/authMiddleware');

router.post('/signup', signUp);
router.post('/verify-signup-otp', verifySignUpOtp);
router.post('/resend-signup-otp', resendSignUpOtp);
router.post('/forgot-password/start', startForgotPassword);
router.post('/login', loginLimiter, login);
router.post('/refresh', refresh);
router.post('/logout', logout);
router.patch('/me/display-name', requireAuth, updateDisplayName);

module.exports = router;