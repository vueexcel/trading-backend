const rateLimit = require('express-rate-limit');

// Rule: Max 5 login attempts per 15 minutes per IP
const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 5, // Limit each IP to 5 requests per windowMs
    message: { error: "Too many login attempts, please try again after 15 minutes" },
    standardHeaders: true, 
    legacyHeaders: false,
});

module.exports = { loginLimiter };