const express = require('express');
const router = express.Router();
const requireAuth = require('../middleware/authMiddleware');

// The requireAuth middleware goes in the middle!
router.get('/profile', requireAuth, (req, res) => {
    // If they make it here, the bouncer let them in.
    // We can access their info via req.user
    res.status(200).json({
        message: 'Welcome to your private profile!',
        userEmail: req.user.email,
        userId: req.user.id
    });
});

module.exports = router;