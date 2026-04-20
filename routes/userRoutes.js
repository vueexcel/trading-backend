const express = require('express');
const router = express.Router();
const requireAuth = require('../middleware/authMiddleware');

// Returns authenticated profile basics + display name from public.user_profiles.
router.get('/profile', requireAuth, async (req, res) => {
    try {
        const userId = req.user?.id;
        const userEmail = req.user?.email || '';

        let displayName = '';
        const { data, error } = await req.supabase
            .from('user_profiles')
            .select('display_name')
            .eq('id', userId)
            .maybeSingle();

        if (!error && data?.display_name) {
            displayName = String(data.display_name);
        }

        res.status(200).json({
            message: 'Welcome to your private profile!',
            userEmail,
            userId,
            userName: displayName || '',
            displayName: displayName || ''
        });
    } catch (e) {
        res.status(500).json({ error: e.message || 'Failed to load profile' });
    }
});

module.exports = router;