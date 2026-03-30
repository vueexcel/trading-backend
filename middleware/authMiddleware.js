const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
require('dotenv').config();

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey, {
    auth: { persistSession: false },
    global: { fetch }
});

const requireAuth = async (req, res, next) => {
    try {
        // 1. Check if the request has an Authorization header
        const authHeader = req.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ error: 'Unauthorized: No token provided' });
        }

        // 2. Extract the token
        const token = authHeader.split(' ')[1];

        // 3. Ask Supabase to verify the token
        const { data: { user }, error } = await supabase.auth.getUser(token);

        if (error || !user) {
            return res.status(401).json({ error: 'Unauthorized: Invalid token' });
        }

        // 4. Attach the user and a Supabase client that uses this JWT (so RLS sees auth.uid())
        req.user = user;
        req.token = token;
        req.supabase = createClient(supabaseUrl, supabaseKey, {
            auth: { persistSession: false },
            global: {
                fetch,
                headers: { Authorization: `Bearer ${token}` }
            }
        });

        next();
    } catch (error) {
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

module.exports = requireAuth;