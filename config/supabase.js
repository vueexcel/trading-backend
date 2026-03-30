// const { createClient } = require('@supabase/supabase-js');
// require('dotenv').config();

// const supabaseUrl = process.env.SUPABASE_URL;
// const supabaseKey = process.env.SUPABASE_KEY;

// const supabase = createClient(supabaseUrl, supabaseKey);

// module.exports = supabase;

const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch'); // <-- 1. Import the new fetch
require('dotenv').config();

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

// 2. Tell Supabase to use the new fetch library instead of the buggy Node one
const supabase = createClient(supabaseUrl, supabaseKey, {
    auth: {
        persistSession: false
    },
    global: {
        fetch: fetch
    }
});

module.exports = supabase;