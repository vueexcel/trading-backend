const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
require('dotenv').config();

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

const supabaseService = createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
    global: { fetch }
});

module.exports = supabaseService;
