/**
 * Supabase client for server-side `tickers` reads where full rows (including `id`)
 * must be visible. Uses SUPABASE_SERVICE_ROLE_KEY when set so RLS does not strip `id`.
 * Falls back to the default client (SUPABASE_KEY) if the service role is not configured.
 */
const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');
require('dotenv').config();

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const defaultClient = require('./supabase');

if (supabaseUrl && serviceRoleKey) {
  module.exports = createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
    global: { fetch }
  });
} else {
  module.exports = defaultClient;
}
