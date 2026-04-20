const supabase = require('../config/supabase');

// 1. Sign Up a new user
const signUp = async (req, res) => {
    const { email, password } = req.body;

    try {
        const { data, error } = await supabase.auth.signUp({
            email: email,
            password: password,
        });

        if (error) throw error;

        res.status(201).json({ message: 'User created successfully', data });
    } catch (error) {
        res.status(400).json({ error: error.message });
    }
};

// 2. Log In an existing user
// Add this helper at the top
const logEvent = async (userId, eventType, ip) => {
    await supabase.from('audit_logs').insert({
        user_id: userId,
        event_type: eventType,
        ip_address: ip
    });
};

// Update the Login function
const login = async (req, res) => {
    const { email, password } = req.body;
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress; // Get IP

    try {
        const { data, error } = await supabase.auth.signInWithPassword({
            email,
            password,
        });

        if (error) throw error;

        // ✅ REQUIREMENT MET: Audit Logging
        // Save 'LOGIN_SUCCESS' to PostgreSQL
        await logEvent(data.user.id, 'LOGIN_SUCCESS', clientIp);

        res.status(200).json({ 
            message: 'Login successful', 
            session: data.session 
        });
    } catch (error) {
        // Optional: Log failed attempts if you had the user ID
        res.status(400).json({ error: error.message });
    }
};

// 3. Log Out
const logout = async (req, res) => {
    try {
        const { error } = await supabase.auth.signOut();
        if (error) throw error;
        
        res.status(200).json({ message: 'Logged out successfully' });
    } catch (error) {
        res.status(400).json({ error: error.message });
    }
};

// 4. Refresh session (exchange refresh_token for new access_token)
const refresh = async (req, res) => {
    const { refresh_token } = req.body || {};

    try {
        if (!refresh_token) {
            return res.status(400).json({ error: 'refresh_token is required' });
        }

        const { data, error } = await supabase.auth.refreshSession({ refresh_token });

        if (error) throw error;

        res.status(200).json({
            message: 'Session refreshed',
            session: data.session
        });
    } catch (error) {
        res.status(401).json({ error: error.message || 'Refresh failed' });
    }
};

/** PATCH display name — requires Bearer JWT (signup flow after session exists). */
const updateDisplayName = async (req, res) => {
    try {
        const raw = req.body?.displayName ?? req.body?.display_name ?? req.body?.['Display Name'];
        const displayName = typeof raw === 'string' ? raw.trim() : '';
        if (!displayName || displayName.length < 2) {
            return res.status(400).json({ error: 'displayName must be at least 2 characters' });
        }

        const { data: authData, error: authErr } = await req.supabase.auth.updateUser({
            data: { display_name: displayName }
        });

        if (authErr) throw authErr;

        let profileWarning = null;
        try {
            let pErr = (await req.supabase.from('profiles').upsert(
                { id: req.user.id, display_name: displayName },
                { onConflict: 'id' }
            )).error;
            if (pErr) {
                const e2 = (await req.supabase.from('profiles').upsert(
                    { id: req.user.id, 'Display Name': displayName },
                    { onConflict: 'id' }
                )).error;
                if (e2) profileWarning = e2.message;
            }
        } catch (e) {
            profileWarning = e.message || String(e);
        }

        res.status(200).json({
            success: true,
            user: authData?.user ?? null,
            profileWarning
        });
    } catch (error) {
        res.status(400).json({ error: error.message || 'Update failed' });
    }
};

/** Email OTP from signup template (`{{ .Token }}`). Returns session when valid. */
const verifySignUpOtp = async (req, res) => {
    const { email, token } = req.body || {};
    const em = typeof email === 'string' ? email.trim() : '';
    const code = typeof token === 'string' ? token.replace(/\D/g, '') : String(token || '').replace(/\D/g, '');

    if (!em || !code) {
        return res.status(400).json({ error: 'email and token are required' });
    }

    try {
        const { data, error } = await supabase.auth.verifyOtp({
            email: em,
            token: code,
            type: 'signup'
        });

        if (error) throw error;

        res.status(200).json({
            message: 'Email verified',
            session: data.session,
            user: data.user
        });
    } catch (error) {
        res.status(400).json({ error: error.message || 'Verification failed' });
    }
};

/** Resend signup confirmation (new OTP) — same as a new signUp without password. */
const resendSignUpOtp = async (req, res) => {
    const { email } = req.body || {};
    const em = typeof email === 'string' ? email.trim() : '';

    if (!em) {
        return res.status(400).json({ error: 'email is required' });
    }

    try {
        const { data, error } = await supabase.auth.resend({
            type: 'signup',
            email: em
        });

        if (error) throw error;

        res.status(200).json({ message: 'Code sent', data: data || null });
    } catch (error) {
        res.status(400).json({ error: error.message || 'Resend failed' });
    }
};

module.exports = { signUp, login, logout, refresh, updateDisplayName, verifySignUpOtp, resendSignUpOtp };