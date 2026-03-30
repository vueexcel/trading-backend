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

module.exports = { signUp, login, logout };