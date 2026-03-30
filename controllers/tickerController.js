const supabase = require('../config/supabase');

// 1. Get all Market Groups (Tabs for your Frontend)
const getGroups = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('market_groups')
            .select('id, name, code')
            .order('id', { ascending: true });

        if (error) throw error;

        res.status(200).json(data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 2. Get all Tickers for a specific group (e.g., code = 'ND' for Nasdaq)
const getTickersByGroup = async (req, res) => {
    const { code } = req.params; // e.g., 'ND'

    try {
        // Step A: Find the Group ID using the code
        const { data: groupData, error: groupError } = await supabase
            .from('market_groups')
            .select('id')
            .eq('code', code.toUpperCase())
            .single();

        if (groupError || !groupData) {
            return res.status(404).json({ error: 'Group not found' });
        }

        // Step B: Get all tickers linked to that Group ID
        const { data: tickers, error: tickerError } = await supabase
            .from('ticker_groups')
            .select(`
                tickers (
                    id,
                    symbol,
                    company_name
                )
            `)
            .eq('group_id', groupData.id);

        if (tickerError) throw tickerError;

        // Clean up the nested response format from Supabase
        const formattedTickers = tickers.map(t => t.tickers);

        res.status(200).json({
            group_code: code.toUpperCase(),
            count: formattedTickers.length,
            tickers: formattedTickers
        });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 3. Search for a Ticker (With OPTIONAL Group Filtering)
const searchTickers = async (req, res) => {
    const { q, group } = req.query; // e.g., ?q=AAPL or ?q=AAPL&group=SP

    if (!q) {
        return res.status(400).json({ error: 'Search query is required' });
    }

    try {
        if (group) {
            // --- SCENARIO A: Search ONLY inside a specific group ---
            
            // 1. Get the Group ID
            const { data: groupData, error: groupError } = await supabase
                .from('market_groups')
                .select('id')
                .eq('code', group.toUpperCase())
                .single();

            if (groupError || !groupData) {
                return res.status(404).json({ error: 'Group not found' });
            }

            // 2. Search inside the Junction Table
            // The '!inner' keyword forces Supabase to only return results that match the search
            const { data, error } = await supabase
                .from('ticker_groups')
                .select(`
                    tickers!inner (
                        id, 
                        symbol, 
                        company_name
                    )
                `)
                .eq('group_id', groupData.id)
                .or(`symbol.ilike.%${q}%,company_name.ilike.%${q}%`, { foreignTable: 'tickers' })
                .limit(10);

            if (error) throw error;

            // Clean up the nested response format
            const formattedData = data.map(item => item.tickers);
            return res.status(200).json(formattedData);

        } else {
            // --- SCENARIO B: Search ALL stocks (No group selected) ---
            const { data, error } = await supabase
                .from('tickers')
                .select('id, symbol, company_name')
                .or(`symbol.ilike.%${q}%,company_name.ilike.%${q}%`)
                .limit(10);`    `

            if (error) throw error;
            return res.status(200).json(data);
        }

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

module.exports = { getGroups, getTickersByGroup, searchTickers };