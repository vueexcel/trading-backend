const supabase = require('../config/supabase');
const tickersDb = require('../config/supabaseTickers');

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

/** Allow only safe characters for ilike patterns (avoid % / _ injection). */
function sanitizeTickerSearchQuery(q) {
    const trimmed = String(q || '').trim();
    return trimmed.replace(/[^a-zA-Z0-9.\s-]/g, '').replace(/\s+/g, ' ').trim().slice(0, 48);
}

// 3. Search for a Ticker (With OPTIONAL Group Filtering)
const searchTickers = async (req, res) => {
    const { group } = req.query;
    const rawQ = req.query.q ?? req.query.query;

    const safe = sanitizeTickerSearchQuery(rawQ);
    if (!safe) {
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
                .or(`symbol.ilike.${safe}%`, { foreignTable: 'tickers' })
                .limit(50);

            if (error) throw error;

            // Clean up the nested response format
            const formattedData = data.map(item => item.tickers);
            return res.status(200).json(formattedData);

        } else {
            // --- SCENARIO B: Search ALL stocks (symbol or company name, substring, case-insensitive) ---
            const compact = safe.replace(/\s/g, '');
            const symPattern = `%${compact}%`;
            const namePattern = `%${safe}%`;

            const [bySymbol, byCompany] = await Promise.all([
                tickersDb
                    .from('tickers')
                    .select('id, symbol, company_name')
                    .ilike('symbol', symPattern)
                    .order('symbol', { ascending: true })
                    .limit(250),
                tickersDb
                    .from('tickers')
                    .select('id, symbol, company_name')
                    .ilike('company_name', namePattern)
                    .order('symbol', { ascending: true })
                    .limit(250)
            ]);

            if (bySymbol.error) throw bySymbol.error;
            if (byCompany.error) throw byCompany.error;

            /** One row per symbol; keep `id` whenever any matching row had it. */
            const bySym = new Map();
            const upsert = (row) => {
                if (!row?.symbol) return;
                const sym = String(row.symbol).trim().toUpperCase();
                const rid = row.id ?? row.ticker_id;
                const id = rid != null && rid !== '' ? rid : null;
                const co = row.company_name ?? null;
                const prev = bySym.get(sym);
                if (!prev) {
                    bySym.set(sym, { symbol: sym, id, company_name: co });
                    return;
                }
                const keepId = prev.id != null && prev.id !== '' ? prev.id : id;
                bySym.set(sym, {
                    symbol: sym,
                    id: keepId,
                    company_name: co || prev.company_name || null
                });
            };
            for (const row of bySymbol.data || []) upsert(row);
            for (const row of byCompany.data || []) upsert(row);

            let list = [...bySym.values()].sort((a, b) => a.symbol.localeCompare(b.symbol));

            const symbolsMissingId = [
                ...new Set(list.filter((r) => (r.id == null || r.id === '') && r.symbol).map((r) => r.symbol))
            ];
            if (symbolsMissingId.length > 0) {
                const { data: idRows, error: idErr } = await tickersDb
                    .from('tickers')
                    .select('id, symbol')
                    .in('symbol', symbolsMissingId);
                if (!idErr && idRows?.length) {
                    const bySym = new Map(
                        idRows.map((r) => [String(r.symbol || '').trim().toUpperCase(), r.id])
                    );
                    list = list.map((r) => ({
                        ...r,
                        id: r.id != null && r.id !== '' ? r.id : bySym.get(r.symbol) ?? r.id
                    }));
                }
            }

            const shaped = list
                .filter((r) => r.id != null && r.id !== '' && r.symbol)
                .slice(0, 250)
                .map((r) => ({
                    id: r.id,
                    symbol: r.symbol,
                    company_name: r.company_name
                }));

            return res.status(200).json(shaped);
        }

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

/**
 * Batch-resolve `id` (and company_name) for exact symbols — uses service-role DB when configured.
 * POST body: { symbols: string[] } (max 150)
 */
const resolveTickerIds = async (req, res) => {
    try {
        const { symbols } = req.body || {};
        if (!Array.isArray(symbols) || symbols.length === 0) {
            return res.status(400).json({ error: 'Body must include symbols: string[]' });
        }
        const cleaned = [
            ...new Set(symbols.map((s) => String(s || '').trim().toUpperCase()).filter(Boolean))
        ].slice(0, 150);

        const { data, error } = await tickersDb
            .from('tickers')
            .select('id, symbol, company_name')
            .in('symbol', cleaned);

        if (error) throw error;

        const tickers = (data || []).map((r) => ({
            id: r.id,
            symbol: String(r.symbol || '')
                .trim()
                .toUpperCase(),
            company_name: r.company_name ?? null
        }));

        return res.status(200).json({ tickers });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
};

module.exports = { getGroups, getTickersByGroup, searchTickers, resolveTickerIds };