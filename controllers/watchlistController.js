const supabase = require('../config/supabase');
const { makeCacheKey, getCache, setCache, getVersion, bumpVersion } = require('../utils/cache');
const {
    getTickersInfoByIds,
    getTickersByGroupId,
    buildWatchlistRows
} = require('../utils/watchlistUtils');

const WATCHLIST_CACHE_TTL_SECS = Number(process.env.WATCHLIST_CACHE_TTL_SECS || 120);

// 1. Create a new Watchlist
const createWatchlist = async (req, res) => {
    const { name } = req.body;
    const userId = req.user.id; // Comes from authMiddleware
    const supabase = req.supabase;

    if (!name) return res.status(400).json({ error: 'Watchlist name is required' });

    try {
        const { data, error } = await supabase
            .from('watchlists')
            .insert([{ user_id: userId, name }])
            .select()
            .single();

        if (error) throw error;
        await bumpVersion(`watchlists:user:${userId}`);
        res.status(201).json(data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 2. Get all Watchlists for the logged-in User (Including the stocks inside them, with computed data from BigQuery)
const getMyWatchlists = async (req, res) => {
    const userId = req.user.id;
    const supabase = req.supabase;

    try {
        const version = await getVersion(`watchlists:user:${userId}`);
        const cacheKey = makeCacheKey('watchlists:mine:v1', { userId, version });
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json(cached);
        }

        // Table only has watchlist_id, ticker_id, created_at – fetch items + ticker info only
        const { data, error } = await supabase
            .from('watchlists')
            .select(`
                id, name, created_at,
                watchlist_items (
                    ticker_id,
                    tickers ( id, symbol, company_name )
                )
            `)
            .eq('user_id', userId)
            .order('created_at', { ascending: false });

        if (error) throw error;

        // For each watchlist, fetch OHLC/signals from BigQuery and merge into response
        const formattedData = await Promise.all(data.map(async (wl) => {
            const tickerIds = (wl.watchlist_items || []).map((i) => i.ticker_id);
            if (tickerIds.length === 0) {
                return { id: wl.id, name: wl.name, tickers: [] };
            }
            const tickerInfo = await getTickersInfoByIds(tickerIds);
            const rows = await buildWatchlistRows(tickerInfo);
            const infoByTicker = new Map(tickerInfo.map((t) => [t.ticker_id, t]));
            const toDateString = (v) => (v && typeof v === 'object' && v.value) ? v.value : (v != null ? String(v) : null);
            const tickers = rows.map((r) => ({
                id: r.ticker_id,
                symbol: r.symbol,
                company_name: (infoByTicker.get(r.ticker_id) || {}).company_name || null,
                ohlc_date: toDateString(r.ohlc_date),
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                adj_close: r.adj_close,
                dma200: r.dma200,
                signal_main: r.signal_main,
                signal_ma1: r.signal_ma1,
                signal_ma2: r.signal_ma2,
                change_diff: r.change_diff,
                change_pct: r.change_pct,
                link_tv: r.link_tv,
                link_sc: r.link_sc,
                link_yf: r.link_yf
            }));
            return { id: wl.id, name: wl.name, tickers };
        }));

        await setCache(cacheKey, formattedData, WATCHLIST_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(formattedData);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 3. Add Multiple Stocks to a Watchlist (Bulk Insert)
const addTickerToWatchlist = async (req, res) => {
    const { watchlist_id, ticker_ids } = req.body; // Notice 'ticker_ids' is plural now
    const supabase = req.supabase;

    // 1. Validate the input
    if (!watchlist_id || !ticker_ids || !Array.isArray(ticker_ids) || ticker_ids.length === 0) {
        return res.status(400).json({ error: 'watchlist_id and an array of ticker_ids are required' });
    }

    try {
        // 2. Format the data for Supabase
        // Supabase bulk insert expects an array of objects: [{wl_id, t_id1}, {wl_id, t_id2}]
        const payload = ticker_ids.map(id => ({
            watchlist_id: watchlist_id,
            ticker_id: id
        }));

        // 3. Perform Bulk Insert (req.supabase sends user JWT so RLS auth.uid() passes)
        // Using 'upsert' with 'ignoreDuplicates' means if the user accidentally 
        // adds Apple twice, it won't crash the API, it will just ignore the duplicate.
        const { error } = await supabase
            .from('watchlist_items')
            .upsert(payload, { onConflict: 'watchlist_id,ticker_id', ignoreDuplicates: true });

        if (error) throw error;
        await bumpVersion(`watchlists:user:${req.user.id}`);

        res.status(201).json({ message: `Successfully added ${ticker_ids.length} stock(s) to the watchlist.` });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 4. Remove a Stock from a Watchlist
const removeTickerFromWatchlist = async (req, res) => {
    const { watchlist_id, ticker_id } = req.params;
    const supabase = req.supabase;

    try {
        const { error } = await supabase
            .from('watchlist_items')
            .delete()
            .match({ watchlist_id, ticker_id });

        if (error) throw error;
        await bumpVersion(`watchlists:user:${req.user.id}`);

        res.status(200).json({ message: 'Stock removed from watchlist' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 5. Provide the five default watchlists
const getDefaultWatchlists = async (req, res) => {
    try {
        const cacheKey = makeCacheKey('watchlists:defaults:v1', {});
        const cached = await getCache(cacheKey);
        if (cached) {
            res.set('X-Cache-Hit', '1');
            return res.status(200).json(cached);
        }

        const { data: groups, error: grpErr } = await supabase
            .from('market_groups')
            .select('id, name');
        if (grpErr) throw grpErr;

        const result = [];
        for (const g of groups) {
            const tickers = await getTickersByGroupId(g.id);
            const rows = await buildWatchlistRows(tickers);
            result.push({ group: g.name, items: rows });
        }
        await setCache(cacheKey, result, WATCHLIST_CACHE_TTL_SECS);
        res.set('X-Cache-Hit', '0');
        res.status(200).json(result);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 6. Delete an entire user watchlist (not used for defaults — those live in market_groups)
const deleteWatchlist = async (req, res) => {
    const { watchlist_id } = req.params;
    const userId = req.user.id;
    const supabase = req.supabase;

    try {
        const { data: existing, error: findErr } = await supabase
            .from('watchlists')
            .select('id')
            .eq('id', watchlist_id)
            .eq('user_id', userId)
            .maybeSingle();

        if (findErr) throw findErr;
        if (!existing) {
            return res.status(404).json({ error: 'Watchlist not found' });
        }

        const { error: delItemsErr } = await supabase
            .from('watchlist_items')
            .delete()
            .eq('watchlist_id', watchlist_id);
        if (delItemsErr) throw delItemsErr;

        const { error: delWlErr } = await supabase
            .from('watchlists')
            .delete()
            .eq('id', watchlist_id)
            .eq('user_id', userId);
        if (delWlErr) throw delWlErr;

        await bumpVersion(`watchlists:user:${userId}`);
        res.status(200).json({ message: 'Watchlist deleted' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

// 7. Update watchlist name and/or replace all tickers (owned by user)
const updateWatchlist = async (req, res) => {
    const { watchlist_id } = req.params;
    const userId = req.user.id;
    const supabase = req.supabase;
    const { name, ticker_ids } = req.body;

    try {
        const { data: existing, error: findErr } = await supabase
            .from('watchlists')
            .select('id')
            .eq('id', watchlist_id)
            .eq('user_id', userId)
            .maybeSingle();

        if (findErr) throw findErr;
        if (!existing) {
            return res.status(404).json({ error: 'Watchlist not found' });
        }

        if (name != null) {
            const trimmed = String(name).trim();
            if (!trimmed) {
                return res.status(400).json({ error: 'Watchlist name cannot be empty' });
            }
            const { error: nameErr } = await supabase
                .from('watchlists')
                .update({ name: trimmed })
                .eq('id', watchlist_id)
                .eq('user_id', userId);
            if (nameErr) throw nameErr;
        }

        if (ticker_ids != null) {
            if (!Array.isArray(ticker_ids)) {
                return res.status(400).json({ error: 'ticker_ids must be an array when provided' });
            }
            const { error: delErr } = await supabase
                .from('watchlist_items')
                .delete()
                .eq('watchlist_id', watchlist_id);
            if (delErr) throw delErr;

            const uniqueIds = [...new Set(ticker_ids.map((id) => String(id)).filter(Boolean))];
            if (uniqueIds.length > 0) {
                const payload = uniqueIds.map((ticker_id) => ({
                    watchlist_id,
                    ticker_id
                }));
                const { error: insErr } = await supabase
                    .from('watchlist_items')
                    .insert(payload);
                if (insErr) throw insErr;
            }
        }

        await bumpVersion(`watchlists:user:${userId}`);
        res.status(200).json({ message: 'Watchlist updated' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

module.exports = {
    createWatchlist,
    getMyWatchlists,
    addTickerToWatchlist,
    removeTickerFromWatchlist,
    getDefaultWatchlists,
    deleteWatchlist,
    updateWatchlist
};

