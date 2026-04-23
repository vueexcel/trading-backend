// const express = require('express');
// const cors = require('cors');
// require('dotenv').config();

// const authRoutes = require('./routes/authRoutes');

// const app = express();

// // Middleware
// app.use(cors()); // Allows your frontend to connect
// app.use(express.json()); // Allows the server to read JSON data

// // Routes
// app.use('/api/auth', authRoutes);

// // A simple test route
// app.get('/', (req, res) => {
//     res.send('Trading App Backend is running!');
// });

// // Start Server - THIS IS THE PART THAT KEEPS IT RUNNING
// const PORT = process.env.PORT || 5000;
// app.listen(PORT, () => {
//     console.log(`Server is running on port ${PORT}`);
// });
const express = require('express');
const cors = require('cors');
require('dotenv').config();
const marketRoutes = require('./routes/marketRoutes');
const authRoutes = require('./routes/authRoutes');
const userRoutes = require('./routes/userRoutes'); 
const tickerRoutes = require('./routes/tickerRoutes');
const watchlistRoutes = require('./routes/watchlistRoutes');
const analyticsRoutes = require('./routes/analyticsRoutes');
const { startSnapshotRefresher } = require('./services/snapshotRefresher');
const { startTickerReturnsPrewarmer, waitForTickerReturnsWarmup } = require('./services/tickerReturnsPrewarmer');

const app = express();

app.use(cors({
    exposedHeaders: [
        'X-Cache-Hit',
        'X-Data-Source',
        'X-Snapshot-Ts',
        'X-Ticker-Returns-Source',
        'X-Compute-Ms',
        'X-Cache-Key'
    ]
}));
app.use(express.json());
app.use(express.static('public'));
app.use('/vendor', express.static('node_modules/@supabase/supabase-js/dist/umd'));

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/user', userRoutes);
app.use('/api/market', marketRoutes);
app.use('/api/tickers', tickerRoutes);
app.use('/api/watchlists', watchlistRoutes);
app.use('/api/analytics', analyticsRoutes);

app.get('/api/public/supabase-config', (req, res) => {
    res.json({
        url: process.env.SUPABASE_URL || '',
        anonKey: process.env.SUPABASE_KEY || ''
    });
});

app.get('/', (req, res) => {
    res.send('Trading App Backend is running!');
});

app.get('/chart', (req, res) => {
    res.sendFile('ohlc-signals.html', { root: 'public' });
});

const PORT = process.env.PORT || 5000;

async function bootstrap() {
    try {
        await waitForTickerReturnsWarmup();
    } catch (err) {
        console.warn('[ticker-returns-prewarm] startup warmup failed:', err?.message || err);
    }
    app.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
        startSnapshotRefresher();
        startTickerReturnsPrewarmer();
    });
}

void bootstrap();