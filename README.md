# Trading Backend API

A robust Node.js/Express backend API for a trading application with real-time market data, ticker management, and watchlist functionality.

## Features

- **User Authentication** - Sign up, login, and logout with Supabase
- **Market Data** - Real-time OHLC (Open, High, Low, Close) stock data
- **Ticker Management** - Browse tickers by groups and search functionality
- **Watchlists** - Create and manage custom watchlists with ticker tracking
- **Rate Limiting** - Protected endpoints to prevent abuse
- **Caching** - Redis/Upstash for optimized performance
- **Big Query Integration** - Advanced data analytics and storage

## Tech Stack

- **Runtime:** Node.js
- **Framework:** Express.js
- **Database:** Supabase (PostgreSQL)
- **Data Analytics:** Google BigQuery
- **Caching:** Upstash Redis
- **Authentication:** Supabase Auth
- **Rate Limiting:** express-rate-limit

## Installation

### Prerequisites
- Node.js (v14 or higher)
- npm or yarn
- Environment variables configured (see Setup section)

### Steps

1. Clone the repository
```bash
git clone <your-repo-url>
cd TradingBackend
```

2. Install dependencies
```bash
npm install
```

3. Create a `.env` file in the root directory with the required environment variables (see Environment Variables section)

4. Start the server
```bash
npm start           # Production
npm run dev         # Development with hot reload
```

The server will run on http://localhost:5000 (or your specified PORT)

## Environment Variables

Create a `.env` file in the root directory:

```
PORT=5000
GOOGLE_CLOUD_PROJECT=extended-byway-454621-s6
BIGQUERY_DATASET=sp500data1
BIGQUERY_TABLE=stock_all_data
TICKER_DETAILS_TABLE=TickerDetails
SUPABASE_URL=<your-supabase-url>
SUPABASE_KEY=<your-supabase-anon-key>
# Strongly recommended on the server: service role key so `tickers.id` is always returned for search/resolve (RLS-safe full reads).
SUPABASE_SERVICE_ROLE_KEY=<your-supabase-service-role-key>
REDIS_URL=<your-upstash-redis-url>
REDIS_TOKEN=<your-upstash-redis-token>
```

### BigQuery Configuration
- `GOOGLE_CLOUD_PROJECT` - Your Google Cloud project ID
- `BIGQUERY_DATASET` - Dataset containing market data (default: `sp500data1`)
- `BIGQUERY_TABLE` - Table with OHLC prices (default: `stock_all_data`)
- `TICKER_DETAILS_TABLE` - Table with ticker metadata (default: `TickerDetails`)
- `TICKER_DETAILS_SYMBOL_COLUMN` (optional) - If set (e.g. `Ticker`), only that column is used for symbol lookups in `TickerDetails`. If unset, the API tries `Symbol` then `Ticker`.

## API Endpoints

### Authentication Routes (`/api/auth`)

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|----------------|
| `POST` | `/signup` | Create a new user account | ❌ |
| `POST` | `/login` | Login user (rate-limited) | ❌ |
| `POST` | `/logout` | Logout user | ✅ |

**Example:**
```bash
# Sign up
curl -X POST http://localhost:5000/api/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"securepassword"}'

# Login
curl -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"securepassword"}'
```

### Market Routes (`/api/market`)

#### Core Market Data

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|----------------|
| `GET` | `/ohlc` | Get stock OHLC data (raw rows, limited to 100) | ✅ |
| `GET` | `/ohlc-ticker-bounds?symbol=AAPL` | Min/max `Date` in OHLC table for a ticker (chart “ALL” range) | ✅ |
| `POST` | `/monthly-ohlc` | Get monthly aggregated OHLC data | ✅ |

**Get OHLC Data:**
```bash
curl -X GET "http://localhost:5000/api/market/ohlc?symbol=AAPL" \
  -H "Authorization: Bearer <your-token>"
```

**Get Monthly OHLC Data:**
```bash
curl -X POST http://localhost:5000/api/market/monthly-ohlc \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "ticker": "AAPL",
    "start_date": "2023-01-01",
    "end_date": "2023-12-31"
  }'
```

**Response:**
```json
{
  "success": true,
  "ticker": "AAPL",
  "monthlyOHLC": [
    {
      "ticker": "AAPL",
      "year": 2023,
      "month": 1,
      "open": 150.43,
      "high": 159.99,
      "low": 148.23,
      "close": 158.78,
      "adj_close": 158.78,
      "start_date": "2023-01-01",
      "end_date": "2023-01-31"
    }
  ]
}
```

#### Analytics & Performance

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|----------------|
| `GET` | `/indices` | Get available index options | ✅ |
| `GET` | `/period-options` | Get period selection options | ✅ |
| `POST` | `/ticker-details` | Get ticker details by index and period | ✅ |
| `POST` | `/ticker-returns` | Get comprehensive return analysis for a ticker | ✅ |

**Get Available Indices:**
```bash
curl -X GET http://localhost:5000/api/market/indices \
  -H "Authorization: Bearer <your-token>"
```

**Response:**
```json
{
  "success": true,
  "indices": [
    { "value": "sp500", "label": "S&P 500" },
    { "value": "nasdaq100", "label": "Nasdaq 100" },
    { "value": "dowjones", "label": "Dow Jones" }
  ]
}
```

**Get Period Options:**
```bash
curl -X GET http://localhost:5000/api/market/period-options \
  -H "Authorization: Bearer <your-token>"
```

**Response:**
```json
{
  "success": true,
  "periods": [
    { "value": "last-date", "label": "Last date" },
    { "value": "week", "label": "Week" },
    { "value": "last-month", "label": "Last Month" },
    { "value": "last-3-months", "label": "Last 3 months" },
    { "value": "last-6-months", "label": "Last 6 months" },
    { "value": "ytd", "label": "Year to Date (YTD)" },
    { "value": "last-1-year", "label": "Last 1 year" },
    { "value": "last-2-years", "label": "Last 2 years" },
    { "value": "last-5-years", "label": "Last 5 years" },
    { "value": "last-10-years", "label": "Last 10 years" }
  ]
}
```

**Get Ticker Details by Index and Period:**
```bash
curl -X POST http://localhost:5000/api/market/ticker-details \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "index": "sp500",
    "period": "last-1-year"
  }'
```

**Response:**
```json
{
  "success": true,
  "index": "sp500",
  "period": "last-1-year",
  "data": [
    {
      "row": 1,
      "symbol": "AAPL",
      "security": "Apple Inc.",
      "sector": "Technology",
      "industry": "Computer Manufacturing",
      "index": "S&P 500",
      "totalReturnPercentage": 28.45,
      "price": 185.64
    }
  ]
}
```

**Get Comprehensive Ticker Returns Analysis:**
```bash
curl -X POST http://localhost:5000/api/market/ticker-returns \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "ticker": "AAPL",
    "customStartDate": "2024-01-01",
    "customEndDate": "2025-01-01"
  }'
```

**Response:**
```json
{
  "success": true,
  "ticker": "AAPL",
  "asOfDate": "2026-03-05",
  "performance": {
    "dynamicPeriods": [
      {
        "period": "Week",
        "startDate": "2026-02-26",
        "endDate": "2026-03-05",
        "years": 0.018,
        "startPrice": 182.43,
        "endPrice": 185.64,
        "priceDifference": 3.21,
        "totalReturn": 1.76,
        "simpleAnnualReturn": 97.68,
        "cagrPercent": 97.68
      }
    ],
    "predefinedPeriods": [
      {
        "period": "2024 - 2026",
        "startDate": "2024-01-01",
        "endDate": "2026-03-05",
        "years": 2,
        "startPrice": 144.29,
        "endPrice": 185.64,
        "priceDifference": 41.35,
        "totalReturn": 28.64,
        "simpleAnnualReturn": 14.32,
        "cagrPercent": 13.82
      }
    ],
    "annualReturns": [
      {
        "period": "2024",
        "startDate": "2024-01-01",
        "endDate": "2024-12-31",
        "years": 1,
        "startPrice": 144.29,
        "endPrice": 159.87,
        "priceDifference": 15.58,
        "totalReturn": 10.79,
        "simpleAnnualReturn": 10.79,
        "cagrPercent": 10.79
      }
    ],
    "monthlyReturns": [
      {
        "period": "2026-01",
        "startDate": "2025-12-31",
        "endDate": "2026-01-31",
        "years": 0.083,
        "startPrice": 182.12,
        "endPrice": 183.45,
        "priceDifference": 1.33,
        "totalReturn": 0.73,
        "simpleAnnualReturn": 8.84,
        "cagrPercent": 8.84
      }
    ],
    "quarterlyReturns": [
      {
        "period": "2026-Q1",
        "startDate": "2025-12-31",
        "endDate": "2026-03-31",
        "years": 0.25,
        "startPrice": 182.12,
        "endPrice": 185.64,
        "priceDifference": 3.52,
        "totalReturn": 1.93,
        "simpleAnnualReturn": 7.72,
        "cagrPercent": 7.72
      }
    ],
    "customRange": [
      {
        "period": "Selected dates",
        "startDate": "2024-01-01",
        "endDate": "2025-01-01",
        "years": 1,
        "startPrice": 144.29,
        "endPrice": 159.87,
        "priceDifference": 15.58,
        "totalReturn": 10.79,
        "simpleAnnualReturn": 10.79,
        "cagrPercent": 10.79
      }
    ]
  }
}
```

### Ticker Routes (`/api/tickers`)

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|----------------|
| `GET` | `/groups` | Get all ticker groups | ✅ |
| `GET` | `/group/:code` | Get tickers by group code (e.g., ND, DJ) | ✅ |
| `GET` | `/search` | Search for tickers (`q` or `query`) | ✅ |
| `POST` | `/resolve` | Body `{ "symbols": ["AAPL","MSFT"] }` → `{ tickers: [{ id, symbol, company_name }] }` | ✅ |

**Example:**
```bash
# Get all groups
curl -X GET http://localhost:5000/api/tickers/groups \
  -H "Authorization: Bearer <your-token>"

# Get tickers in a specific group
curl -X GET http://localhost:5000/api/tickers/group/ND \
  -H "Authorization: Bearer <your-token>"

# Search tickers (use `q` or `query` — same parameter)
curl -X GET "http://localhost:5000/api/tickers/search?q=AAPL" \
  -H "Authorization: Bearer <your-token>"
```

### User Routes (`/api/user`)

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|----------------|
| `GET` | `/profile` | Get current user profile | ✅ |

**Example:**
```bash
curl -X GET http://localhost:5000/api/user/profile \
  -H "Authorization: Bearer <your-token>"
```

**Response:**
```json
{
  "message": "Welcome to your private profile!",
  "userEmail": "user@example.com",
  "userId": "user-id-123"
}
```

### Watchlist Routes (`/api/watchlists`)

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|----------------|
| `POST` | `/` | Create a new watchlist | ✅ |
| `GET` | `/` | Get all user watchlists | ✅ |
| `POST` | `/add` | Add ticker to watchlist | ✅ |
| `DELETE` | `/:watchlist_id/remove/:ticker_id` | Remove ticker from watchlist | ✅ |

**Example:**
```bash
# Create watchlist
curl -X POST http://localhost:5000/api/watchlists \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{"name":"My Watchlist","description":"Tech stocks"}'

# Get all watchlists
curl -X GET http://localhost:5000/api/watchlists \
  -H "Authorization: Bearer <your-token>"

# Add ticker to watchlist
curl -X POST http://localhost:5000/api/watchlists/add \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{"watchlist_id":"wl-123","ticker_id":"AAPL"}'

# Remove ticker from watchlist
curl -X DELETE http://localhost:5000/api/watchlists/wl-123/remove/AAPL \
  -H "Authorization: Bearer <your-token>"
```

- When watchlist items are added the backend automatically queries BigQuery and
  calculates a full data row for each ticker.  The following fields are stored
  alongside the `ticker_id` in the `watchlist_items` table (see schema section
  above):
  `ohlc_date`, `open`, `high`, `low`, `close`, `adj_close`, `dma200`,
  `signal_main`, `signal_ma1`, `signal_ma2`, `change_diff`, `change_pct`,
  `link_tv`, `link_sc`, `link_yf`.

### Default watchlists

The server exposes all five built-in group watchlists under a separate
endpoint.  They do not belong to any user and are regenerated on every request
using the same BigQuery logic.

```bash
# Fetch default (Dow Jones / Nasdaq 100 / SP500 / ETF / Other) watchlists
curl -X GET http://localhost:5000/api/watchlists/defaults
```


## Project Structure

```
.
├── index.js                    # Main entry point
├── analyticsData.js            # Analytics calculations (returns, performance)
├── package.json                # Dependencies and scripts
├── .env                         # Environment variables (not in repo)
├── service-account.json         # Google Cloud credentials (not in repo)
│
├── config/                      # Configuration files
│   ├── bigquery.js             # BigQuery setup
│   ├── redis.js                # Redis/Upstash setup
│   └── supabase.js             # Supabase setup
│
├── controllers/                # Business logic
│   ├── authController.js       # Authentication logic
│   ├── marketController.js     # Market data & analytics logic
│   ├── tickerController.js     # Ticker management logic
│   └── watchlistController.js  # Watchlist management logic
│
├── middleware/                 # Express middleware
│   ├── authMiddleware.js       # Authentication verification
│   └── rateLimitMiddleware.js  # Rate limiting
│
└── routes/                     # API routes
    ├── authRoutes.js           # Auth endpoints
    ├── marketRoutes.js         # Market & analytics endpoints
    ├── tickerRoutes.js         # Ticker endpoints
    ├── userRoutes.js           # User profile endpoints
    └── watchlistRoutes.js      # Watchlist endpoints
```

## Middleware

### Authentication Middleware (`authMiddleware.js`)
Protects authenticated routes by verifying the user's token.

### Rate Limiting Middleware (`rateLimitMiddleware.js`)
Prevents abuse on login endpoint with request throttling.

## Analytics Module (`analyticsData.js`)

The analytics module provides comprehensive financial performance calculations:

### Supported Return Calculations

1. **Dynamic Periods** - Pre-defined time periods relative to today:
   - Last date, Week, Month, Quarter, 6 months, YTD, 1-10 year ranges, 15-50 year ranges

2. **Predefined Periods** - Specific year-to-date ranges:
   - Years: 1975, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024

3. **Annual Returns** - Year-by-year performance breakdown

4. **Monthly Returns** - Month-over-month performance tracking

5. **Quarterly Returns** - Quarter-over-quarter performance analysis

6. **Custom Range** - User-defined date ranges for custom analysis

### Metrics Provided

For each period, the API returns:
- **Start/End Dates** - Both requested and actual available dates
- **Prices** - Opening and closing prices for the period
- **Price Difference** - Absolute price change
- **Total Return %** - Percentage gain/loss
- **Simple Annual Return** - Annualized return rate
- **CAGR %** - Compound Annual Growth Rate

### Data Sources

- Stock prices from BigQuery (`stock_all_data` table)
- Ticker metadata from BigQuery (`TickerDetails` table)
- Adjusted close prices preferred over regular close

## Security Features

- ✅ CORS enabled for frontend integration
- ✅ Rate limiting on login endpoint
- ✅ Authentication required for protected routes
- ✅ Secure token-based authentication via Supabase
- ✅ Environment variables for sensitive data
- ✅ BigQuery authentication via service account

## How to Use the Analytics Endpoints

### Step 1: Get Available Indices
```bash
curl -X GET http://localhost:5000/api/market/indices \
  -H "Authorization: Bearer <token>"
```
Returns available stock indices (S&P 500, Nasdaq 100, Dow Jones, etc.)

### Step 2: Get Period Options
```bash
curl -X GET http://localhost:5000/api/market/period-options \
  -H "Authorization: Bearer <token>"
```
Returns available time periods for analysis

### Step 3a: Get Ticker Details by Index (Performance Return View)
```bash
curl -X POST http://localhost:5000/api/market/ticker-details \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"index": "sp500", "period": "last-1-year"}'
```
Returns all tickers in an index with their performance for the selected period

### Step 3b: Get Detailed Ticker Analysis (Multiple Return Types)
```bash
curl -X POST http://localhost:5000/api/market/ticker-returns \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL"}'
```
Returns comprehensive performance data including:
- Dynamic periods (Week, Month, YTD, 1-10 years, etc.)
- Predefined periods (specific year ranges)
- Annual returns (year-by-year breakdown)
- Monthly returns (month-over-month changes)
- Quarterly returns (quarterly breakdown)
- Custom date ranges (optional)

### Step 4: Get Monthly OHLC Data
```bash
curl -X POST http://localhost:5000/api/market/monthly-ohlc \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "start_date": "2023-01-01", "end_date": "2023-12-31"}'
```
Returns monthly aggregated Open, High, Low, Close prices

## Getting Started

### 1. Setup Database (Supabase)
- Create a Supabase project
- Set up authentication enabled
- Configure your `.env` with Supabase credentials

### 2. Setup Cache (Upstash Redis)
- Create Upstash Redis database
- Add credentials to `.env`

### 3. Setup Analytics (Google BigQuery)
- Create Google Cloud project
- Enable BigQuery API
- Create two tables:
  - `stock_all_data` - Contains Ticker, Date, Open, High, Low, Close, Adj Close columns
  - `TickerDetails` - Contains Symbol, Security, Sector, Industry, Index columns
- Download service account credentials JSON
- Set `GOOGLE_CLOUD_PROJECT`, `BIGQUERY_DATASET`, `BIGQUERY_TABLE` in `.env`

### 4. Install Dependencies
```bash
npm install
```

### 5. Start Development Server
```bash
npm run dev
```

The server will run on `http://localhost:5000`

## Development

### Scripts
```bash
npm start      # Start production server
npm run dev    # Start with nodemon (auto-reload)
```

### Testing Endpoints
Use tools like:
- Postman
- Insomnia
- curl (command line)
- VS Code REST Client extension

## API Request/Response Examples

### Authentication Example

**Login Request:**
```bash
curl -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securepassword"
  }'
```

**Response:**
```json
{
  "success": true,
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {
    "id": "user-123",
    "email": "user@example.com"
  }
}
```

### Ticker Returns Example (Detailed)

**Request:**
```bash
curl -X POST http://localhost:5000/api/market/ticker-returns \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "ticker": "MSFT",
    "customStartDate": "2024-01-01",
    "customEndDate": "2026-03-05"
  }'
```

**Key Response Fields:**
- `dynamicPeriods` - Fixed intervals relative to today
- `annualReturns` - Yearly breakdown starting from earliest available date
- `monthlyReturns` - Month-to-month performance
- `quarterlyReturns` - Quarter-to-quarter performance
- `customRange` - Optional custom date range analysis

**Example Period Data:**
```json
{
  "period": "Last 1 year",
  "startDate": "2025-03-05",
  "endDate": "2026-03-05",
  "years": 1.0,
  "startPrice": 380.50,
  "endPrice": 425.75,
  "priceDifference": 45.25,
  "totalReturn": 11.89,
  "simpleAnnualReturn": 11.89,
  "cagrPercent": 11.89
}
```

### Performance Return Example (Index-based)

**Request:**
```bash
curl -X POST http://localhost:5000/api/market/ticker-details \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "index": "sp500",
    "period": "last-1-year"
  }'
```

**Response (first 3 tickers):**
```json
{
  "success": true,
  "index": "sp500",
  "period": "last-1-year",
  "data": [
    {
      "row": 1,
      "symbol": "AAPL",
      "security": "Apple Inc.",
      "sector": "Technology",
      "industry": "Computer Manufacturing",
      "index": "S&P 500",
      "totalReturnPercentage": 28.45,
      "price": 185.64
    },
    {
      "row": 2,
      "symbol": "MSFT",
      "security": "Microsoft Corporation",
      "sector": "Technology",
      "industry": "Software",
      "index": "S&P 500",
      "totalReturnPercentage": 32.67,
      "price": 425.75
    },
    {
      "row": 3,
      "symbol": "NVDA",
      "security": "NVIDIA Corporation",
      "sector": "Technology",
      "industry": "Semiconductors",
      "index": "S&P 500",
      "totalReturnPercentage": 45.82,
      "price": 875.30
    }
  ]
}
```

## API Response Format

### Success Response
```json
{
  "success": true,
  "data": { /* response data */ }
}
```

### Error Response
```json
{
  "success": false,
  "error": "Error message"
}
```

## Rate Limiting

- **Login endpoint:** Rate limited to prevent brute force attacks
- **Other authenticated endpoints:** Standard rate limiting applies

## Common Issues

### "Cannot find module"
```bash
npm install
```

### "Invalid token"
Ensure your authentication token is valid and included in request headers

### "Port already in use"
Change PORT in `.env` or kill the process using the port

## License

ISC

## Support

For issues or questions, please contact the development team.

