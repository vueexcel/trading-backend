# Site Development Plan (Frontend + Backend Integration)

## 1) Purpose of this document
- Provide a frontend-ready integration guide for the current TradingBackend.
- Explain what each backend endpoint does, what payload it expects, and what response it returns.
- Define implementation priorities and architecture for security, SEO, speed, and scalability.
- Give a phased execution plan your frontend engineer can use immediately.

## 2) Current backend system overview

### Core stack
- Runtime/API: Node.js + Express
- Authentication + relational data: Supabase Auth + Supabase Postgres
- Market analytics data: Google BigQuery
- Caching layer (available): Upstash Redis (credentials present in env)

### Main backend modules
- Route mounting: `index.js`
- Auth middleware: `middleware/authMiddleware.js`
- Login rate limiter: `middleware/rateLimitMiddleware.js`
- Market APIs: `controllers/marketController.js`, `routes/marketRoutes.js`
- Odin API: `controllers/analyticsController.js`, `routes/analyticsRoutes.js`
- Ticker returns/details service: `analyticsData.js`
- Ticker groups/search: `controllers/tickerController.js`
- Watchlists: `controllers/watchlistController.js`

### Data sources in use
- BigQuery price table (env): `GOOGLE_CLOUD_PROJECT.BIGQUERY_DATASET.BIGQUERY_TABLE` (currently `stock_all_data`)
- BigQuery ticker metadata table (env): `GOOGLE_CLOUD_PROJECT.BIGQUERY_DATASET.TICKER_DETAILS_TABLE` (currently `TickerDetails`)
- Supabase tables (used by controllers): users/auth, market_groups, tickers, ticker_groups, watchlists, watchlist_items

## 3) Security model (current and target)

### Current
- Most APIs are protected by Bearer token via `requireAuth`.
- Login route has IP rate limiting (5 attempts / 15 min).
- Supabase JWT is verified on each protected request.

### Required improvements (priority 1)
- Add endpoint-level rate limits for expensive endpoints:
  - `/api/market/ticker-returns`
  - `/api/market/ticker-details`
  - `/api/analytics/odin-index`
- Add strict payload validation for all POST routes.
- Add centralized sanitized error responses.
- Add `helmet` + CORS allowlist by environment.
- Add request id and audit logging for sensitive actions.

## 4) SEO strategy (site-level)

### Public SEO pages (indexable)
- Market landing pages
- Index overview pages
- Symbol pages that do not expose private watchlist data

### Private pages (non-indexed)
- Authenticated dashboard
- Watchlists
- User profile and account pages

### SEO implementation checklist
- SSR/SSG framework (recommended: Next.js app router)
- Per-page metadata: title, description, canonical URL, OG/Twitter tags
- Structured data (JSON-LD): organization + breadcrumbs + financial instrument pages where applicable
- Dynamic sitemap generation
- Robots policy for private routes

## 5) Performance and scalability strategy

### Backend performance
- Use Redis cache for expensive/read-heavy endpoints.
- Add bounded query guards (date range limits, symbol limits, pagination).
- Add p95 latency monitoring by endpoint.
- Add BigQuery query timing and error-rate metrics.

### Frontend performance
- Route-level code splitting.
- Lazy-load heavy chart libraries.
- Optimize image/font loading.
- Client-side caching with stale-while-revalidate pattern.
- Web Vitals targets:
  - LCP < 2.5s
  - INP < 200ms
  - CLS < 0.1

### Scaling approach
- Stateless API deployment behind load balancer.
- Horizontal scale by API instance count.
- Redis shared cache for consistency across instances.
- Keep BigQuery reads optimized with caching + narrower query windows.

## 6) Redis integration plan (what it does and how it helps)

### Why Redis here
- Reduce repeated BigQuery cost and latency.
- Improve perceived frontend speed for repeated filter combinations.
- Absorb burst traffic without overloading analytics queries.

### Suggested first cache targets
- `GET /api/market/indices` (TTL 12h)
- `GET /api/market/period-options` (TTL 12h)
- `POST /api/market/ticker-details` (TTL 2-10 min by `index+period`)
- `POST /api/market/ticker-returns` (TTL 2-10 min by `ticker+customRange`)
- `POST /api/market/monthly-ohlc` and `/weekly-ohlc` (TTL 5-15 min by `ticker+range`)

### Cache key examples
- `market:indices:v1`
- `market:period-options:v1`
- `market:ticker-details:v1:{index}:{period}`
- `market:ticker-returns:v1:{ticker}:{start}:{end}`
- `market:monthly-ohlc:v1:{ticker}:{start}:{end}`

### Invalidation policy
- Primarily TTL-based for analytics endpoints.
- Manual busting endpoint (admin-only) can be added later.

## 7) Frontend integration layer design

### API client requirements
- Centralized typed API service module.
- Automatic Bearer token injection for protected routes.
- Uniform error model mapping (`{success:false,error:...}` and HTTP errors).
- Request retries only for idempotent GET endpoints.

### Response normalization
- Normalize all date fields to ISO strings.
- Normalize numeric fields to numbers or null (never mixed string/number).
- Provide fallback placeholders for null analytics values in UI.

### Recommended app structure
- `services/api/` for endpoint wrappers
- `services/contracts/` for types/interfaces
- `features/market/`, `features/watchlist/`, `features/analytics/`
- `lib/auth/` for token/session handling

## 8) Complete endpoint catalog for frontend

Base URL: `http://localhost:5000`

Auth header for protected routes:
- `Authorization: Bearer <access_token>`

---

### 8.1 Health/root
- **GET** `/`
- Purpose: basic server liveness check
- Auth: No
- Request: none
- Response: text message `"Trading App Backend is running!"`

---

### 8.2 Authentication

#### POST `/api/auth/signup`
- Purpose: create user account
- Auth: No
- Body:
```json
{
  "email": "user@example.com",
  "password": "strongPassword"
}
```
- Response (201):
```json
{
  "message": "User created successfully",
  "data": {
    "user": {},
    "session": {}
  }
}
```

#### POST `/api/auth/login`
- Purpose: user login (rate-limited)
- Auth: No
- Body:
```json
{
  "email": "user@example.com",
  "password": "strongPassword"
}
```
- Response (200):
```json
{
  "message": "Login successful",
  "session": {
    "access_token": "...",
    "refresh_token": "...",
    "user": {}
  }
}
```

#### POST `/api/auth/logout`
- Purpose: logout
- Auth: currently not protected in route file
- Body: none
- Response (200):
```json
{ "message": "Logged out successfully" }
```

---

### 8.3 User

#### GET `/api/user/profile`
- Purpose: fetch authenticated user context
- Auth: Yes
- Response (200):
```json
{
  "message": "Welcome to your private profile!",
  "userEmail": "user@example.com",
  "userId": "..."
}
```

---

### 8.4 Market data

#### GET `/api/market/ohlc`
- Purpose: daily OHLC data for charting
- Auth: Yes
- Query params:
  - `symbol` (required)
  - `start_date` (optional, must be paired with `end_date`)
  - `end_date` (optional, must be paired with `start_date`)
  - `limit` (optional, max guarded in controller)
- Example:
  - `/api/market/ohlc?symbol=AAPL&start_date=2025-01-01&end_date=2025-12-31&limit=250`
- Response (200):
```json
{
  "symbol": "AAPL",
  "data": [
    {
      "Ticker": "AAPL",
      "Date": "2025-12-31",
      "Open": 250.1,
      "High": 252.0,
      "Low": 248.8,
      "Close": 251.6
    }
  ]
}
```

#### POST `/api/market/monthly-ohlc`
- Purpose: monthly aggregated OHLC
- Auth: Yes
- Body:
```json
{
  "ticker": "AAPL",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31"
}
```
- Response (200):
```json
{
  "success": true,
  "ticker": "AAPL",
  "monthlyOHLC": [
    {
      "ticker": "AAPL",
      "year": 2024,
      "month": 1,
      "open": 182.5,
      "high": 191.2,
      "low": 180.0,
      "close": 189.7,
      "adj_close": 189.7,
      "start_date": "2024-01-01",
      "end_date": "2024-01-31"
    }
  ]
}
```

#### POST `/api/market/weekly-ohlc`
- Purpose: weekly aggregated OHLC
- Auth: Yes
- Body:
```json
{
  "ticker": "AAPL",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31"
}
```
- Response (200):
```json
{
  "success": true,
  "ticker": "AAPL",
  "weeklyOHLC": [
    {
      "ticker": "AAPL",
      "year": 2024,
      "week": 1,
      "open": 182.5,
      "high": 186.0,
      "low": 181.9,
      "close": 185.4,
      "adj_close": 185.4,
      "start_date": "2024-01-02",
      "end_date": "2024-01-05"
    }
  ]
}
```

---

### 8.5 Market analytics endpoints

#### GET `/api/market/indices`
- Purpose: available index filters
- Auth: Yes
- Response:
```json
{
  "success": true,
  "indices": ["S&P 500", "Nasdaq 100", "Dow Jones", "ETF"]
}
```

#### GET `/api/market/period-options`
- Purpose: time period dropdown options
- Auth: Yes
- Response:
```json
{
  "success": true,
  "periods": [
    { "value": "last-1-year", "label": "Last 1 year" }
  ]
}
```

#### POST `/api/market/ticker-details`
- Purpose: screener-style list by index + period with return%
- Auth: Yes
- Body:
```json
{
  "index": "sp500",
  "period": "last-1-year"
}
```
- Response:
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
      "industry": "Consumer Electronics",
      "index": "S&P 500",
      "totalReturnPercentage": 22.45,
      "price": 251.64
    }
  ]
}
```

Notes:
- Start date resolution uses first trading day on/after requested start.
- End date resolution uses last trading day on/before requested end.

#### POST `/api/market/ticker-returns`
- Purpose: full returns payload for one ticker
- Auth: Yes
- Body:
```json
{
  "ticker": "AAPL",
  "customStartDate": "2024-01-01",
  "customEndDate": "2024-12-31"
}
```
- Response:
```json
{
  "success": true,
  "ticker": "AAPL",
  "asOfDate": "2026-03-24",
  "performance": {
    "dynamicPeriods": [],
    "predefinedPeriods": [],
    "annualReturns": [],
    "customRange": [],
    "quarterlyReturns": [],
    "monthlyReturns": []
  }
}
```

---

### 8.6 Tickers

#### GET `/api/tickers/groups`
- Purpose: group tabs for market categories
- Auth: Yes
- Response: list of groups with `id/name/code`

#### GET `/api/tickers/search?q=<text>&group=<optionalCode>`
- Purpose: symbol/company search, optional filter by group
- Auth: Yes
- Response: list of matching tickers

#### GET `/api/tickers/group/:code`
- Purpose: tickers inside a group
- Auth: Yes
- Response:
```json
{
  "group_code": "SP",
  "count": 500,
  "tickers": [
    { "id": "...", "symbol": "AAPL", "company_name": "Apple Inc." }
  ]
}
```

---

### 8.7 Watchlists

#### GET `/api/watchlists/defaults`
- Purpose: public default grouped watchlists
- Auth: No
- Response: list of groups and item arrays

#### POST `/api/watchlists`
- Purpose: create user watchlist
- Auth: Yes
- Body:
```json
{ "name": "My Watchlist" }
```

#### GET `/api/watchlists`
- Purpose: fetch user watchlists with ticker details
- Auth: Yes

#### POST `/api/watchlists/add`
- Purpose: bulk add ticker ids to watchlist
- Auth: Yes
- Body:
```json
{
  "watchlist_id": "uuid",
  "ticker_ids": ["uuid1", "uuid2"]
}
```

#### DELETE `/api/watchlists/:watchlist_id/remove/:ticker_id`
- Purpose: remove ticker from watchlist
- Auth: Yes

---

### 8.8 Odin analytics

#### POST `/api/analytics/odin-index`
- Purpose: Odin backtest + trade log + equity curve
- Auth: Yes
- Body:
```json
{
  "ticker": "AAPL",
  "start_date": "2025-09-15",
  "end_date": "2026-03-05",
  "initial_portfolio": 1000
}
```
- Response includes:
  - summary metrics (`final_portfolio`, `total_return_pct`)
  - `equity_curve`
  - `odin_sheet_headers`
  - `odin_sheet_rows`

## 9) Frontend screen-to-endpoint integration map

### Header/global
- Auth state: `/api/user/profile`
- Logout: `/api/auth/logout`

### Market chart pages
- Daily chart: `/api/market/ohlc`
- Weekly chart: `/api/market/weekly-ohlc`
- Monthly chart: `/api/market/monthly-ohlc`

### Screener page
- Index dropdown: `/api/market/indices`
- Period dropdown: `/api/market/period-options`
- Table rows: `/api/market/ticker-details`

### Ticker analytics page
- Returns blocks/charts: `/api/market/ticker-returns`
- Odin blocks/table: `/api/analytics/odin-index`

### Watchlist page
- Defaults (public): `/api/watchlists/defaults`
- User watchlists: `/api/watchlists`
- Create/add/remove: `/api/watchlists`, `/api/watchlists/add`, `/api/watchlists/:id/remove/:ticker_id`

## 10) API quality contract for frontend team

- Date fields must be treated as ISO strings.
- Numeric analytics values may be null if no valid trading boundary exists.
- All protected endpoints require Bearer token.
- POST validation errors return 400 with `{ success:false, error: "..." }` (or `{ error: "..." }` on some routes; standardization recommended).

## 11) Gaps and recommended backend enhancements for production

- Standardize error shape across all controllers.
- Add OpenAPI spec generation and contract tests.
- Add endpoint-specific cache + rate limiting.
- Add health endpoint: `/health` and dependency checks.
- Add response metadata fields for resolved dates:
  - `startDateRequested`, `startDateUsed`
  - `endDateRequested`, `endDateUsed`
- Introduce API versioning (`/api/v1/...`) before major frontend release.

## 12) Delivery plan (execution order)

### Phase 1: Contract freeze and security hardening
- Lock current payload/response schemas.
- Add request validation and security middleware.
- Add endpoint-level rate limits.

### Phase 2: Frontend integration baseline
- Implement API client and typed models.
- Integrate market charts, screener, returns, watchlists.
- Add null-safe UI behavior for analytics fields.

### Phase 3: SEO + performance
- SSR metadata, sitemap, robots, canonical strategy.
- Add Redis caching for high-latency endpoints.
- Add Web Vitals monitoring and optimize chart pages.

### Phase 4: Scale + operations
- Add observability dashboards and alerts.
- Load test critical routes.
- Define runbooks and rollback strategy.

## 13) Handoff checklist for frontend engineer

- Base URL and environment config documented.
- Auth token flow verified end-to-end.
- All endpoint payloads and response types mapped.
- Error states and null states handled in UI.
- Chart pages tested with daily/weekly/monthly ranges.
- Performance budget and SEO checks included in CI.

