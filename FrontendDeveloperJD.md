# Frontend Developer — Trading Analytics Platform

## About the project

We are building a professional-grade trading analytics web application that helps investors and traders analyze stock market data, track signal performance, and manage watchlists across major indices (S&P 500, Dow Jones, Nasdaq 100, and more). The backend API is fully built and documented. We need a skilled frontend developer to bring the product to life.

This is a medium-to-long-term engagement with room to grow as the platform scales.

---

## What you will build

You will design and implement the complete frontend of a data-heavy financial analytics platform. The application includes:

- A **market screener** — filter and rank stocks by index and time period with sortable return performance tables
- **Ticker detail pages** — comprehensive return analysis across multiple time horizons (weekly, monthly, quarterly, annual, custom ranges), with interactive price charts
- A **watchlist system** — users can create, manage, and monitor custom stock watchlists with live market data
- A **dashboard** — at-a-glance market overview showing signal summaries across indices
- **Authentication flows** — login, signup, and protected route management
- A **backtest / analytics view** — display trade logs, equity curves, and performance summaries from our signal engine

---

## Tech stack (required)

- **Next.js 14** (App Router) — SSR and SSG for performance and SEO
- **TypeScript** — strict typing throughout
- **Tailwind CSS** — utility-first styling
- **shadcn/ui** — accessible component primitives
- **React Query (TanStack Query)** — server state management, caching, and background refetch
- **Zustand** — lightweight client state
- **Recharts or TradingView Lightweight Charts** — financial chart rendering

---

## Responsibilities

- Build all pages and components from the ground up following our design system
- Implement a typed API client layer that connects to our REST backend (all endpoints documented)
- Handle all loading, error, and empty states gracefully across the UI
- Ensure the app is fully responsive — mobile, tablet, and desktop
- Implement authentication with token-based session management
- Optimize for Web Vitals: LCP < 2.5s, INP < 200ms, CLS < 0.1
- Write clean, maintainable, component-driven code with clear folder structure
- Integrate financial charts (daily, weekly, monthly OHLC candlestick and line views)
- Implement SEO best practices — per-page metadata, Open Graph tags, structured data, sitemap, and robots configuration
- Set up code splitting, lazy loading, and other Next.js performance patterns
- Write unit and component tests (Vitest + Testing Library) and E2E tests (Playwright) for critical flows

---

## Pages to build

1. **Login / Signup** — clean auth forms with validation and error handling
2. **Dashboard (home)** — market overview cards, recent signal change feed, default group watchlists
3. **Screener** — filterable, sortable table of 500+ tickers with return percentages; client-side search and pagination
4. **Ticker detail** — return analysis tables across six time groupings, interactive OHLC chart, signal history table, add-to-watchlist flow
5. **Watchlists** — user watchlist management with live ticker data, add/remove flows, default public group watchlists
6. **Odin analytics view** — backtest results, trade log table, equity curve chart, performance summary metrics and others

---

## What we provide

- Fully documented backend API (REST + JSON) — all endpoints, payloads, and response shapes are already defined and currently being worked on
- Tech stack decision and folder structure recommendation
- Component and page specifications with data requirements per screen
- Screen-to-endpoint integration map (you know exactly which API call each UI element makes)
- Design direction and UI reference (Tailwind + shadcn primitives)

---

## Requirements

**Must have:**
- 3+ years of frontend development experience
- Strong frontend and TypeScript portfolio (show us real projects)
- Experience building data-heavy UIs — tables with large datasets, sorting, filtering, pagination
- Experience integrating financial or analytics charts
- Understanding of React Server Components vs Client Components
- Solid grasp of authentication patterns (JWT, token refresh, protected routes)
- Experience with React Query or SWR for API state management
- Attention to detail — pixel-accurate UI, proper loading states, graceful error handling
- Strong communication — you will need to ask questions and flag blockers clearly

**Nice to have:**
- Experience with TradingView Lightweight Charts
- Background in fintech or trading applications
- Experience with Playwright for E2E testing
- Familiarity with SEO implementation in Next.js (metadata API, sitemap generation, JSON-LD)
- Experience with Upstash Redis or caching patterns on the frontend

---

## Engagement details

- **Type:** Contract (hourly or milestone-based, open to discussion)
- **Estimated duration:** 2–4 weeks for initial build, ongoing engagement possible
- **Hours per week:** 20–40 hours (flexible based on candidate)
- **Communication:** Weekly check-ins, async-first via Slack or similar
- **Time zone:** Flexible — we work async but prefer overlap of at least 4 hours with IST (India Standard Time)

---

## How to apply

Please include in your proposal:

1. Links to 2–3 relevant projects (Next.js / TypeScript / data-heavy UI preferred)
2. A brief description of the most complex frontend feature you have built and what made it challenging
3. Your estimated timeline to complete the initial 8-page build described above
4. Your hourly rate or preferred milestone structure
5. Any questions you have about the project

**Applications without portfolio links will not be considered.**

We are looking for someone who takes ownership, communicates proactively, and writes code they are proud of and also someone who can make it done really fast. If that sounds like you, we would love to hear from you.