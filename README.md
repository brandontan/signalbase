# Signalbase

Real-time business intelligence for AI agents. Pay per request with USDC on Base via x402.

**Live API:** https://signalbase-production.up.railway.app
**OpenAPI spec:** https://signalbase-production.up.railway.app/openapi.json
**Swagger docs:** https://signalbase-production.up.railway.app/docs

## Why Signalbase

Most "data APIs" hand you raw search results and call it intelligence. Signalbase runs a multi-stage quality pipeline that turns noisy web data into decision-ready signals your agent can act on immediately.

**4 search engines, each used to its strength.** Exa for semantic/neural discovery (statement-style queries with autoprompt). Brave Search for keyword news with extra snippets and discussion threads. Firecrawl for search-and-scrape with full markdown extraction. X API with author expansions, follower data, and context annotations. No single engine catches everything. We run all four and merge the results.

**3-layer quality filter before anything reaches your agent.** Layer 1: universal garbage filter drops guides, playbooks, listicles, predictions, and career advice articles. 40+ hard-drop patterns plus category-specific rules. Layer 2: seller disqualifier catches self-promoters, job posting bots, coaching threads, and bait tweets. Checks tweet text AND author bio. Normalizes Unicode for consistent matching. Layer 3: LLM classifier (GPT-4o-mini via OpenRouter) does a binary accept/reject pass on lead and hiring signals. Kills political commentary, spam, customer service complaints, and gibberish that survive keyword matching.

**The result:** lead_signal precision went from ~30% (raw keyword search) to 67-100% (after full pipeline). hiring_signal went from ~62% to ~100%. Every item in your feed is a real event, not a think-piece.

## 8 Signal Categories

| Category | What it captures | Source engines |
|---|---|---|
| `lead_signal` | People actively looking to buy a product, tool, or service | X API |
| `company_intel` | Funding rounds, product launches, team expansions | Brave, Exa |
| `competitor_news` | Outages, acquisitions, pricing shifts, shutdowns | Brave, Firecrawl |
| `market_trend` | Emerging platforms, protocols, infrastructure patterns | Exa, X API |
| `funding_signal` | Specific funding events with dollar amounts | Brave |
| `hiring_signal` | Real hiring events and executive appointments | Brave (LLM-filtered) |
| `developer_signal` | Open source releases, framework launches, new SDKs | Exa |
| `pricing_intel` | Price hikes, plan changes, billing backlash | Firecrawl, Brave |

Lead signals include industry vertical tags (15 verticals: hr, sales, marketing, devtools, ai_ml, automation, design, ecommerce, fintech, web3, healthcare, education, real_estate, pm_ops, cybersecurity) and author metadata (username, follower count) for prioritization.

## Signal Fields

Every item includes:

```json
{
  "id": "sha256-hash",
  "category": "lead_signal",
  "signal_type": "buying_intent",
  "title": "Can anyone recommend brokers/platforms with live time updates?",
  "url": "https://x.com/i/web/status/...",
  "source_engine": "x",
  "intent_score": 8,
  "published_at": "2026-03-04T12:00:00Z",
  "summary": "...",
  "content_excerpt": "...",
  "vertical": "fintech",
  "author": {
    "username": "issaomotosho",
    "followers": 1241
  },
  "llm_verdict": {
    "accepted": true,
    "reason": "requesting recommendations for platforms"
  }
}
```

Preview endpoints return compact items (10 fields). Paid endpoints return everything including author data, verticals, LLM verdicts, and full content.

## Agent Discovery Flow

1. `GET /openapi.json` — machine-readable API spec (every endpoint, parameter, response shape)
2. `GET /health` — pricing, x402 config, data freshness
3. `GET /preview/catalog` — all categories with item counts and value descriptions
4. `GET /preview/leads?limit=5` — sample the data before paying
5. `GET /leads` — triggers 402, pay USDC, get full data

## Pricing (x402 USDC on Base)

| Endpoint | Price |
|---|---|
| `GET /feed` | $0.01 |
| `GET /leads` | $0.005 |
| `GET /companies` | $0.005 |
| `GET /competitors` | $0.003 |
| `GET /market` | $0.003 |
| `GET /funding` | $0.003 |
| `GET /hiring` | $0.003 |
| `GET /developer` | $0.003 |

Free endpoints: `/health`, `/preview/*`, `/openapi.json`, `/docs`

## Payment Flow

1. Agent requests a paid endpoint
2. Server responds `402 Payment Required` with payment instructions (price, pay-to address, network)
3. Agent's wallet sends USDC on Base to the pay-to address
4. Agent re-sends request with payment proof in `X-Payment` header
5. Server verifies via x402 facilitator and returns full data

No API keys. No subscriptions. No rate limits. Pay per request.

## Data Freshness

The scraper runs daily at 6am UTC via GitHub Actions cron. Each run produces a dated feed in `/data/{date}/feed.json`. The API always serves the most recent feed. Check `GET /health` for `data_freshness.latest_feed_date`.

## Architecture

```
GitHub Actions (6am cron)
  → POST /cron/scrape
    → scraper.py
      → Exa (semantic search, 6 queries)
      → Brave (keyword search, 13 queries)
      → Firecrawl (search + scrape, 4 queries)
      → X API (social intent, 6 queries)
      → Garbage filter (40+ patterns)
      → Dedup + quality floor
      → LLM classifier (OpenRouter)
      → feed.json
    → api.py (FastAPI + x402 middleware)
      → Railway (auto-deploy from main)
```

## Cost Structure

| Service | Monthly cost |
|---|---|
| X API Basic | $200 |
| Hunter.io (lead enrichment) | $49 |
| Firecrawl (search + scrape) | ~$30 |
| Railway (hosting) | ~$10-20 |
| OpenRouter LLM classifier | ~$0.12 |
| Exa | free tier |
| Brave Search | free tier |
| **Total** | **~$290/month** |
