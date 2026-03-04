# Signalbase

Real-time business intelligence for AI agents. Pay per request with USDC on Base via x402.

**Live API:** https://signalbase-production.up.railway.app
**OpenAPI spec:** https://signalbase-production.up.railway.app/openapi.json

## Why Signalbase

Most data APIs return raw search results and call it intelligence. Signalbase is different. Every signal passes through a multi-stage quality pipeline before it reaches your agent: multi-source aggregation across web, social, and news, followed by pattern-based filtering, deduplication, and LLM-powered classification. The result is a feed of verified events and real buying signals, not listicles, think-pieces, or self-promotional noise.

We built this because AI agents deserve data they can act on without second-guessing.

## What Your Agent Gets

**8 signal categories, updated daily:**

| Category | What it captures |
|---|---|
| `lead_signal` | People actively looking to buy a product, tool, or service. Vertical-tagged across 15 industries with author metadata. |
| `company_intel` | Funding rounds, product launches, team expansions, strategic moves. |
| `competitor_news` | Outages, acquisitions, pricing shifts, shutdowns. Real events only. |
| `market_trend` | Emerging platforms, protocols, and infrastructure patterns. |
| `funding_signal` | Verified funding events with dollar amounts and named companies. |
| `hiring_signal` | Hiring announcements and executive appointments. LLM-verified. |
| `developer_signal` | Open source releases, framework launches, new SDKs. |
| `pricing_intel` | Price hikes, plan changes, and billing backlash. |

**130+ signals per day** across all categories. Each scored by intent relevance. Garbage filtered. Seller-free. Decision-ready.

## Example Signal

```json
{
  "id": "sha256-hash",
  "category": "lead_signal",
  "signal_type": "buying_intent",
  "title": "Can anyone recommend brokers/platforms with live time updates?",
  "url": "https://x.com/i/web/status/...",
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

Preview endpoints return compact items (10 fields). Paid endpoints return full data including author metadata, industry verticals, LLM verdicts, and content excerpts.

## Agent Discovery Flow

Your agent can self-serve the entire journey, no human setup required:

1. `GET /openapi.json` — machine-readable API spec with every endpoint, parameter, and response shape
2. `GET /health` — pricing, x402 payment config, data freshness
3. `GET /preview/catalog` — all categories with item counts and descriptions
4. `GET /preview/leads?limit=5` — sample the data before paying
5. `GET /leads` — triggers 402, pay USDC, get full data

## Pricing (x402 USDC on Base)

| Endpoint | Price |
|---|---|
| `GET /feed` | $0.10 |
| `GET /leads` | $0.05 |
| `GET /companies` | $0.05 |
| `GET /competitors` | $0.03 |
| `GET /market` | $0.03 |
| `GET /funding` | $0.03 |
| `GET /hiring` | $0.03 |
| `GET /developer` | $0.03 |

Free endpoints: `/health`, `/preview/*`, `/openapi.json`

## Payment Flow

1. Agent requests a paid endpoint
2. Server responds `402 Payment Required` with payment instructions
3. Agent's wallet sends USDC on Base
4. Agent re-sends request with payment proof in `X-Payment` header
5. Server verifies via x402 facilitator and returns full data

No API keys. No subscriptions. No rate limits. Pay per request.

## Data Freshness

Feeds are refreshed daily. Check `GET /health` for `data_freshness.latest_feed_date` and `generated_at` timestamps. The API always serves the most recent feed.
