# Signalbase API

Decision-ready intelligence signals for AI agents.

Live API: https://signalbase-production.up.railway.app
OpenAPI docs: https://signalbase-production.up.railway.app/docs

## What You Get

Signalbase serves structured, machine-readable signal feeds across 7 categories:

- `lead_signal`
- `market_trend`
- `company_intel`
- `competitor_news`
- `funding_signal`
- `hiring_signal`
- `developer_signal`

Each signal includes normalized fields such as:

- `entity`
- `signal_type`
- `intent_score`
- `confidence`
- `impact_score`
- `signal_window`
- `source_engine`
- `published_at`

## Example Signal

```json
{
  "signal_type": "hiring",
  "entity": {
    "name": "Anthropic",
    "type": "company"
  },
  "category": "hiring_signal",
  "title": "Anthropic expanding agent infrastructure team",
  "confidence": 0.82,
  "impact_score": 0.74,
  "signal_window": "24h",
  "intent_score": 8,
  "source_engine": "x",
  "url": "https://example.com/post",
  "published_at": "2026-03-04T05:00:00Z"
}
```

## Pricing (x402)

- `GET /feed` — `$0.01`
- `GET /leads` — `$0.005`
- `GET /companies` — `$0.005`
- `GET /competitors` — `$0.003`
- `GET /market` — `$0.003`
- `GET /funding` — `$0.003`
- `GET /hiring` — `$0.003`
- `GET /developer` — `$0.003`

Free endpoint:

- `GET /health`

## Preview Endpoints

- `GET /preview`
- `GET /preview/catalog`
- `GET /preview/leads`
- `GET /preview/companies`
- `GET /preview/competitors`
- `GET /preview/market`
- `GET /preview/category/{id}`

## Payment Flow

Signalbase uses x402 USDC payment on Base:

1. Agent requests a paid endpoint.
2. Server responds with `402 Payment Required`.
3. Agent submits payment proof (`X-Payment`).
4. Server verifies and returns data.
