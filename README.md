# Signalbase

Signalbase is data infrastructure for AI agents: a Python service that scrapes fresh web intelligence daily and serves it over paid HTTP endpoints.

- No API keys for consumers
- No accounts
- Agents pay per request in USDC on Base via x402

## Stack

- Python 3.11
- FastAPI + uvicorn
- exa-py (semantic web search)
- firecrawl-py (clean extraction)
- x402 (Coinbase payment middleware)
- python-dotenv

## Project Structure

```text
signalbase/
├── scraper.py
├── api.py
├── requirements.txt
├── .env.example
└── README.md
```

## x402 Payment Flow (4 Steps)

1. Agent requests a paid endpoint (for example `GET /leads`).
2. API returns `402 Payment Required` with x402 payment requirements (amount, network, recipient).
3. Agent submits a signed payment payload (USDC on Base) in a follow-up request.
4. x402 middleware verifies payment through the facilitator and the API returns data.

`/health` is intentionally free and not protected by payment middleware.

## Pricing

| Endpoint | Price | Notes |
|---|---:|---|
| `GET /feed` | $0.01 | Full daily feed |
| `GET /leads` | $0.005 | Lead signals, sorted by intent score, supports `?min_intent=7` |
| `GET /companies` | $0.005 | Company intel, supports `?signal_type=funding` |
| `GET /competitors` | $0.003 | Competitor news |
| `GET /market` | $0.003 | Market trends |
| `GET /health` | Free | Pricing + freshness + network metadata |

## Setup

1. Create and activate a Python 3.11 virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create `.env` from `.env.example` and fill in values:

```env
EXA_API_KEY=
FIRECRAWL_API_KEY=
PAY_TO_ADDRESS=
X402_NETWORK=eip155:84532
FACILITATOR_URL=https://x402.org/facilitator
```

## Run the Scraper (Daily Pipeline)

Generate the daily feed:

```bash
python scraper.py
```

Output file:

```text
data/YYYY-MM-DD/feed.json
```

Example cron (every day at 06:00 UTC):

```cron
0 6 * * * cd /path/to/signalbase && /path/to/venv/bin/python scraper.py >> /tmp/signalbase-scraper.log 2>&1
```

## Start the API

```bash
uvicorn api:app --reload --port 8000
```

Health endpoint:

```bash
curl http://localhost:8000/health
```

## Endpoint Behavior

- All endpoints are async.
- CORS is enabled.
- All endpoints except `/health` require x402 payment.
- Data is served from the latest `data/*/feed.json`.

## Example Agent Client (Python)

This example shows the request pattern. In production, use an x402-capable client to satisfy the 402 response.

```python
import requests

BASE_URL = "http://localhost:8000"

resp = requests.get(f"{BASE_URL}/leads?min_intent=8", timeout=30)

if resp.status_code == 402:
    payment_requirements = resp.json()
    print("Payment required:", payment_requirements)
    # 1) Build/supply x402 payment header using your agent wallet.
    # 2) Re-issue request with payment proof headers.
elif resp.ok:
    print(resp.json())
else:
    print(resp.status_code, resp.text)
```

## Notes

- Default x402 network is Base Sepolia (`eip155:84532`).
- Set `PAY_TO_ADDRESS` to your Base wallet address before serving paid requests.
