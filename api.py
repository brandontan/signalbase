from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.server import x402ResourceServer

load_dotenv()

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

PAY_TO_ADDRESS = os.getenv("PAY_TO_ADDRESS", "").strip() or ZERO_ADDRESS
X402_NETWORK = os.getenv("X402_NETWORK", "eip155:84532").strip()
_raw_facilitator_url = os.getenv("FACILITATOR_URL", "").strip()
if not _raw_facilitator_url or "api.x402.org" in _raw_facilitator_url:
    FACILITATOR_URL = "https://x402.org/facilitator"
else:
    FACILITATOR_URL = _raw_facilitator_url

PRICING: dict[str, str] = {
    "GET /feed": "$0.01",
    "GET /leads": "$0.005",
    "GET /companies": "$0.005",
    "GET /competitors": "$0.003",
    "GET /market": "$0.003",
}

app = FastAPI(
    title="Signalbase API",
    version="0.1.0",
    description="Paid web intelligence feed for AI agents using x402 USDC payments.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def payment_option(price: str) -> PaymentOption:
    return PaymentOption(
        scheme="exact",
        pay_to=PAY_TO_ADDRESS,
        price=price,
        network=X402_NETWORK,
    )


PAID_ROUTES: dict[str, RouteConfig] = {
    "GET /feed": RouteConfig(
        accepts=[payment_option("$0.01")],
        mime_type="application/json",
        description="Full daily Signalbase feed.",
    ),
    "GET /leads": RouteConfig(
        accepts=[payment_option("$0.005")],
        mime_type="application/json",
        description="Lead signals sorted by intent score.",
    ),
    "GET /companies": RouteConfig(
        accepts=[payment_option("$0.005")],
        mime_type="application/json",
        description="Company intelligence feed with optional signal type filter.",
    ),
    "GET /competitors": RouteConfig(
        accepts=[payment_option("$0.003")],
        mime_type="application/json",
        description="Competitor news feed.",
    ),
    "GET /market": RouteConfig(
        accepts=[payment_option("$0.003")],
        mime_type="application/json",
        description="Market trend feed.",
    ),
}

facilitator = HTTPFacilitatorClient(FacilitatorConfig(url=FACILITATOR_URL))
resource_server = x402ResourceServer(facilitator)
resource_server.register(X402_NETWORK, ExactEvmServerScheme())

app.add_middleware(
    PaymentMiddlewareASGI,
    routes=PAID_ROUTES,
    server=resource_server,
)


def list_feed_paths() -> list[Path]:
    if not DATA_DIR.exists():
        return []
    return sorted(DATA_DIR.glob("*/feed.json"), reverse=True)


def load_latest_feed() -> tuple[dict[str, Any], Path]:
    candidates = list_feed_paths()
    if not candidates:
        raise HTTPException(
            status_code=404,
            detail="No feed found. Run `python scraper.py` to generate daily data.",
        )
    feed_path = candidates[0]
    with feed_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload, feed_path


def filter_category(feed: dict[str, Any], category: str) -> list[dict[str, Any]]:
    items = feed.get("items", [])
    if not isinstance(items, list):
        return []
    return [item for item in items if item.get("category") == category]


@app.get("/feed")
async def get_feed() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    return feed


@app.get("/leads")
async def get_leads(min_intent: int = Query(default=7, ge=1, le=10)) -> dict[str, Any]:
    feed, _ = load_latest_feed()
    leads = [
        item
        for item in filter_category(feed, "lead_signal")
        if int(item.get("intent_score") or 0) >= min_intent
    ]
    leads.sort(key=lambda item: int(item.get("intent_score") or 0), reverse=True)
    return {
        "date": feed.get("date"),
        "count": len(leads),
        "min_intent": min_intent,
        "items": leads,
    }


@app.get("/companies")
async def get_companies(signal_type: str | None = Query(default=None)) -> dict[str, Any]:
    feed, _ = load_latest_feed()
    companies = filter_category(feed, "company_intel")
    if signal_type:
        requested = signal_type.strip().lower()
        companies = [
            item
            for item in companies
            if str(item.get("signal_type") or "").lower() == requested
        ]
    return {
        "date": feed.get("date"),
        "count": len(companies),
        "signal_type": signal_type,
        "items": companies,
    }


@app.get("/competitors")
async def get_competitors() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "competitor_news")
    return {
        "date": feed.get("date"),
        "count": len(items),
        "items": items,
    }


@app.get("/market")
async def get_market() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "market_trend")
    return {
        "date": feed.get("date"),
        "count": len(items),
        "items": items,
    }


@app.get("/health")
async def get_health() -> dict[str, Any]:
    latest_feed_date = None
    generated_at = None
    latest_path = None

    try:
        feed, path = load_latest_feed()
        latest_feed_date = feed.get("date")
        generated_at = feed.get("generated_at")
        latest_path = str(path)
    except HTTPException:
        pass

    return {
        "status": "ok",
        "pricing": PRICING,
        "x402": {
            "network": X402_NETWORK,
            "facilitator_url": FACILITATOR_URL,
            "pay_to_address": None if PAY_TO_ADDRESS == ZERO_ADDRESS else PAY_TO_ADDRESS,
        },
        "data_freshness": {
            "latest_feed_date": latest_feed_date,
            "generated_at": generated_at,
            "latest_feed_path": latest_path,
            "available_feeds": len(list_feed_paths()),
        },
    }
