from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.facilitator_client_base import CreateHeadersAuthProvider
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.server import x402ResourceServer

load_dotenv(override=True)

ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(ROOT / "data")))
CRON_SECRET = os.getenv("CRON_SECRET", "").strip()
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

PAY_TO_ADDRESS = os.getenv("PAY_TO_ADDRESS", "").strip() or ZERO_ADDRESS
X402_NETWORK = os.getenv("X402_NETWORK", "eip155:8453").strip()

# CDP facilitator auth (Coinbase Developer Platform)
CDP_API_KEY_ID = os.getenv("CDP_API_KEY_ID", "").strip()
CDP_API_KEY_SECRET = os.getenv("CDP_API_KEY_SECRET", "").strip()

if CDP_API_KEY_ID and CDP_API_KEY_SECRET:
    # Use Coinbase CDP facilitator with auth (supports Base mainnet)
    from cdp.x402 import create_facilitator_config as _create_cdp_config
    _cdp_config = _create_cdp_config(
        api_key_id=CDP_API_KEY_ID,
        api_key_secret=CDP_API_KEY_SECRET,
    )
    FACILITATOR_URL = _cdp_config["url"]
    _FACILITATOR_AUTH = CreateHeadersAuthProvider(_cdp_config["create_headers"])
else:
    # Fallback: unauthenticated facilitator (testnet only)
    _raw_facilitator_url = os.getenv("FACILITATOR_URL", "").strip()
    if not _raw_facilitator_url or "api.x402.org" in _raw_facilitator_url:
        FACILITATOR_URL = "https://x402.org/facilitator"
    else:
        FACILITATOR_URL = _raw_facilitator_url
    _FACILITATOR_AUTH = None

PRICING: dict[str, str] = {
    "GET /feed": "$0.10",
    "GET /leads": "$0.05",
    "GET /companies": "$0.05",
    "GET /competitors": "$0.03",
    "GET /market": "$0.03",
    "GET /funding": "$0.03",
    "GET /hiring": "$0.03",
    "GET /developer": "$0.03",
}

DATA_CATALOG: list[dict[str, Any]] = [
    {
        "id": "lead_signal",
        "name": "Buyer Intent Signals",
        "status": "active",
        "paid_endpoint": "/leads",
        "preview_endpoint": "/preview/leads",
        "why_agents_buy": "Find live demand from teams asking for data/API/agent integration help.",
    },
    {
        "id": "company_intel",
        "name": "Company Intelligence",
        "status": "active",
        "paid_endpoint": "/companies",
        "preview_endpoint": "/preview/companies",
        "why_agents_buy": "Track funding, hiring, pricing, and launch events that trigger outreach and strategy.",
    },
    {
        "id": "competitor_news",
        "name": "Competitor Moves",
        "status": "active",
        "paid_endpoint": "/competitors",
        "preview_endpoint": "/preview/competitors",
        "why_agents_buy": "Detect competitor outages, launches, and pricing shifts for fast response workflows.",
    },
    {
        "id": "market_trend",
        "name": "Market Trends",
        "status": "active",
        "paid_endpoint": "/market",
        "preview_endpoint": "/preview/market",
        "why_agents_buy": "Capture emerging platform/protocol patterns to prioritize product bets.",
    },
    {
        "id": "web_search_intel",
        "name": "Agentic Web Search & Retrieval",
        "status": "planned",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/web_search_intel",
        "why_agents_buy": "Research agents pay for fresh search + citation-backed answers.",
    },
    {
        "id": "people_company_enrichment",
        "name": "People & Company Enrichment",
        "status": "planned",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/people_company_enrichment",
        "why_agents_buy": "GTM agents pay for person/company attributes to qualify and route leads.",
    },
    {
        "id": "financial_market_data",
        "name": "Financial & Crypto Market Data",
        "status": "planned",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/financial_market_data",
        "why_agents_buy": "Trading/treasury agents pay for real-time prices, news, and event feeds.",
    },
    {
        "id": "pricing_intel",
        "name": "Pricing Intelligence",
        "status": "active",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/pricing_intel",
        "why_agents_buy": "Revenue and procurement agents monitor price changes and plan migration triggers.",
    },
    {
        "id": "product_launch_intel",
        "name": "Product Launch Intelligence",
        "status": "active",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/product_launch_intel",
        "why_agents_buy": "Product and BD agents track launches to prioritize integrations and partnership outreach.",
    },
    {
        "id": "funding_hiring_intel",
        "name": "Funding & Hiring Intelligence",
        "status": "active",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/funding_hiring_intel",
        "why_agents_buy": "Growth and sales agents use new funding/hiring as a timing signal for outreach.",
    },
    {
        "id": "protocol_signal",
        "name": "Protocol & Ecosystem Signals",
        "status": "active",
        "paid_endpoint": None,
        "preview_endpoint": "/preview/category/protocol_signal",
        "why_agents_buy": "Infra agents monitor MCP/x402 and ecosystem movement to adjust toolchains quickly.",
    },
]

BIG_CATEGORY_RULES: dict[str, dict[str, Any]] = {
    "lead_signal": {
        "source_categories": ["lead_signal"],
    },
    "company_intel": {
        "source_categories": ["company_intel"],
    },
    "competitor_news": {
        "source_categories": ["competitor_news"],
    },
    "market_trend": {
        "source_categories": ["market_trend"],
    },
    "pricing_intel": {
        "source_categories": ["company_intel", "competitor_news"],
        "signal_types": ["pricing"],
        "keywords": ["pricing", "price", "billing", "plan update", "price increase", "pricing change"],
    },
    "product_launch_intel": {
        "source_categories": ["company_intel", "competitor_news", "market_trend"],
        "signal_types": ["product_launch"],
        "keywords": ["launch", "launched", "release", "released", "shipped", "new product", "new feature"],
    },
    "funding_hiring_intel": {
        "source_categories": ["company_intel"],
        "signal_types": ["funding", "hiring"],
        "keywords": ["raised", "funding", "series a", "series b", "seed", "hiring", "recruiting", "job opening"],
    },
    "protocol_signal": {
        "source_categories": ["market_trend"],
        "keywords": ["x402", "mcp", "agent economy", "autonomous agent", "agentic", "marketplace"],
    },
    # Planned categories with placeholder preview routes; no in-feed rules yet.
    "web_search_intel": {
        "source_categories": [],
    },
    "people_company_enrichment": {
        "source_categories": [],
    },
    "financial_market_data": {
        "source_categories": [],
    },
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
        accepts=[payment_option("$0.10")],
        mime_type="application/json",
        description="Full daily Signalbase feed.",
    ),
    "GET /leads": RouteConfig(
        accepts=[payment_option("$0.05")],
        mime_type="application/json",
        description="Lead signals sorted by intent score.",
    ),
    "GET /companies": RouteConfig(
        accepts=[payment_option("$0.05")],
        mime_type="application/json",
        description="Company intelligence feed with optional signal type filter.",
    ),
    "GET /competitors": RouteConfig(
        accepts=[payment_option("$0.03")],
        mime_type="application/json",
        description="Competitor news feed.",
    ),
    "GET /market": RouteConfig(
        accepts=[payment_option("$0.03")],
        mime_type="application/json",
        description="Market trend feed.",
    ),
    "GET /funding": RouteConfig(
        accepts=[payment_option("$0.03")],
        mime_type="application/json",
        description="Funding signal feed.",
    ),
    "GET /hiring": RouteConfig(
        accepts=[payment_option("$0.03")],
        mime_type="application/json",
        description="Hiring signal feed.",
    ),
    "GET /developer": RouteConfig(
        accepts=[payment_option("$0.03")],
        mime_type="application/json",
        description="Developer signal feed.",
    ),
}

facilitator = HTTPFacilitatorClient(FacilitatorConfig(
    url=FACILITATOR_URL,
    auth_provider=_FACILITATOR_AUTH,
))
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


def item_text_blob(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("title") or ""),
        str(item.get("summary") or ""),
        str(item.get("content_excerpt") or ""),
        str(item.get("query") or ""),
        str(item.get("signal_type") or ""),
    ]
    return " ".join(parts).lower()


def filter_big_category(feed: dict[str, Any], category_id: str) -> list[dict[str, Any]]:
    config = BIG_CATEGORY_RULES.get(category_id)
    if not config:
        return []

    source_categories = set(config.get("source_categories") or [])
    if not source_categories:
        return []

    base_items = [
        item
        for item in feed.get("items", [])
        if str(item.get("category") or "") in source_categories
    ]

    signal_types = {str(value).lower() for value in (config.get("signal_types") or [])}
    if signal_types:
        by_signal_type = [
            item
            for item in base_items
            if str(item.get("signal_type") or "").lower() in signal_types
        ]
    else:
        by_signal_type = base_items

    keywords = [str(value).lower() for value in (config.get("keywords") or [])]
    if keywords:
        by_keywords = [
            item
            for item in by_signal_type
            if any(keyword in item_text_blob(item) for keyword in keywords)
        ]
        if by_keywords:
            return by_keywords

    return by_signal_type


def rank_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            -int(item.get("intent_score") or 0),
            str(item.get("published_at") or ""),
        ),
        reverse=False,
    )


def compact_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "category": item.get("category"),
        "signal_type": item.get("signal_type"),
        "title": item.get("title"),
        "url": item.get("url"),
        "source": item.get("source"),
        "source_engine": item.get("source_engine"),
        "summary": item.get("summary"),
        "intent_score": item.get("intent_score"),
        "published_at": item.get("published_at"),
    }


def preview_response(
    feed: dict[str, Any],
    category: str,
    items: list[dict[str, Any]],
    limit: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ranked = rank_items(items)[:limit]
    payload = {
        "date": feed.get("date"),
        "category": category,
        "count": len(ranked),
        "limit": limit,
        "items": [compact_item(item) for item in ranked],
    }
    if extra:
        payload.update(extra)
    return payload


@app.get("/feed")
async def get_feed() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    return feed


@app.get("/leads")
async def get_leads(min_intent: int = Query(default=5, ge=1, le=10)) -> dict[str, Any]:
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


@app.get("/funding")
async def get_funding() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "funding_signal")
    return {
        "date": feed.get("date"),
        "count": len(items),
        "items": items,
    }


@app.get("/hiring")
async def get_hiring() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "hiring_signal")
    return {
        "date": feed.get("date"),
        "count": len(items),
        "items": items,
    }


@app.get("/developer")
async def get_developer() -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "developer_signal")
    return {
        "date": feed.get("date"),
        "count": len(items),
        "items": items,
    }


@app.get("/preview")
async def get_preview(limit_per_category: int = Query(default=3, ge=1, le=20)) -> dict[str, Any]:
    feed, _ = load_latest_feed()

    sections: dict[str, list[dict[str, Any]]] = {}
    for category_id in (
        "lead_signal",
        "company_intel",
        "competitor_news",
        "market_trend",
        "pricing_intel",
        "product_launch_intel",
        "funding_hiring_intel",
        "protocol_signal",
    ):
        items = filter_big_category(feed, category_id)
        if category_id == "lead_signal":
            items = [item for item in items if int(item.get("intent_score") or 0) >= 5]
        sections[category_id] = [compact_item(item) for item in rank_items(items)[:limit_per_category]]

    return {
        "date": feed.get("date"),
        "limit_per_category": limit_per_category,
        "sections": sections,
    }


@app.get("/preview/leads")
async def get_preview_leads(
    limit: int = Query(default=5, ge=1, le=20),
    min_intent: int = Query(default=5, ge=1, le=10),
) -> dict[str, Any]:
    feed, _ = load_latest_feed()
    leads = [
        item
        for item in filter_category(feed, "lead_signal")
        if int(item.get("intent_score") or 0) >= min_intent
    ]
    return preview_response(
        feed=feed,
        category="lead_signal",
        items=leads,
        limit=limit,
        extra={"min_intent": min_intent},
    )


@app.get("/preview/companies")
async def get_preview_companies(
    limit: int = Query(default=5, ge=1, le=20),
    signal_type: str | None = Query(default=None),
) -> dict[str, Any]:
    feed, _ = load_latest_feed()
    companies = filter_category(feed, "company_intel")
    if signal_type:
        target = signal_type.strip().lower()
        companies = [
            item
            for item in companies
            if str(item.get("signal_type") or "").lower() == target
        ]
    return preview_response(
        feed=feed,
        category="company_intel",
        items=companies,
        limit=limit,
        extra={"signal_type": signal_type},
    )


@app.get("/preview/competitors")
async def get_preview_competitors(limit: int = Query(default=5, ge=1, le=20)) -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "competitor_news")
    return preview_response(feed=feed, category="competitor_news", items=items, limit=limit)


@app.get("/preview/market")
async def get_preview_market(limit: int = Query(default=5, ge=1, le=20)) -> dict[str, Any]:
    feed, _ = load_latest_feed()
    items = filter_category(feed, "market_trend")
    return preview_response(feed=feed, category="market_trend", items=items, limit=limit)


@app.get("/preview/catalog")
async def get_preview_catalog() -> dict[str, Any]:
    latest_feed_date = None
    feed: dict[str, Any] | None = None

    try:
        feed, _ = load_latest_feed()
        latest_feed_date = feed.get("date")
    except HTTPException:
        pass

    catalog = []
    for entry in DATA_CATALOG:
        row = dict(entry)
        if feed is None:
            row["latest_count"] = 0
        else:
            row["latest_count"] = len(filter_big_category(feed, str(entry["id"])))
        catalog.append(row)

    return {
        "latest_feed_date": latest_feed_date,
        "categories": catalog,
    }


@app.get("/preview/category/{category_id}")
async def get_preview_category(
    category_id: str,
    limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, Any]:
    if category_id not in BIG_CATEGORY_RULES:
        raise HTTPException(
            status_code=404,
            detail={
                "error": f"Unknown category '{category_id}'",
                "available_categories": sorted(BIG_CATEGORY_RULES.keys()),
            },
        )

    feed, _ = load_latest_feed()
    items = filter_big_category(feed, category_id)
    catalog_row = next((entry for entry in DATA_CATALOG if entry["id"] == category_id), None)

    extra = {
        "category_name": catalog_row["name"] if catalog_row else category_id,
        "status": catalog_row.get("status") if catalog_row else "active",
        "why_agents_buy": catalog_row.get("why_agents_buy") if catalog_row else None,
        "preview_endpoint": f"/preview/category/{category_id}",
    }
    return preview_response(feed=feed, category=category_id, items=items, limit=limit, extra=extra)


@app.post("/cron/scrape")
async def trigger_scrape(secret: str = Query(...)) -> dict[str, Any]:
    if not CRON_SECRET or secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    def run_scraper():
        subprocess.run(["python3", "scraper.py"], cwd=str(ROOT))

    thread = threading.Thread(target=run_scraper, daemon=True)
    thread.start()

    return {"status": "scraper_started", "message": "Scraper running in background"}


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

    preview_endpoints = [
        "/preview",
        "/preview/catalog",
        "/preview/category/{category_id}",
    ]
    for row in DATA_CATALOG:
        endpoint = row.get("preview_endpoint")
        if isinstance(endpoint, str) and endpoint and endpoint not in preview_endpoints:
            preview_endpoints.append(endpoint)

    return {
        "status": "ok",
        "pricing": PRICING,
        "x402": {
            "network": X402_NETWORK,
            "facilitator_url": FACILITATOR_URL,
            "pay_to_address": None if PAY_TO_ADDRESS == ZERO_ADDRESS else PAY_TO_ADDRESS,
        },
        "preview_endpoints": preview_endpoints,
        "data_freshness": {
            "latest_feed_date": latest_feed_date,
            "generated_at": generated_at,
            "latest_feed_path": latest_path,
            "available_feeds": len(list_feed_paths()),
        },
    }
