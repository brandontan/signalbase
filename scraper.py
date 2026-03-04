"""
Signalbase — Daily Intelligence Scraper
Each tool used to its strength:

  X API      → real-time social intent signals (FREE)
               best for: people complaining, asking for alternatives RIGHT NOW
  Brave      → keyword news search (FREE tier: 2000/month, then $0.003/query)
               best for: funding news, product launches, pricing changes
  Exa        → semantic/neural search ($0.003/search + $0.001/result)
               best for: intent-based conceptual queries, Reddit/HN threads
  Firecrawl  → URL content extraction ($0.0053/page) — FALLBACK ONLY
               fires only when Exa/Brave return thin content (<200 chars)

Daily cost estimate:
  Exa:        4 queries × 5 results = 20 results → ~$0.032/day
  Brave:      8 queries             → FREE (under 2000/month limit)
  X API:      4 queries             → FREE
  Firecrawl:  ~10 fallback scrapes  → ~$0.053/day
  TOTAL:      ~$0.085/day = $2.55/month
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlencode

import requests
from dotenv import load_dotenv
from exa_py import Exa

try:
    from firecrawl import Firecrawl
except ImportError:
    from firecrawl import FirecrawlApp as Firecrawl  # type: ignore

load_dotenv()

X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")
X_HEADERS = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}

LOG_LEVEL = os.getenv("SCRAPER_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(levelname)s %(message)s",
)
logger = logging.getLogger("signalbase.scraper")


# ─── Search Engine Routing ───────────────────────────────────────────────────
# Which tool handles which category and why

# EXA: semantic/neural — finds content by MEANING not keywords
# Best for intent queries where concept matters more than exact words
EXA_QUERIES: dict[str, list[str]] = {
    "lead_signal": [
        "teams building AI agents need real-time data feed for autonomous workflows",
        "developer integrating external data source into LLM agent pipeline",
    ],
    "market_trend": [
        "x402 protocol HTTP micropayments AI agent economy 2026",
        "MCP server marketplace agent-to-agent data transactions emerging",
    ],
}

# BRAVE: keyword news search — best for recent news, announcements, factual events
# Cheaper than Exa for keyword searches, fresher for news
BRAVE_QUERIES: dict[str, list[str]] = {
    "company_intel": [
        "AI agent startup funding round raised 2026",
        "autonomous agent platform launch developer tools 2026",
        "agentic AI company hiring engineers 2026",
        "AI infrastructure startup new product release 2026",
    ],
    "competitor_news": [
        "AI data API pricing change developer reaction 2026",
        "agent tooling platform acquisition merger 2026",
        "AI API provider outage migration alternative 2026",
        "LLM data provider new competitor launch 2026",
    ],
}

# X (TWITTER): real-time social intent — FREE, freshest signal available
# Best for catching buying intent the moment someone expresses it publicly
X_QUERIES: dict[str, list[str]] = {
    "lead_signal": [
        '"building an agent" OR "building agents" ("need data" OR "data source" OR "real-time") -is:retweet lang:en',
        '"AI agent" ("looking for" OR "anyone know" OR "recommend") data API -is:retweet lang:en',
    ],
    "market_trend": [
        '"x402" OR "MCP server" OR "agent economy" (launched OR building OR shipped) -is:retweet lang:en',
        '"agentic" OR "autonomous agent" (infrastructure OR data OR API) 2026 -is:retweet lang:en',
    ],
}

# Intent scoring weights — higher = stronger buying signal
INTENT_KEYWORDS: dict[str, int] = {
    "looking for": 3,
    "anyone recommend": 4,
    "need a": 2,
    "building an agent": 4,
    "building agents": 4,
    "data source": 3,
    "real-time data": 3,
    "integrate": 2,
    "pipeline": 2,
    "feed": 2,
    "api for": 3,
    "switching from": 4,
    "alternative to": 4,
    "too expensive": 3,
    "evaluating": 3,
    "we're hiring": 2,
    "just raised": 4,
    "series a": 4,
    "series b": 4,
    "seed round": 4,
    "launched": 2,
    "shipped": 2,
    "new product": 2,
    "acquired": 3,
    "pricing change": 3,
}

COMPANY_SIGNAL_KEYWORDS: dict[str, list[str]] = {
    "funding":      ["funding", "series a", "series b", "seed round", "raised", "investment"],
    "hiring":       ["hiring", "job opening", "headcount", "recruiting", "we're growing"],
    "pricing":      ["pricing", "price increase", "plan update", "billing change", "new pricing"],
    "product_launch": ["launch", "released", "new product", "new feature", "now available"],
}


# ─── Utilities ───────────────────────────────────────────────────────────────

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: str) -> str:
    return " ".join(value.split()).strip()


def extract_value(obj: Any, *keys: str) -> Any:
    for key in keys:
        if isinstance(obj, dict) and key in obj and obj[key] is not None:
            return obj[key]
        if hasattr(obj, key):
            value = getattr(obj, key)
            if value is not None:
                return value
    return None


def compute_md5_id(url: str, title: str, content: str) -> str:
    raw = f"{url}|{title}|{content[:2000]}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def score_intent(text: str) -> int:
    normalized = text.lower()
    score = 3  # not 1
    for keyword, weight in INTENT_KEYWORDS.items():
        if keyword in normalized:
            score += weight
    return max(1, min(score, 10))


def classify_company_signal(text: str) -> str:
    normalized = text.lower()
    for signal_type, keywords in COMPANY_SIGNAL_KEYWORDS.items():
        if any(kw in normalized for kw in keywords):
            return signal_type
    return "general"


def response_snippet(resp: requests.Response, limit: int = 220) -> str:
    body = (resp.text or "").replace("\n", " ").strip()
    return body[:limit]


def build_signal_item(
    category: str,
    query: str,
    url: str,
    title: str,
    text: str,
    published_at: str | None = None,
    source_engine: str = "unknown",
) -> dict[str, Any]:
    combined = normalize_text(f"{title}\n{text}")[:10000]
    return {
        "id": compute_md5_id(url, title, combined),
        "category": category,
        "query": query,
        "title": title or "(untitled)",
        "url": url,
        "source": urlparse(url).netloc,
        "source_engine": source_engine,
        "summary": combined[:320],
        "content_excerpt": combined[:2500],
        "published_at": published_at,
        "collected_at": utc_now_iso(),
        "intent_score": score_intent(combined),
        "signal_type": classify_company_signal(combined) if category == "company_intel" else None,
    }


# ─── Firecrawl — URL extraction fallback ONLY ────────────────────────────────

def firecrawl_extract(client: Any, url: str) -> str:
    """Only called when Exa/Brave return thin content. Extracts clean markdown from URL."""
    attempts = [
        lambda: client.scrape(url=url, formats=["markdown"], maxAge=0),
        lambda: client.scrape(url, formats=["markdown"]),
        lambda: client.scrape_url(url=url, params={"formats": ["markdown"]}),
    ]
    for fn in attempts:
        try:
            result = fn()
            if isinstance(result, dict):
                md = result.get("markdown") or (result.get("data") or {}).get("markdown") or ""
                return str(md).strip()
            return str(extract_value(result, "markdown", "content", "text") or "").strip()
        except Exception:
            continue
    return ""


def enrich_if_thin(firecrawl: Any, text: str, url: str, threshold: int = 200) -> str:
    """Use Firecrawl only when content is too thin to be useful."""
    if len(text) >= threshold:
        return text
    enriched = firecrawl_extract(firecrawl, url)
    return enriched if enriched else text


# ─── Exa — semantic/neural search ────────────────────────────────────────────

def search_exa(
    exa: Exa,
    firecrawl: Any,
    category: str,
    queries: list[str],
    num_results: int = 5,
    days_back: int = 1,
) -> list[dict[str, Any]]:
    """
    Exa shines for semantic intent queries.
    num_results=5 (not 8) — saves $0.003 per result, same quality.
    contents=text fetched inline to avoid separate API calls.
    """
    since = (date.today() - timedelta(days=days_back)).isoformat()
    items = []

    for query in queries:
        try:
            response = exa.search(
                query,
                num_results=num_results,
                start_published_date=since,
                type="neural",          # semantic matching — Exa's real strength
                contents={"text": {"maxCharacters": 800}},
            )
        except Exception:
            try:
                response = exa.search_and_contents(
                    query, num_results=num_results,
                    start_published_date=since, text=True,
                )
            except Exception as exc:
                logger.warning("Exa query failed query=%r error=%s", query, exc)
                continue

        raw = extract_value(response, "results") or []
        for item in raw:
            url   = str(extract_value(item, "url") or "").strip()
            title = str(extract_value(item, "title") or "").strip()
            text  = str(
                extract_value(item, "text", "content", "summary") or ""
            ).strip()

            if not url:
                continue

            text = enrich_if_thin(firecrawl, text, url)
            if not text:
                continue

            items.append(build_signal_item(
                category=category,
                query=query,
                url=url,
                title=title,
                text=text,
                published_at=extract_value(item, "published_date", "publishedDate"),
                source_engine="exa",
            ))

    return items


# ─── Brave — keyword news search ─────────────────────────────────────────────

def search_brave(
    brave_key: str,
    firecrawl: Any,
    category: str,
    queries: list[str],
) -> list[dict[str, Any]]:
    """
    Brave Search API for keyword/news queries.
    FREE up to 2000 queries/month — use for all news-style searches.
    Fresher than Exa for recent announcements and press releases.
    """
    items = []

    for query in queries:
        try:
            r = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": brave_key,
                },
                params={"q": query, "count": 8, "freshness": "pw"},  # pw = past week
                timeout=15,
            )
        except Exception as exc:
            logger.warning("Brave request error query=%r error=%s", query, exc)
            continue

        if r.status_code != 200:
            logger.warning(
                "Brave query failed status=%s query=%r body=%s",
                r.status_code,
                query,
                response_snippet(r),
            )
            continue

        try:
            results = r.json().get("web", {}).get("results", [])
        except Exception as exc:
            logger.warning("Brave JSON parse failed query=%r error=%s", query, exc)
            continue

        if not results:
            logger.info("Brave query returned 0 results query=%r", query)

        for result in results:
            url   = result.get("url", "")
            title = result.get("title", "")
            text  = result.get("description", "") or result.get("extra_snippets", [""])[0]

            if not url:
                continue

            text = enrich_if_thin(firecrawl, text, url, threshold=150)
            if not text:
                continue

            items.append(build_signal_item(
                category=category,
                query=query,
                url=url,
                title=title,
                text=text,
                published_at=result.get("age"),
                source_engine="brave",
            ))

        time.sleep(0.2)  # Brave rate limit respect

    return items


# ─── X API — real-time social intent ─────────────────────────────────────────

def search_x(
    category: str,
    queries: list[str],
) -> list[dict[str, Any]]:
    """
    X API for real-time intent signals using bearer token auth.
    Best source for catching the moment someone publicly expresses
    buying intent, frustration, or tool evaluation.
    No Firecrawl needed — tweet text IS the signal.
    """
    items = []

    for query in queries:
        try:
            r = requests.get(
                "https://api.twitter.com/2/tweets/search/recent",
                headers=X_HEADERS,
                params={
                    "query": query,
                    "max_results": 10,
                    "tweet.fields": "text,public_metrics,created_at,author_id",
                },
                timeout=15,
            )
        except Exception as exc:
            logger.warning("X request error query=%r error=%s", query, exc)
            continue

        if r.status_code != 200:
            logger.warning(
                "X query failed status=%s query=%r body=%s",
                r.status_code,
                query,
                response_snippet(r),
            )
            continue

        try:
            tweets = r.json().get("data", [])
        except Exception as exc:
            logger.warning("X JSON parse failed query=%r error=%s", query, exc)
            continue

        if not tweets:
            logger.info("X query returned 0 results query=%r", query)

        for tweet in tweets:
            text = tweet.get("text", "")
            if not text or len(text) < 30:
                continue

            # Skip retweets (already filtered in query but double-check)
            if text.startswith("RT @"):
                continue

            metrics = tweet.get("public_metrics", {})
            tweet_url = f"https://x.com/i/web/status/{tweet['id']}"

            signal = build_signal_item(
                category=category,
                query=query,
                url=tweet_url,
                title=text[:80],
                text=text,
                published_at=tweet.get("created_at"),
                source_engine="x",
            )
            # Boost intent score for tweets with engagement
            likes = metrics.get("like_count", 0)
            retweets = metrics.get("retweet_count", 0)
            if likes + retweets > 5:
                signal["intent_score"] = min(10, signal["intent_score"] + 1)

            items.append(signal)

        time.sleep(0.5)  # X rate limit respect

    return items


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def collect_signals(
    exa: Exa,
    firecrawl: Any,
    brave_key: str,
    num_results: int,
    days_back: int,
) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Exa: semantic intent queries
    for category, queries in EXA_QUERIES.items():
        all_items.extend(search_exa(exa, firecrawl, category, queries, num_results, days_back))

    # Brave: keyword news queries
    for category, queries in BRAVE_QUERIES.items():
        all_items.extend(search_brave(brave_key, firecrawl, category, queries))

    # X: real-time social intent
    for category, queries in X_QUERIES.items():
        all_items.extend(search_x(category, queries))

    # Deduplicate
    deduped: list[dict[str, Any]] = []
    for item in all_items:
        if item["id"] not in seen:
            seen.add(item["id"])
            deduped.append(item)

    # Sort: lead signals first, then by intent score desc
    deduped.sort(key=lambda r: (
        0 if r["category"] == "lead_signal" else 1,
        -int(r.get("intent_score") or 0),
        r.get("published_at") or "",
    ))

    return deduped


def build_feed_payload(run_date: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    all_categories = list(EXA_QUERIES.keys()) + list(BRAVE_QUERIES.keys())
    counts = {cat: 0 for cat in all_categories}
    engine_counts: dict[str, int] = {}

    for item in items:
        counts[item["category"]] = counts.get(item["category"], 0) + 1
        engine = item.get("source_engine", "unknown")
        engine_counts[engine] = engine_counts.get(engine, 0) + 1

    return {
        "date": run_date,
        "generated_at": utc_now_iso(),
        "counts": {
            "total": len(items),
            "by_category": counts,
            "by_engine": engine_counts,
        },
        "items": items,
    }


def save_feed(run_date: str, payload: dict[str, Any]) -> Path:
    root = Path(__file__).resolve().parent
    out_path = root / "data" / run_date / "feed.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    return out_path


def build_clients() -> tuple[Exa, Any, str]:
    exa_key       = os.getenv("EXA_API_KEY", "").strip()
    firecrawl_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    brave_key     = os.getenv("BRAVE_API_KEY", "").strip()

    if not exa_key:       raise SystemExit("EXA_API_KEY is required.")
    if not firecrawl_key: raise SystemExit("FIRECRAWL_API_KEY is required.")
    if not brave_key:     raise SystemExit("BRAVE_API_KEY is required.")
    if not X_BEARER_TOKEN:
        raise SystemExit("X_BEARER_TOKEN is required.")

    exa       = Exa(api_key=exa_key)
    firecrawl = Firecrawl(api_key=firecrawl_key)

    return exa, firecrawl, brave_key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Signalbase daily scrape pipeline.")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--num-results", type=int, default=5,
                        help="Exa results per query (default 5 — cost optimized).")
    parser.add_argument("--days-back", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    exa, firecrawl, brave_key = build_clients()

    items = collect_signals(
        exa=exa,
        firecrawl=firecrawl,
        brave_key=brave_key,
        num_results=args.num_results,
        days_back=args.days_back,
    )

    payload  = build_feed_payload(run_date=args.date, items=items)
    out_path = save_feed(run_date=args.date, payload=payload)

    counts = payload["counts"]
    print(f"Signalbase scrape complete: {counts['total']} items")
    print(f"  By category: {counts['by_category']}")
    print(f"  By engine:   {counts['by_engine']}")
    print(f"  Saved to:    {out_path}")


if __name__ == "__main__":
    main()
