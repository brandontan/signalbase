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
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlencode

import requests
from dotenv import load_dotenv
from exa_py import Exa
from requests_oauthlib import OAuth1

try:
    from firecrawl import Firecrawl
except ImportError:
    from firecrawl import FirecrawlApp as Firecrawl  # type: ignore


# ─── Search Engine Routing ───────────────────────────────────────────────────
# Which tool handles which category and why

# EXA: semantic/neural — finds content by MEANING not keywords
# Best for intent queries where concept matters more than exact words
EXA_QUERIES: dict[str, list[str]] = {
    "lead_signal": [
        "people evaluating switching away from their current B2B software tool",
        "founders frustrated want to replace expensive SaaS subscription looking for alternative",
    ],
    "market_trend": [
        "AI agent infrastructure MCP x402 autonomous agent economy emerging 2026",
        "agent-native tools developer adoption data access API marketplace",
    ],
}

# BRAVE: keyword news search — best for recent news, announcements, factual events
# Cheaper than Exa for keyword searches, fresher for news
BRAVE_QUERIES: dict[str, list[str]] = {
    "company_intel": [
        "AI startup raises series funding round 2026",
        "SaaS company hiring growth engineering 2026",
        "software company pricing change restructure announcement",
        "new AI product launch developer tools 2026",
    ],
    "competitor_news": [
        "SaaS company acquired merger AI tooling 2026",
        "software platform pricing increase customers reaction",
        "AI infrastructure tool launch competitor announcement",
        "SaaS outage users migrating alternative 2026",
    ],
}

# X (TWITTER): real-time social intent — FREE, freshest signal available
# Best for catching buying intent the moment someone expresses it publicly
X_QUERIES: dict[str, list[str]] = {
    "lead_signal": [
        '"looking for" OR "anyone recommend" ("AI tool" OR "SaaS" OR "data API") -is:retweet lang:en',
        '"switching from" OR "migrating from" OR "replacing" ("HubSpot" OR "Salesforce" OR "SaaS") -is:retweet lang:en',
    ],
    "market_trend": [
        '"MCP server" OR "x402" OR "agent economy" startup building launched -is:retweet lang:en',
        '"AI agents" ("buy" OR "pay for" OR "data access") infrastructure -is:retweet lang:en',
    ],
}

# Intent scoring weights — higher = stronger buying signal
INTENT_KEYWORDS: dict[str, int] = {
    "looking for": 3,
    "switching from": 4,
    "migrating": 3,
    "replacing": 3,
    "alternative to": 4,
    "recommendation": 2,
    "frustrated": 2,
    "need": 1,
    "evaluate": 2,
    "rfp": 4,
    "budget": 2,
    "buy": 2,
    "purchase": 2,
    "vendor": 2,
    "too expensive": 3,
    "cancel": 3,
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
    score = 1
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
            except Exception:
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
                params={"q": query, "count": 5, "freshness": "pd"},  # pd = past day
                timeout=15,
            )
            r.raise_for_status()
        except Exception:
            continue

        for result in r.json().get("web", {}).get("results", []):
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
    auth: OAuth1,
    category: str,
    queries: list[str],
) -> list[dict[str, Any]]:
    """
    X API for real-time intent signals — completely FREE with OAuth1.
    Best source for catching the moment someone publicly expresses
    buying intent, frustration, or tool evaluation.
    No Firecrawl needed — tweet text IS the signal.
    """
    items = []

    for query in queries:
        try:
            r = requests.get(
                "https://api.twitter.com/2/tweets/search/recent",
                auth=auth,
                params={
                    "query": query,
                    "max_results": 10,
                    "tweet.fields": "text,public_metrics,created_at,author_id",
                },
                timeout=15,
            )
            r.raise_for_status()
        except Exception:
            continue

        for tweet in r.json().get("data", []):
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
    x_auth: OAuth1,
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
        all_items.extend(search_x(x_auth, category, queries))

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


def build_clients() -> tuple[Exa, Any, str, OAuth1]:
    exa_key       = os.getenv("EXA_API_KEY", "").strip()
    firecrawl_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    brave_key     = os.getenv("BRAVE_API_KEY", "").strip()
    x_consumer_key        = os.getenv("X_CONSUMER_KEY", "").strip()
    x_consumer_secret     = os.getenv("X_CONSUMER_SECRET", "").strip()
    x_access_token        = os.getenv("X_ACCESS_TOKEN", "").strip()
    x_access_token_secret = os.getenv("X_ACCESS_TOKEN_SECRET", "").strip()

    if not exa_key:       raise SystemExit("EXA_API_KEY is required.")
    if not firecrawl_key: raise SystemExit("FIRECRAWL_API_KEY is required.")
    if not brave_key:     raise SystemExit("BRAVE_API_KEY is required.")
    if not all([x_consumer_key, x_consumer_secret, x_access_token, x_access_token_secret]):
        raise SystemExit("X API credentials are required (X_CONSUMER_KEY, X_CONSUMER_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET).")

    exa       = Exa(api_key=exa_key)
    firecrawl = Firecrawl(api_key=firecrawl_key)
    x_auth    = OAuth1(x_consumer_key, x_consumer_secret, x_access_token, x_access_token_secret)

    return exa, firecrawl, brave_key, x_auth


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Signalbase daily scrape pipeline.")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--num-results", type=int, default=5,
                        help="Exa results per query (default 5 — cost optimized).")
    parser.add_argument("--days-back", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    exa, firecrawl, brave_key, x_auth = build_clients()

    items = collect_signals(
        exa=exa,
        firecrawl=firecrawl,
        brave_key=brave_key,
        x_auth=x_auth,
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
