"""
Signalbase — Daily Intelligence Scraper
Each tool used to its strength:

  X API      → real-time social intent signals ($200/mo Basic tier)
               best for: buying intent, tool evaluation, frustration RIGHT NOW
               uses context: operator for topic filtering
  Brave      → keyword news search (Search tier: extra_snippets + news)
               best for: funding news, product launches, pricing changes, competitor events
  Exa        → semantic/neural search (free tier: 1000 req/mo)
               best for: conceptual queries, emerging trends, protocol news
               queries are statement-style with autoprompt + category filter
  Firecrawl  → search + scrape (free tier: 500 credits/mo)
               dual role: supplemental search for weak categories + thin-content enricher

Daily cost estimate:
  Exa:        6 queries × 5 results = 30 results → free tier
  Brave:      13 queries + news     → Search tier (within limits)
  X API:      6 queries             → Basic tier ($200/mo fixed)
  Firecrawl:  4 search queries + ~10 enrichment scrapes → ~18 credits/day
  OpenRouter: ~70 items × LLM classify  → ~$0.004/day (GPT-4o-mini via OpenRouter)
  TOTAL:      ~$6.67/day ($200/mo X) + free tiers + ~$0.12/mo LLM classifier
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from exa_py import Exa

try:
    from firecrawl import Firecrawl
except ImportError:
    from firecrawl import FirecrawlApp as Firecrawl  # type: ignore

_is_railway = bool(os.getenv("RAILWAY_ENVIRONMENT_NAME", ""))
load_dotenv(override=not _is_railway)  # Never override Railway env vars

ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(ROOT / "data")))

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
# Exa docs: queries should be STATEMENTS, not keywords.
# "Here is a great article about X:" outperforms "X keyword keyword"
# use_autoprompt=True lets Exa rewrite for best results
# category="news" filters to news articles only
# excludeText kills listicles before they enter the pipeline
EXA_QUERIES: dict[str, list[str]] = {
    "market_trend": [
        "Here is a recent article about the x402 protocol enabling AI agent micropayments:",
        "Here is an article about MCP servers and the emerging agent-to-agent data marketplace:",
        "Here is a news article about a new AI infrastructure trend gaining traction in 2026:",
    ],
    "developer_signal": [
        "Here is a news article about a newly released open-weights AI model:",
        "Here is an article about an AI agent framework that just launched on GitHub:",
        "Here is a blog post announcing a new developer tool for building AI agents:",
    ],
}

# Exa listicle patterns to exclude via excludeText parameter
EXA_EXCLUDE_PATTERNS = [
    "top 5", "top 10", "top 20", "best tools", "complete guide",
    "how to get started", "beginner's guide", "comparison of",
]

# BRAVE: keyword news search — best for recent news, announcements, factual events
# Cheaper than Exa for keyword searches, fresher for news
BRAVE_QUERIES: dict[str, list[str]] = {
    "company_intel": [
        '"raised" OR "raises" AI startup funding 2026',
        '"launched" OR "announces" AI agent platform 2026',
        '"hiring" AI engineers company expanding 2026',
        '"new product" OR "new feature" AI infrastructure 2026',
    ],
    "competitor_news": [
        '"outage" OR "downtime" AI API platform 2026',
        '"acquired" OR "acquires" OR "acquisition" AI data company 2026',
        '"price increase" OR "pricing change" AI API SaaS 2026',
        '"launches" OR "launched" competitor AI data API agent 2026',
        '"shutting down" OR "deprecated" OR "end of life" AI API 2026',
    ],
    "funding_signal": [
        '"raises" OR "raised" OR "secures" seed series AI startup 2026',
        '"funding round" OR "investment" AI machine learning announced 2026',
    ],
    "hiring_signal": [
        '"hiring" OR "open roles" AI engineer startup 2026',
        '"head of" OR "VP of" AI engineering new hire 2026',
    ],
    "pricing_intel": [
        '"price increase" OR "pricing change" OR "new pricing" AI API SaaS 2026',
        '"billing update" OR "plan change" OR "free tier" AI tool developer 2026',
    ],
}

# FIRECRAWL SEARCH: web search + scraping in one call
# 2 credits per 10 results, 500 free credits/month = ~8 queries/day budget
# Use sparingly for categories where Brave/Exa underperform
FIRECRAWL_QUERIES: dict[str, list[str]] = {
    "competitor_news": [
        "AI API company outage downtime incident report 2026",
        "AI startup acquired acquisition 2026",
    ],
    "pricing_intel": [
        "AI API pricing change increase 2026",
        "SaaS AI tool new pricing plan announced 2026",
    ],
}

# X (TWITTER): real-time social intent — FREE, freshest signal available
# Best for catching buying intent the moment someone expresses it publicly
X_QUERIES: dict[str, list[str]] = {
    "lead_signal": [
        # Buying intent within Business & Tech context
        # context:131 = Technology domain, context:66 = Business & Finance domain
        '"anyone recommend" OR "can anyone recommend" (tool OR software OR platform OR service) context:131.848920371311001600 -is:retweet lang:en',
        '"looking for a" (tool OR software OR platform OR service OR solution OR vendor) context:131.848920371311001600 -is:retweet lang:en',
        '"switching from" OR "alternative to" OR "replacing" context:131.848920371311001600 -is:retweet lang:en',
        '"anyone recommend" OR "looking for" (tool OR software OR service OR agency) context:66.848921413196984320 -is:retweet lang:en',
        '"need help with" OR "struggling with" (automation OR workflow OR integration OR process) -is:retweet lang:en',
        '"evaluating" OR "comparing" (software OR tool OR platform OR SaaS) -is:retweet lang:en',
    ],
    "market_trend": [
        '"x402" OR "MCP server" OR "agent economy" (launched OR building OR shipped) -is:retweet lang:en',
        '"agentic" OR "autonomous agent" (infrastructure OR data OR API) 2026 -is:retweet lang:en',
    ],
}

# Intent scoring weights — higher = stronger signal
INTENT_KEYWORDS: dict[str, int] = {
    # Buying intent (lead_signal)
    "looking for": 3,
    "anyone recommend": 4,
    "anyone know": 4,
    "can someone recommend": 4,
    "need a": 2,
    "building an agent": 4,
    "building agents": 4,
    "searching for": 3,
    "where to find": 3,
    "data source": 3,
    "real-time data": 3,
    "integrate": 2,
    "pipeline": 2,
    "api for": 3,
    "switching from": 4,
    "alternative to": 4,
    "too expensive": 3,
    "evaluating": 3,
    "data provider": 3,
    # Funding events
    "just raised": 4,
    "raises $": 5,
    "raised $": 5,
    "million": 3,
    "series a": 4,
    "series b": 4,
    "series c": 4,
    "seed round": 4,
    "pre-seed": 3,
    "funding round": 3,
    "led by": 2,
    # Hiring events
    "we're hiring": 3,
    "hiring": 2,
    "open roles": 3,
    "head of engineering": 4,
    "vp of": 3,
    "new hire": 3,
    # Competitor events (high-value signals)
    "acquired": 4,
    "acquires": 4,
    "acquisition": 4,
    "outage": 5,
    "downtime": 4,
    "incident": 3,
    "shutting down": 5,
    "deprecated": 4,
    "end of life": 4,
    "price increase": 4,
    "pricing change": 4,
    "billing change": 3,
    # Product events
    "launched": 3,
    "launches": 3,
    "shipped": 3,
    "announces": 3,
    "now available": 3,
    "new product": 3,
    "new feature": 3,
    "open source": 2,
    "open-source": 2,
}

# Listicle/guide patterns — penalize in scoring (-3)
LISTICLE_PENALTIES: list[str] = [
    "top 5 ", "top 10 ", "top 20 ", "top 50 ",
    "best ", "complete guide", "how to ",
    "comparison)", "compared", "reviews |",
    "leaderboard", "rankings", "tier list",
    "tips for ", "things to know",
]

# Hard garbage filter — items matching these patterns get DROPPED entirely.
# These are content types that look relevant by keyword but carry zero signal.
# Applied to ALL categories before dedup and quality floor.
GARBAGE_PATTERNS: list[str] = [
    # Guides/playbooks/tutorials — about a topic, not an event
    "playbook", "handbook", "beginner's guide", "starter guide",
    "step-by-step", "complete breakdown", "ultimate guide",
    "everything you need to know", "what you need to know",
    "a guide to", "guide for", "introduction to",
    # Aggregation/list content — no actionable signal
    "options compared", "tools compared", "best options",
    "best tools for", "best platforms", "best software",
    "alternatives to consider", "tools to use in",
    "top careers", "top jobs", "high-demand jobs",
    "tools you should", "tools every",
    # Cost/salary guides — not pricing events
    "cost to hire", "salary guide", "compensation report",
    "how much does it cost", "pricing comparison",
    "complete breakdown by",
    # Predictions/trend reports — opinion, not events
    "what lies ahead", "what to expect in",
    "predictions for", "trends to watch",
    "future of", "state of", "outlook for",
    # Courses/certifications
    "free course", "certification", "bootcamp",
    "learn how to", "masterclass",
    # Strategy/framework docs
    "talent strategy", "hiring strategy", "growth strategy",
    "the 2026 guide", "the 2025 guide",
]

# Category-specific garbage patterns — items matching these in their category get dropped
CATEGORY_GARBAGE: dict[str, list[str]] = {
    "funding_signal": [
        # Lists of funded companies, not individual funding events
        "list of funded", "top funded", "most funded",
        "funded startups", "funded companies",
    ],
    "hiring_signal": [
        # Articles about hiring trends, not actual job openings
        "hiring mindset", "hiring playbook", "hiring strategy",
        "talent strategy", "workforce", "shaping the",
        "skills gap", "labor market",
        "guide to", "what employers want", "hiring funnel",
    ],
    "competitor_news": [
        # Listicles of competitors, not competitor events
        "best ai agents", "best ai tools", "best ai platforms",
        "top ai companies",
    ],
    "pricing_intel": [
        # Pricing comparison articles, not actual pricing changes
        "options compared", "pricing comparison", "pricing guide",
    ],
}

COMPANY_SIGNAL_KEYWORDS: dict[str, list[str]] = {
    "funding":      ["funding", "series a", "series b", "seed round", "raised", "investment"],
    "hiring":       ["hiring", "job opening", "headcount", "recruiting", "we're growing"],
    "pricing":      ["pricing", "price increase", "plan update", "billing change", "new pricing"],
    "product_launch": ["launch", "released", "new product", "new feature", "now available"],
}

DEFAULT_SIGNAL_TYPE_BY_CATEGORY: dict[str, str] = {
    "lead_signal": "lead_intent",
    "market_trend": "trend_cluster",
    "developer_signal": "developer_signal",
    "company_intel": "general",
    "competitor_news": "competitor_move",
    "funding_signal": "funding",
    "hiring_signal": "hiring",
}

# ─── Lead Vertical Taxonomy ──────────────────────────────────────────────────
# Tags each lead_signal item with the industry vertical the buyer is in.
# Sales/lead-mgmt agents filter by vertical to find THEIR leads.
# Two-pass tagging: X context_annotations first, keyword fallback second.

# Map X context_annotation entity names → our vertical labels
X_TOPIC_TO_VERTICAL: dict[str, str] = {
    # Technology
    "Technology": "devtools",
    "Technology Business": "devtools",
    "Computer programming": "devtools",
    "Tech Personalities": "devtools",
    "Artificial intelligence": "ai_ml",
    "Machine Learning": "ai_ml",
    "ChatGPT": "ai_ml",
    "OpenAI": "ai_ml",
    "Grok Ai": "ai_ml",
    # Business & Finance
    "Business & finance": "business",
    "Financial Services Business": "fintech",
    "Cryptocurrency": "web3",
    "Bitcoin": "web3",
    "Ethereum": "web3",
    # Marketing & Sales
    "Digital creator": "marketing",
    "Content creation": "marketing",
    "Brand": "marketing",
    "Advertising": "marketing",
    # E-commerce & Retail
    "Retail industry": "ecommerce",
    "E-Commerce": "ecommerce",
    "Apparel/Accessories - Retail": "ecommerce",
    # Design & Creative
    "Design": "design",
    "Graphic design": "design",
    "UX Design": "design",
    # Healthcare
    "Healthcare": "healthcare",
    "Medical": "healthcare",
    # Education
    "Education": "education",
    "EdTech": "education",
    # Real Estate
    "Real Estate": "real_estate",
    "Property": "real_estate",
}

# Keyword fallback: if no X topic matched, scan tweet text for vertical clues
VERTICAL_KEYWORDS: dict[str, list[str]] = {
    "hr": ["hiring", "recruit", "onboarding", "payroll", "hris", "ats", "talent",
           "hr software", "applicant tracking", "people ops", "headcount"],
    "sales": ["crm", "pipeline", "outreach", "cold email", "lead gen", "quota",
              "sales tool", "prospecting", "hubspot", "salesforce", "close.com"],
    "marketing": ["seo", "sem", "ads", "content marketing", "social media", "analytics",
                  "attribution", "email marketing", "mailchimp", "convertkit"],
    "devtools": ["api", "sdk", "github", "deploy", "ci/cd", "monitoring", "backend",
                 "frontend", "developer tool", "dev tool", "hosting", "database"],
    "ai_ml": ["ai agent", "llm", "machine learning", "model", "fine-tune", "embeddings",
              "rag", "vector database", "chatbot", "gpt", "claude", "openai"],
    "automation": ["n8n", "zapier", "make.com", "workflow", "automation", "no-code",
                   "low-code", "airtable", "integromat"],
    "design": ["figma", "design tool", "ui/ux", "prototype", "branding", "logo",
               "canva", "creative", "designer"],
    "ecommerce": ["shopify", "ecommerce", "e-commerce", "store", "inventory",
                  "fulfillment", "cart", "woocommerce", "stripe"],
    "fintech": ["payment", "fintech", "banking", "lending", "invoice", "billing",
                "accounting", "stripe", "plaid", "neobank"],
    "web3": ["blockchain", "smart contract", "web3", "crypto", "defi", "nft",
             "token", "wallet", "solidity", "x402"],
    "healthcare": ["healthcare", "medical", "health tech", "patient", "ehr",
                   "telehealth", "hipaa", "clinical"],
    "education": ["edtech", "learning", "course", "lms", "training",
                  "education", "student", "tutor"],
    "real_estate": ["real estate", "property", "rental", "tenant", "proptech",
                    "listing", "mortgage"],
    "pm_ops": ["project management", "task manager", "jira", "asana", "linear",
               "notion", "monday.com", "ticketing", "helpdesk"],
    "cybersecurity": ["security", "vulnerability", "pentest", "soc", "siem",
                      "compliance", "audit", "encryption", "zero trust"],
}


def classify_lead_vertical(text: str, x_topics: list[str] | None = None) -> str:
    """Tag a lead with its industry vertical. X topics first, keywords second."""
    # Pass 1: X context_annotations (most reliable)
    if x_topics:
        for topic in x_topics:
            vertical = X_TOPIC_TO_VERTICAL.get(topic)
            if vertical:
                return vertical

    # Pass 2: keyword scan on tweet text
    lower = text.lower()
    best_vertical = "general"
    best_hits = 0
    for vertical, keywords in VERTICAL_KEYWORDS.items():
        hits = sum(1 for kw in keywords if kw in lower)
        if hits > best_hits:
            best_hits = hits
            best_vertical = vertical

    return best_vertical if best_hits > 0 else "general"


ENTITY_STOPWORDS = {
    "The", "This", "That", "We", "Our", "You", "Your", "A", "An", "And",
    "For", "From", "With", "Phase", "Agent", "AI", "MCP", "API", "New",
    "Today", "Breaking",
}

# Phrases that indicate the tweet is about tech/business products/services.
# Purpose: filter out tweets about "looking for a restaurant" or "need a ride"
# Must be BROAD — our customers sell everything from HR tools to automation platforms.
LEAD_TECH_PHRASES = [
    # Acronyms (safe, rarely used outside tech/biz)
    "api", "sdk", "saas", "llm", "crm", "erp", "etl", "hris", "ats",
    "cms", "cdn", "seo", "sem", "roi", "kpi", "okr", "mrr", "arr",
    # Software/platform categories
    "software", "platform", "tool", "app", "dashboard", "plugin",
    "integration", "automation", "workflow", "template",
    # Product types that sales agents sell
    "data feed", "data source", "data pipeline", "data provider",
    "dev tool", "developer tool", "open source", "open-source",
    "cloud service", "web service", "microservice",
    "ai agent", "ai model", "ai tool", "ai platform",
    "no-code", "low-code", "workflow automation",
    "chatbot", "voicebot", "voice agent",
    # Business functions (HR, sales, marketing, ops)
    "hr software", "recruiting tool", "payroll", "onboarding",
    "sales tool", "lead gen", "cold email", "outreach",
    "marketing tool", "analytics", "reporting",
    "project management", "task manager", "helpdesk", "ticketing",
    "accounting", "invoicing", "billing",
    # Specific products/ecosystems people ask about
    "n8n", "zapier", "make.com", "airtable", "notion",
    "hubspot", "salesforce", "slack", "stripe", "shopify",
    "github", "npm", "pypi", "docker", "kubernetes",
    "postgres", "supabase", "firebase", "vercel", "netlify",
    # Tech infrastructure
    "tech stack", "backend", "frontend", "full stack",
    "vector database", "embeddings", "rag", "fine-tune",
    "rest api", "graphql", "webhook", "endpoint",
    "hosting", "deployment", "ci/cd", "monitoring",
    # Web3/fintech
    "mcp", "x402", "blockchain", "smart contract", "web3",
    "payment", "fintech", "neobank",
    # Pricing/evaluation signals
    "pricing page", "free tier", "free trial", "self-hosted",
    "subscription", "per seat", "per user", "enterprise plan",
    # General business
    "startup", "b2b", "b2c", "agency", "freelancer", "consultant",
    "vendor", "provider", "service provider",
]

# ─── Utilities ───────────────────────────────────────────────────────────────

def is_garbage(item: dict[str, Any]) -> bool:
    """Hard filter: returns True if item should be dropped entirely.
    Checks universal garbage patterns + category-specific patterns.
    Applied to ALL categories."""
    title = (item.get("title") or "").lower()
    text = (item.get("summary") or item.get("content_excerpt") or "").lower()
    combined = f"{title} {text}"
    category = item.get("category", "")

    # Universal garbage patterns
    if any(pattern in combined for pattern in GARBAGE_PATTERNS):
        return True

    # Category-specific garbage
    cat_patterns = CATEGORY_GARBAGE.get(category, [])
    if any(pattern in combined for pattern in cat_patterns):
        return True

    return False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: str) -> str:
    stripped = re.sub(r"<[^>]+>", "", value)
    stripped = re.sub(r"\[.*?\]<web_link>", "", stripped)
    return " ".join(stripped.split()).strip()


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


def score_intent(
    text: str,
    category: str = "unknown",
    source_engine: str = "unknown",
    engagement_boost: int = 0,
) -> int:
    normalized = text.lower()
    base_by_category = {
        "lead_signal": 2,
        "funding_signal": 2,
        "hiring_signal": 2,
        "developer_signal": 1,
        "market_trend": 1,
        "company_intel": 1,
        "competitor_news": 1,
    }
    score = base_by_category.get(category, 1)
    for keyword, weight in INTENT_KEYWORDS.items():
        if keyword in normalized:
            score += weight
    if source_engine == "x":
        score += 1
    score += max(0, engagement_boost)
    # Lead signals with engagement are higher quality
    if category == "lead_signal" and engagement_boost >= 2:
        score += 1
    # Penalize listicles and guides — they look relevant but aren't actionable
    if any(pattern in normalized for pattern in LISTICLE_PENALTIES):
        score -= 3
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


def parse_relative_age_to_iso(value: str) -> str | None:
    normalized = value.strip().lower()
    now = datetime.now(timezone.utc)
    if normalized in {"now", "just now"}:
        return now.isoformat()
    match = re.search(r"(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago", normalized)
    if not match:
        return None
    qty = int(match.group(1))
    unit = match.group(2)
    if unit == "minute":
        dt = now - timedelta(minutes=qty)
    elif unit == "hour":
        dt = now - timedelta(hours=qty)
    elif unit == "day":
        dt = now - timedelta(days=qty)
    elif unit == "week":
        dt = now - timedelta(weeks=qty)
    elif unit == "month":
        dt = now - timedelta(days=30 * qty)
    else:
        dt = now - timedelta(days=365 * qty)
    return dt.isoformat()


def normalize_published_at(value: str | None) -> str:
    if not value:
        return utc_now_iso()
    as_text = str(value).strip()
    if not as_text:
        return utc_now_iso()
    try:
        return datetime.fromisoformat(as_text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        pass
    relative = parse_relative_age_to_iso(as_text)
    if relative:
        return relative
    return utc_now_iso()


def extract_entity_name(title: str, url: str = "") -> str | None:
    candidates = re.findall(r"\b([A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+)?)\b", title or "")
    for candidate in candidates:
        token = candidate.split()[0]
        if token in ENTITY_STOPWORDS:
            continue
        if len(token) < 3:
            continue
        return candidate
    host = urlparse(url).netloc.lower()
    if host:
        root = host.replace("www.", "").split(".")[0]
        if root:
            return root.title()
    return "Unknown Company"


def classify_signal_type(category: str, text: str, title: str, url: str) -> str:
    normalized = f"{title}\n{text}\n{url}".lower()
    if category == "company_intel":
        return classify_company_signal(normalized)
    if category == "funding_signal":
        return "funding"
    if category == "hiring_signal":
        return "hiring"
    if category == "developer_signal":
        if any(k in normalized for k in ["model", "weights", "benchmark", "gpt", "llm"]):
            return "model_release"
        if any(k in normalized for k in ["framework", "sdk", "library", "langchain", "langgraph"]):
            return "framework_launch"
        if any(k in normalized for k in ["github", "stars", "trending", "repo"]):
            return "repo_velocity"
        if any(k in normalized for k in ["api", "version", "deprecation", "breaking"]):
            return "api_change"
        return "developer_signal"
    if category == "competitor_news":
        if any(k in normalized for k in ["pricing", "price increase", "billing", "plan"]):
            return "pricing_change"
        if any(k in normalized for k in ["breach", "vulnerability", "outage", "incident"]):
            return "security_vulnerability"
        if any(k in normalized for k in ["launch", "released", "ship", "new product"]):
            return "product_launch"
        if any(k in normalized for k in ["acquisition", "acquired", "merger"]):
            return "startup_momentum"
        return "competitor_move"
    if category == "market_trend":
        if any(k in normalized for k in ["mcp", "x402", "protocol", "api standard"]):
            return "api_change"
        if any(k in normalized for k in ["momentum", "surge", "trend", "emerging", "cluster"]):
            return "trend_cluster"
        return "trend_cluster"
    return DEFAULT_SIGNAL_TYPE_BY_CATEGORY.get(category, "general")


def build_signal_item(
    category: str,
    query: str,
    url: str,
    title: str,
    text: str,
    published_at: str | None = None,
    source_engine: str = "unknown",
    engagement_boost: int = 0,
) -> dict[str, Any]:
    combined = normalize_text(f"{title}\n{text}")[:10000]
    normalized_published_at = normalize_published_at(published_at)
    signal_type = classify_signal_type(category=category, text=combined, title=title, url=url)
    intent_score = score_intent(
        combined,
        category=category,
        source_engine=source_engine,
        engagement_boost=engagement_boost,
    )
    entity_name = extract_entity_name(title, url)
    confidence = round(min(1.0, (intent_score + 1) / 10), 2)
    impact_score = round(min(intent_score + engagement_boost, 10) / 10, 2)
    actionability_score = round(min(1.0, (confidence * 0.5) + (impact_score * 0.5)), 2)
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
        "published_at": normalized_published_at,
        "collected_at": utc_now_iso(),
        "intent_score": intent_score,
        "entity": {
            "name": entity_name,
            "type": "company",
        },
        "confidence": confidence,
        "impact_score": impact_score,
        "actionability_score": actionability_score,
        "signal_window": "24h",
        "signal_type": signal_type,
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


# ─── Firecrawl — search + scrape (supplemental) ─────────────────────────────

def search_firecrawl(
    firecrawl_client: Any,
    category: str,
    queries: list[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    """
    Firecrawl v2 /search: web search + inline page scraping in one call.
    With scrape_options, returns Document objects with full markdown content
    and rich metadata (title, published_time, etc.) — far richer than
    Brave snippets or Exa text excerpts.

    Key params:
      - scrape_options: ScrapeOptions(formats=['markdown'], only_main_content=True)
        gives us scraped page content inline (no separate scrape call needed)
      - tbs='qdr:w' restricts to past week (same as Brave's freshness=pw)
      - limit=5 keeps credit usage tight (2 credits per 10 results)

    Budget: ~4 queries/day × 5 results = ~4 credits/day = 120/month (well within 500)
    """
    from firecrawl.v2.types import ScrapeOptions

    items = []

    for query in queries:
        try:
            response = firecrawl_client.search(
                query=query,
                limit=limit,
                tbs="qdr:w",                  # past week freshness
                scrape_options=ScrapeOptions(
                    formats=["markdown"],
                    only_main_content=True,    # skip nav, footers, ads
                    block_ads=True,            # cleaner markdown
                    exclude_tags=["nav", "footer", "aside", "header"],  # strip layout noise
                    remove_base64_images=True,  # lighter response payload
                ),
            )
        except Exception as exc:
            logger.warning("Firecrawl search failed query=%r error=%s", query, exc)
            continue

        # With scrape_options, results come back as Document objects in .web
        results = response.web or [] if hasattr(response, "web") else []

        for result in results:
            # Document objects have .metadata with url/title/published_time
            meta = getattr(result, "metadata", None)
            if meta:
                url = getattr(meta, "url", "") or getattr(meta, "source_url", "") or ""
                title = getattr(meta, "title", "") or getattr(meta, "og_title", "") or ""
                published = getattr(meta, "published_time", None) or getattr(meta, "dc_date", None)
            else:
                url = getattr(result, "url", "")
                title = getattr(result, "title", "") or ""
                published = None

            # Markdown content from scraped page
            text = getattr(result, "markdown", "") or ""
            # Fallback to description if no markdown
            if not text and meta:
                text = getattr(meta, "description", "") or getattr(meta, "og_description", "") or ""

            if not url or not text:
                continue

            # Truncate markdown — we only need enough for scoring and excerpts
            text = text[:2500]

            items.append(build_signal_item(
                category=category,
                query=query,
                url=url,
                title=title,
                text=text,
                published_at=published,
                source_engine="firecrawl",
            ))

        time.sleep(0.3)  # Rate limit respect

    return items


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
    Optimized per Exa docs:
      - Queries are natural-language statements, not keyword soup
      - use_autoprompt=True lets Exa rewrite queries for best retrieval
      - category="news" filters to news articles (skips forums, docs, etc.)
      - excludeText kills listicles before they enter our pipeline
    """
    since = (date.today() - timedelta(days=days_back)).isoformat()
    items = []

    for query in queries:
        try:
            response = exa.search(
                query,
                num_results=num_results,
                start_published_date=since,
                type="auto",               # let Exa pick neural vs keyword
                use_autoprompt=True,        # Exa rewrites query for best results
                category="news",            # news articles only
                exclude_text=EXA_EXCLUDE_PATTERNS,  # kill listicles at source
                contents={
                    "text": {"maxCharacters": 1200},
                    "highlights": {           # query-biased key sentences
                        "query": query,
                        "maxCharacters": 500,
                    },
                    "summary": {              # Exa-generated summary biased to query
                        "query": query,
                    },
                },
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

            # Build richest possible text: summary > highlights > raw text
            exa_summary = str(extract_value(item, "summary") or "").strip()
            exa_highlights = extract_value(item, "highlights") or []
            exa_text = str(extract_value(item, "text", "content") or "").strip()

            # Combine: summary first (most useful), then highlights, then raw
            parts = []
            if exa_summary:
                parts.append(exa_summary)
            if exa_highlights:
                parts.append(" ".join(str(h) for h in exa_highlights))
            if exa_text:
                parts.append(exa_text)
            text = " ".join(parts).strip()

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
    Brave Search API (Search tier) for keyword/news queries.
    Search tier gives extra_snippets (4 per result) — much richer content
    than Base tier's single description. We concatenate all snippets
    for better intent scoring and content excerpts.
    extra_snippets=true requires Search subscription key.
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
                params={
                    "q": query,
                    "count": 10,                       # was 8, max is 20 but 10 is quality sweet spot
                    "freshness": "pw",                 # past week
                    "extra_snippets": "true",          # Search tier: 4 extra snippets per result
                    "result_filter": "web,news,discussions",  # include forums/Reddit threads
                    "text_decorations": "false",       # cleaner text without <b> tags
                },
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
            data = r.json()
            results = data.get("web", {}).get("results", [])
            # Also pull from news + discussion results if available
            news_results = data.get("news", {}).get("results", [])
            discussions = data.get("discussions", {}).get("results", [])
        except Exception as exc:
            logger.warning("Brave JSON parse failed query=%r error=%s", query, exc)
            continue

        if not results and not news_results and not discussions:
            logger.info("Brave query returned 0 results query=%r", query)

        # Process web results with extra_snippets
        for result in results:
            url   = result.get("url", "")
            title = result.get("title", "")
            # Combine description + extra_snippets for richer content
            description = result.get("description", "")
            extra = result.get("extra_snippets", [])
            if extra:
                text = description + " " + " ".join(extra)
            else:
                text = description

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

        # Process news results (often fresher, more event-focused)
        for result in news_results:
            url   = result.get("url", "")
            title = result.get("title", "")
            text  = result.get("description", "")

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

        # Process discussion results (Reddit, HN, forums — real user opinions)
        for result in discussions:
            url   = result.get("url", "")
            title = result.get("title", "")
            text  = result.get("description", "")

            if not url:
                continue

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
    X API v2 for real-time intent signals using bearer token auth.
    Maximized fields:
      - context_annotations: X's own topic/entity tags per tweet
      - expansions=author_id + user.fields: author username, follower count, bio
      - entities: structured URLs, mentions, hashtags from tweet text
    Author follower count feeds into engagement_boost for better scoring.
    """
    items = []
    lead_quality_terms = [
        "need",
        "looking for",
        "recommend",
        "evaluating",
        "comparing",
        "alternative",
        "switching",
        "searching",
        "who sells",
        "where to find",
        "anyone know",
        "suggestions for",
        "help me find",
        "struggling to find",
        "which one",
        "should i use",
    ]

    # Seller/self-promo patterns — these people are SELLING, not BUYING
    # A CEO saying "we launched our API" is not a lead, it's a competitor
    seller_disqualifiers = [
        "we launched",
        "we built",
        "we shipped",
        "we released",
        "our api",
        "our platform",
        "our tool",
        "our sdk",
        "my api",
        "i built",
        "i launched",
        "i shipped",
        "just launched",
        "now live",
        "check out",
        "try our",
        "announcing",
        "introducing our",
        "proud to",
        "excited to announce",
        "we're building",
        "we are building",
        "join our",
        "sign up for",
        "i'd recommend",
        "grab your",
        "get your free",
        "available at",
        "available on",
        "download our",
        "use our",
        # Job posting / hiring announcements (not buying)
        "exciting opportunity",
        "is hiring", "we are hiring", "we're hiring", "hiring for",
        "is looking for a remote",
        "open position", "open role",
        "apply now", "apply here", "apply today",
        "job alert", "job opening",
        # Service sellers ("I build X" / "if you need X")
        "i create", "i develop", "i design", "i build",
        "we create", "we develop", "we design",
        "i can help you", "we can help you",
        "i offer", "we offer", "offering a",
        "if you're looking for", "if you are looking for",
        "if you need a", "if you need an",
        "dm me", "dm for", "book a call", "free consultation",
        "hire me", "hire us",
        "day 1:", "day 2:", "day 3:", "day 4:", "day 5:",  # coaching threads
        # Negations
        "not looking for",
        "i am not looking",
        "i'm not looking",
        "don't need",
        "no longer looking",
    ]

    for query in queries:
        try:
            r = requests.get(
                "https://api.twitter.com/2/tweets/search/recent",
                headers=X_HEADERS,
                params={
                    "query": query,
                    "max_results": 10,
                    "tweet.fields": "text,public_metrics,created_at,author_id,context_annotations,entities",
                    "expansions": "author_id",
                    "user.fields": "username,public_metrics,description,verified",
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
            resp_json = r.json()
            tweets = resp_json.get("data", [])
            # Build author lookup from includes
            users_by_id = {}
            for user in (resp_json.get("includes", {}).get("users", [])):
                users_by_id[user["id"]] = user
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
            likes = int(metrics.get("like_count", 0) or 0)
            retweets = int(metrics.get("retweet_count", 0) or 0)
            replies = int(metrics.get("reply_count", 0) or 0)

            # Author data from expansion
            author = users_by_id.get(tweet.get("author_id", ""), {})
            author_username = author.get("username", "")
            author_followers = int(
                (author.get("public_metrics", {}) or {}).get("followers_count", 0) or 0
            )
            author_bio = author.get("description", "")

            # Engagement boost: factor in author reach + tweet engagement
            raw_engagement = likes + retweets + replies
            follower_boost = 1 if author_followers >= 1000 else 0
            high_reach_boost = 1 if author_followers >= 10000 else 0
            engagement_boost = (
                (1 if raw_engagement > 5 else 0)
                + follower_boost
                + high_reach_boost
            )

            # Enrich text with author context for better scoring
            enriched_text = text
            if author_bio:
                enriched_text = f"{text} [author: @{author_username} — {author_bio}]"

            # Extract context_annotations for signal metadata
            ctx_annotations = tweet.get("context_annotations", [])
            topics = [
                a.get("entity", {}).get("name", "")
                for a in ctx_annotations
                if a.get("entity", {}).get("name")
            ]

            signal = build_signal_item(
                category=category,
                query=query,
                url=tweet_url,
                title=text[:80],
                text=enriched_text,
                published_at=tweet.get("created_at"),
                source_engine="x",
                engagement_boost=engagement_boost,
            )

            # Add X-specific metadata
            if author_username:
                signal["author"] = {
                    "username": author_username,
                    "followers": author_followers,
                }
            if topics:
                signal["x_topics"] = topics[:5]  # keep top 5 topic annotations

            if category == "lead_signal":
                # Normalize curly quotes to straight for consistent matching
                lower_text = text.lower().replace("\u2019", "'").replace("\u2018", "'")
                lower_bio = (author_bio.lower().replace("\u2019", "'").replace("\u2018", "'")
                             if author_bio else "")
                # Filter 1: reject sellers and self-promoters (check tweet + bio)
                if any(phrase in lower_text for phrase in seller_disqualifiers):
                    continue
                # Also check bio for seller signals (freelancers advertising)
                bio_seller_phrases = [
                    "i build", "i create", "i design", "i develop",
                    "we build", "we create", "hire me", "dm for",
                    "book a call", "available for", "open to work",
                ]
                if any(phrase in lower_bio for phrase in bio_seller_phrases) and \
                   any(phrase in lower_text for phrase in ["looking for", "need a", "need an"]):
                    # Bio says seller + tweet uses "looking for" = likely bait
                    continue
                # Filter 2: must be about tech/business (not personal)
                if not any(phrase in lower_text for phrase in LEAD_TECH_PHRASES):
                    continue
                # Filter 3: low-score items need explicit buying language
                has_buying_phrase = any(term in lower_text for term in lead_quality_terms)
                if int(signal.get("intent_score", 0) or 0) < 4 and not has_buying_phrase:
                    continue
                # Filter 4: bot/spam detection
                if author_followers < 5 and len(text) < 80:
                    continue
                # Filter 4b: gibberish detector — high ratio of nonsense words
                words = lower_text.split()
                if len(words) > 5:
                    real_words = sum(1 for w in words if len(w) > 1 and w.isalpha())
                    if real_words / len(words) < 0.5:
                        continue  # too much gibberish
                # Tag with industry vertical
                signal["vertical"] = classify_lead_vertical(text, topics)

            items.append(signal)

        time.sleep(0.5)  # X rate limit respect

    return items


# ─── LLM Classifier (OpenRouter) ──────────────────────────────────────────────

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
LLM_CLASSIFIER_MODEL = os.getenv("LLM_CLASSIFIER_MODEL", "openai/gpt-4o-mini")
LLM_CLASSIFY_CATEGORIES = {"lead_signal", "hiring_signal"}

LLM_PROMPTS: dict[str, str] = {
    "lead_signal": (
        "You are a buying-intent classifier for a B2B sales intelligence platform.\n"
        "Determine if this social media post is from a BUSINESS or PROFESSIONAL "
        "actively looking to buy a B2B product, service, tool, or solution.\n\n"
        "ACCEPT: business professionals asking for software/tool recommendations, "
        "evaluating B2B vendors, switching business tools, expressing frustration "
        "with a business tool, comparing enterprise/SaaS options, requesting proposals "
        "for business services.\n\n"
        "REJECT:\n"
        "- Consumer/personal queries: publishing novels, finding dating apps, "
        "personal hobbies, creative writing platforms, music production, art tools\n"
        "- Adult content: OnlyFans, findom, tributes, NSFW platforms, adult services\n"
        "- Someone selling/advertising their own product or service\n"
        "- Job postings, coaching/tips threads, political commentary\n"
        "- Customer service complaints, spam, gibberish\n"
        "- General opinions not related to B2B purchasing\n"
        "- Recommendations/advice FROM others (not the person seeking to buy)\n\n"
        "The post must show BUYING INTENT from the author in a BUSINESS context.\n\n"
        "Reply with ONLY a JSON object: {\"accept\": true/false, \"reason\": \"<10 words>\"}"
    ),
    "hiring_signal": (
        "You are a hiring-event classifier for a business intelligence platform.\n"
        "Determine if this content describes a SPECIFIC, REAL hiring event — "
        "a company actively hiring or a concrete job opening.\n\n"
        "ACCEPT: specific company hiring announcements, job postings with company names, "
        "new VP/CTO/head-of appointments, team expansion news.\n\n"
        "REJECT: career advice articles, hiring trend think-pieces, opinion columns about "
        "the job market, generic 'future of work' content, salary guides, "
        "interview tips, skills gap discussions.\n\n"
        "Reply with ONLY a JSON object: {\"accept\": true/false, \"reason\": \"<10 words>\"}"
    ),
}


def llm_classify_batch(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Second-pass LLM filter on lead_signal and hiring_signal items.
    Sends each candidate to OpenRouter for binary accept/reject.
    Items from other categories pass through unchanged.
    Returns only accepted items (with llm_verdict metadata attached)."""

    if not OPENROUTER_API_KEY:
        logger.warning("OPENROUTER_API_KEY not set — skipping LLM classifier")
        return items

    accepted: list[dict[str, Any]] = []
    llm_rejected = 0
    llm_errors = 0

    for item in items:
        category = item.get("category", "")

        # Pass through categories that don't need LLM classification
        if category not in LLM_CLASSIFY_CATEGORIES:
            accepted.append(item)
            continue

        # Build the content string for classification
        title = item.get("title", "")
        excerpt = item.get("content_excerpt") or item.get("summary") or ""
        author_info = ""
        author = item.get("author", {})
        if author:
            author_info = f" [posted by @{author.get('username', '?')}]"
        content = f"{title} {excerpt}{author_info}".strip()[:500]

        system_prompt = LLM_PROMPTS.get(category, "")
        if not system_prompt:
            accepted.append(item)
            continue

        reply = None
        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": LLM_CLASSIFIER_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": content},
                    ],
                    "max_tokens": 60,
                    "temperature": 0,
                },
                timeout=15,
            )

            if resp.status_code != 200:
                logger.warning("LLM classifier error status=%s body=%s", resp.status_code, resp.text[:200])
                llm_errors += 1
                accepted.append(item)  # on error, keep the item (fail open)
                continue

            reply = resp.json()["choices"][0]["message"]["content"].strip()

            # Parse JSON from reply (handle markdown code fences)
            clean = reply.strip("`").strip()
            if clean.startswith("json"):
                clean = clean[4:].strip()
            verdict = json.loads(clean)

            if verdict.get("accept", True):
                item["llm_verdict"] = {"accepted": True, "reason": verdict.get("reason", "")}
                accepted.append(item)
            else:
                llm_rejected += 1
                logger.debug(
                    "LLM rejected [%s] %s — %s",
                    category, title[:60], verdict.get("reason", "no reason"),
                )

            time.sleep(0.1)  # rate limit politeness

        except (json.JSONDecodeError, KeyError, IndexError) as exc:
            logger.warning("LLM classifier parse error: %s reply=%s", exc, reply[:200] if reply else "N/A")
            llm_errors += 1
            accepted.append(item)  # fail open
        except Exception as exc:
            logger.warning("LLM classifier request error: %s", exc)
            llm_errors += 1
            accepted.append(item)  # fail open

    if llm_rejected:
        logger.info("LLM classifier rejected %d items from %s", llm_rejected, LLM_CLASSIFY_CATEGORIES)
    if llm_errors:
        logger.warning("LLM classifier had %d errors (items kept on fail-open)", llm_errors)

    return accepted


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

    # Exa: semantic intent queries (statement-style, autoprompt, category filter)
    for category, queries in EXA_QUERIES.items():
        all_items.extend(search_exa(exa, firecrawl, category, queries, num_results, days_back))

    # Brave: keyword news queries (Search tier with extra_snippets)
    for category, queries in BRAVE_QUERIES.items():
        all_items.extend(search_brave(brave_key, firecrawl, category, queries))

    # Firecrawl: supplemental search + scrape (richer content for weak categories)
    for category, queries in FIRECRAWL_QUERIES.items():
        all_items.extend(search_firecrawl(firecrawl, category, queries))

    # X: real-time social intent
    for category, queries in X_QUERIES.items():
        all_items.extend(search_x(category, queries))

    # Garbage filter → Deduplicate → Quality floor → Author dedup (X)
    garbage_dropped = 0
    score_dropped = 0
    deduped: list[dict[str, Any]] = []
    x_authors_per_category: dict[str, set[str]] = {}  # limit 1 tweet per author per category

    for item in all_items:
        if item["id"] in seen:
            continue
        seen.add(item["id"])

        # Hard garbage filter (guides, listicles, playbooks, etc.)
        if is_garbage(item):
            garbage_dropped += 1
            continue

        # Quality floor (score < 2)
        if int(item.get("intent_score") or 0) < 2:
            score_dropped += 1
            continue

        # Author dedup for X items: max 1 tweet per author per category
        if item.get("source_engine") == "x":
            author_username = (item.get("author") or {}).get("username", "")
            if author_username:
                cat = item.get("category", "")
                key = cat
                if key not in x_authors_per_category:
                    x_authors_per_category[key] = set()
                if author_username in x_authors_per_category[key]:
                    continue  # already have a tweet from this author in this category
                x_authors_per_category[key].add(author_username)

        deduped.append(item)

    if garbage_dropped:
        logger.info("Garbage filter dropped %d items (guides/listicles/playbooks)", garbage_dropped)
    if score_dropped:
        logger.info("Quality floor dropped %d items with score < 2", score_dropped)

    # LLM second-pass: classify lead_signal + hiring_signal candidates
    classified = llm_classify_batch(deduped)

    # Sort: lead signals first, then by intent score desc
    classified.sort(key=lambda r: (
        0 if r["category"] == "lead_signal" else 1,
        -int(r.get("intent_score") or 0),
        r.get("published_at") or "",
    ))

    return classified


def build_feed_payload(run_date: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    all_categories = list(set(
        list(EXA_QUERIES.keys()) + list(BRAVE_QUERIES.keys()) +
        list(FIRECRAWL_QUERIES.keys()) + list(X_QUERIES.keys())
    ))
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
    out_path = DATA_DIR / run_date / "feed.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, out_path)
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
