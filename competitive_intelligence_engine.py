#!/usr/bin/env python3

from __future__ import annotations
"""
PRINTMAXX Competitive Intelligence Engine
==========================================
Goes way deeper than competitor_monitor.py. Tracks apps, services, content,
and runs gap analysis across the full PrintMaxx portfolio.

Data sources (all free, no auth):
  - iTunes Search API: app rankings, pricing, reviews, version history
  - Gumroad discover pages: product pricing, sales estimates
  - Twitter/X public profiles: engagement rates, posting frequency
  - Substack public pages: newsletter subscriber estimates
  - Fiverr/Upwork public listings: freelancer rates, gig pricing
  - Agency websites: pricing page scraping

Outputs:
  LEDGER/COMPETITIVE_INTEL.csv       - structured competitive data
  OPS/COMPETITIVE_INTEL_REPORT_*.md  - human-readable report

Usage:
    python3 competitive_intelligence_engine.py --scan-all
    python3 competitive_intelligence_engine.py --apps
    python3 competitive_intelligence_engine.py --services
    python3 competitive_intelligence_engine.py --content
    python3 competitive_intelligence_engine.py --gaps
    python3 competitive_intelligence_engine.py --report

Cron (weekly, Sunday 6AM):
  0 6 * * 0 cd /Users/macbookpro/Documents/p/PRINTMAXX_STARTER_KITttttt && python3 AUTOMATIONS/competitive_intelligence_engine.py --scan-all >> AUTOMATIONS/logs/competitive_intel.log 2>&1
"""

import argparse
import csv
import json
import os
import re
import ssl
import sys
import time
import random
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import quote, urlencode

try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CONTEXT = ssl.create_default_context()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEDGER_DIR = PROJECT_ROOT / "LEDGER"
OPS_DIR = PROJECT_ROOT / "OPS"
LOGS_DIR = PROJECT_ROOT / "AUTOMATIONS" / "logs"
CACHE_DIR = PROJECT_ROOT / "AUTOMATIONS" / "logs" / "intel_cache"
INTEL_CSV = LEDGER_DIR / "COMPETITIVE_INTEL.csv"
HISTORY_FILE = CACHE_DIR / "competitive_intel_history.json"
LOG_FILE = LOGS_DIR / "competitive_intel.log"

for d in [LEDGER_DIR, OPS_DIR, LOGS_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Path safety
def safe_path(target):
    resolved = Path(target).resolve()
    if not str(resolved).startswith(str(PROJECT_ROOT)):
        raise ValueError(f"BLOCKED: {resolved} is outside project root")
    return resolved

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
_last_request_time = 0.0

def rate_limit(min_delay=1.5):
    """Enforce minimum delay between HTTP requests to avoid bans."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < min_delay:
        jitter = random.uniform(0.1, 0.5)
        time.sleep(min_delay - elapsed + jitter)
    _last_request_time = time.time()

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

def fetch_url(url, headers=None, timeout=20, min_delay=1.5):
    """Fetch URL with rate limiting, retries, and error handling."""
    rate_limit(min_delay)
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if headers:
        default_headers.update(headers)
    req = Request(url, headers=default_headers)
    for attempt in range(3):
        try:
            with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
                data = resp.read()
                encoding = resp.headers.get_content_charset() or "utf-8"
                return data.decode(encoding, errors="replace")
        except (URLError, HTTPError) as e:
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            log(f"  FETCH ERROR ({url[:80]}): {e}")
            return None
        except Exception as e:
            log(f"  FETCH EXCEPTION ({url[:80]}): {e}")
            return None
    return None

def fetch_json(url, timeout=15, min_delay=1.0):
    """Fetch and parse JSON from URL."""
    rate_limit(min_delay)
    req = Request(url, headers={
        "User-Agent": "PrintMaxx-IntelEngine/2.0",
        "Accept": "application/json",
    })
    try:
        with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log(f"  JSON FETCH ERROR ({url[:80]}): {e}")
        return None

def load_history():
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_history(data):
    safe_path(HISTORY_FILE)
    with open(HISTORY_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)

# ---------------------------------------------------------------------------
# A) APP COMPETITOR TRACKING (iTunes Search API - free, no auth)
# ---------------------------------------------------------------------------
APP_CATEGORIES = {
    "faith": {
        "our_app": "PrintMaxx Faith",
        "competitors": [
            "Hallow Prayer Meditation", "Pray.com Daily Prayer", "Glorify Daily Devotional",
            "Abide Bible Meditation", "YouVersion Bible", "Lectio Prayer Meditation",
            "Bible App for Kids", "Rosary Prayer Catholic", "Catholic Answers",
            "Daily Devotional Bible", "Pray Inc", "Soultime Meditation Sleep",
            "Amen Catholic Prayer", "Reimagine Faith", "Bible Memory",
            "Echo Prayer", "Dwell Audio Bible", "Verses Bible Memory",
            "PrayerMate Christian Prayer", "First 5 Proverbs 31",
        ],
    },
    "screen_time": {
        "our_app": "PrintMaxx ScreenGuard",
        "competitors": [
            "Opal Screen Time", "BePresent Screen Time", "one sec screen time",
            "ScreenZen Screen Time", "Roots Screen Time", "AppBlock Stay Focused",
            "Stay Focused App Block", "YourHour Phone Addiction", "Flipd Focus",
            "Offtime Unplug Disconnect", "Space Break Phone Addiction",
            "Freedom Screen Time", "Forest Focus Timer", "Flora Focus Timer",
            "Minimalist Phone Launcher", "Digital Wellbeing", "ClearSpace",
            "BeFocused Focus Timer", "BlockSite Block Apps", "SocialX Control",
        ],
    },
    "study": {
        "our_app": "PrintMaxx Study",
        "competitors": [
            "Gauth AI Study", "Knowunity Study", "Quizlet Flashcards",
            "Anki Flashcards", "Brainly Homework Help", "Photomath",
            "Socratic by Google", "Chegg Study", "StudySmarter Flashcards",
            "Notion Calendar", "Todoist Task Manager", "Obsidian Notes",
            "Remnote Flashcards", "Kahoot Study", "Khan Academy",
            "Duolingo Language", "Coursera Learn", "Brilliant Math",
            "Wolfram Alpha", "Symbolab Math Solver",
        ],
    },
    "fitness": {
        "our_app": "PrintMaxx Fitness",
        "competitors": [
            "FitBod Workout", "Streaks Workout", "StepBet Walking",
            "Nike Training Club", "Peloton Fitness", "Strava Running",
            "MyFitnessPal Calorie", "Strong Workout Tracker", "JEFIT Workout",
            "Hevy Gym Workout", "Fitbit Activity", "Apple Fitness",
            "Gymshark Training", "SWEAT Fitness", "Centr Chris Hemsworth",
            "Future Personal Training", "Caliber Strength", "Stronglifts 5x5",
            "WorkoutGen AI", "Freeletics Workouts",
        ],
    },
    "productivity": {
        "our_app": "PrintMaxx Focus",
        "competitors": [
            "Forest Focus Timer", "Flora Focus Timer", "Focus Keeper Timer",
            "Todoist Task Manager", "Things 3 Todo", "TickTick Task",
            "Notion Project Management", "Obsidian Notes", "Bear Notes",
            "Craft Documents", "GoodNotes Writing", "Notability Notes",
            "Structured Day Planner", "Sorted Day Planner", "Fantastical Calendar",
            "Spark Email", "Superhuman Email", "Raycast Productivity",
            "Alfred Productivity", "Keyboard Maestro",
        ],
    },
    "sleep": {
        "our_app": "PrintMaxx Sleep",
        "competitors": [
            "Sleep Cycle alarm", "Pillow Sleep Tracker", "AutoSleep Track Sleep",
            "Calm Meditation Sleep", "Headspace Mindfulness", "BetterSleep Sounds",
            "Insight Timer Meditation", "White Noise Generator", "Noisli Focus",
            "Rain Rain Sleep Sounds", "Tide Focus Sleep", "Pzizz Sleep Hypnosis",
            "SleepScore Sleep Tracker", "Sleep Watch Tracker", "Loona Bedtime Calm",
            "Sleepa Ambient Sounds", "Relax Melodies Sleep", "ShutEye Sleep Tracker",
            "Sleepiest Stories", "NapBot Sleep Tracker",
        ],
    },
    "journal": {
        "our_app": "PrintMaxx Journal",
        "competitors": [
            "Day One Journal", "Journey Diary Journal", "Stoic Mental Health",
            "Finch Self Care Pet", "Presently Gratitude", "Five Minute Journal",
            "Grid Diary Journal", "Reflectly Journal AI", "Daylio Journal",
            "Momento Photo Journal", "Penzu Diary Journal", "Diarium Journal",
            "Daily Bean Mood Diary", "Year in Pixels Mood", "Bearable Health",
            "MoodPanda Mood Diary", "Moodnotes CBT Journal", "Gratitude Journal",
            "Morning Routine Planner", "Balance Meditation",
        ],
    },
    "adhd": {
        "our_app": "ADHD-Streak",
        "competitors": [
            "Routinery Habit Routine", "Focusmate Body Double", "Tiimo ADHD Planner",
            "Sunsama Daily Planner", "Llama Life ADHD Tasks", "Brain Focus Timer",
            "Goblin Tools ADHD", "Due Reminders", "Streaks Habit Tracker",
            "Done Habit Tracker", "Habitify Habit Tracker", "Productive Habit Tracker",
            "Way of Life Habit", "Beeper Habit Tracker", "Momentum Habit Tracker",
            "Loop Habit Tracker", "Daylio Mood Tracker", "Structured Day Planner",
            "Focusplan Task Board", "ADHD Planner Daily",
        ],
    },
}

def scan_app_competitor(search_term, category):
    """Query iTunes Search API for a single competitor. Returns dict or None."""
    encoded = quote(search_term)
    url = f"https://itunes.apple.com/search?term={encoded}&entity=software&country=us&limit=3"
    data = fetch_json(url, min_delay=1.2)
    if not data or not data.get("results"):
        return None

    # Find best match
    results = data["results"]
    target_lower = search_term.lower().split()[0]
    best = results[0]
    for r in results:
        name = r.get("trackName", "").lower()
        if target_lower in name:
            best = r
            break

    # Extract review sentiment from description keywords
    desc = (best.get("description", "") or "").lower()
    positive_keywords = ["love", "amazing", "great", "best", "excellent", "perfect", "beautiful", "easy"]
    negative_keywords = ["crash", "bug", "slow", "broken", "terrible", "awful", "scam", "waste"]
    pos_count = sum(1 for kw in positive_keywords if kw in desc)
    neg_count = sum(1 for kw in negative_keywords if kw in desc)

    # Extract features from release notes
    release_notes = (best.get("releaseNotes", "") or "")
    features = []
    for line in release_notes.split("\n"):
        line = line.strip()
        if line and (line.startswith("-") or line.startswith("*") or line.startswith("+")):
            features.append(line.lstrip("-*+ ").strip()[:100])

    return {
        "trackName": best.get("trackName", ""),
        "bundleId": best.get("bundleId", ""),
        "price": best.get("price", 0),
        "formattedPrice": best.get("formattedPrice", "Free"),
        "averageUserRating": round(best.get("averageUserRating", 0), 2),
        "userRatingCount": best.get("userRatingCount", 0),
        "version": best.get("version", ""),
        "currentVersionReleaseDate": best.get("currentVersionReleaseDate", ""),
        "releaseNotes": release_notes[:500],
        "recentFeatures": features[:5],
        "primaryGenreName": best.get("primaryGenreName", ""),
        "sellerName": best.get("sellerName", ""),
        "fileSizeBytes": best.get("fileSizeBytes", "0"),
        "description_preview": (best.get("description", "") or "")[:300],
        "positive_sentiment_score": pos_count,
        "negative_sentiment_score": neg_count,
        "has_iap": bool(best.get("isGameCenterEnabled") or best.get("price", 0) == 0),
        "content_rating": best.get("contentAdvisoryRating", ""),
        "min_os": best.get("minimumOsVersion", ""),
        "category": category,
        "scan_date": datetime.now().isoformat(),
    }


def scan_all_apps(history):
    """Scan all 7 categories x 20 competitors = up to 140 apps."""
    log("=" * 60)
    log("APP COMPETITOR SCAN")
    log("=" * 60)

    app_results = history.get("apps", {})
    total_scanned = 0
    total_errors = 0

    for cat_name, cat_data in APP_CATEGORIES.items():
        log(f"\n--- {cat_name.upper()} ({cat_data['our_app']}) ---")
        competitors = cat_data["competitors"]

        for i, search_term in enumerate(competitors, 1):
            log(f"  [{i}/{len(competitors)}] {search_term}")
            result = scan_app_competitor(search_term, cat_name)

            if result:
                key = result["trackName"].lower().replace(" ", "_")
                if key not in app_results:
                    app_results[key] = {"snapshots": []}
                app_results[key]["snapshots"].append(result)
                # Keep last 52 snapshots (1 year of weekly scans)
                if len(app_results[key]["snapshots"]) > 52:
                    app_results[key]["snapshots"] = app_results[key]["snapshots"][-52:]
                total_scanned += 1

                price_str = result["formattedPrice"]
                rating = result["averageUserRating"]
                count = result["userRatingCount"]
                log(f"    {result['trackName']}: {price_str}, {rating:.1f}/5 ({count:,} ratings)")
            else:
                total_errors += 1
                log(f"    NOT FOUND")

    history["apps"] = app_results
    log(f"\nApp scan complete. {total_scanned} scanned, {total_errors} errors.")
    return history


# ---------------------------------------------------------------------------
# B) SERVICE COMPETITOR PRICING (Fiverr, Upwork, Agencies)
# ---------------------------------------------------------------------------
SERVICE_CATEGORIES = {
    "app_development": {
        "fiverr_query": "ios app development swift",
        "upwork_query": "ios app developer",
        "keywords": ["app", "ios", "swift", "mobile", "react native", "flutter"],
    },
    "web_design": {
        "fiverr_query": "website design",
        "upwork_query": "web designer",
        "keywords": ["web", "design", "landing page", "wordpress", "shopify"],
    },
    "seo_services": {
        "fiverr_query": "seo optimization",
        "upwork_query": "seo specialist",
        "keywords": ["seo", "search engine", "backlinks", "ranking"],
    },
    "content_writing": {
        "fiverr_query": "blog content writing",
        "upwork_query": "content writer",
        "keywords": ["content", "blog", "copywriting", "articles"],
    },
    "social_media": {
        "fiverr_query": "social media management",
        "upwork_query": "social media manager",
        "keywords": ["social media", "instagram", "twitter", "tiktok"],
    },
    "email_marketing": {
        "fiverr_query": "email marketing automation",
        "upwork_query": "email marketing specialist",
        "keywords": ["email", "newsletter", "mailchimp", "automation"],
    },
}

# Known competitor agencies to monitor
COMPETITOR_AGENCIES = [
    {"name": "Toptal", "url": "https://www.toptal.com", "category": "freelance_platform"},
    {"name": "99designs", "url": "https://99designs.com", "category": "design"},
    {"name": "Designhill", "url": "https://www.designhill.com", "category": "design"},
    {"name": "Codeable", "url": "https://www.codeable.io", "category": "development"},
    {"name": "GrowthMachine", "url": "https://www.growthmachine.com", "category": "seo"},
    {"name": "WebFX", "url": "https://www.webfx.com", "category": "agency"},
    {"name": "Siege Media", "url": "https://www.siegemedia.com", "category": "content"},
    {"name": "SingleGrain", "url": "https://www.singlegrain.com", "category": "agency"},
]


def extract_pricing_from_html(html, keywords):
    """Extract pricing signals from HTML content using regex patterns."""
    if not html:
        return []

    prices = []
    # Match patterns like $XX, $XX/hr, $XX/mo, $X,XXX, starting at $XX
    price_patterns = [
        r'\$[\d,]+(?:\.\d{2})?(?:\s*/\s*(?:hr|hour|mo|month|project|page|word))?',
        r'(?:starting|from|as low as)\s+(?:at\s+)?\$[\d,]+',
        r'(?:price|cost|rate|fee)s?\s*(?::|from|starting)?\s*\$[\d,]+',
    ]

    for pattern in price_patterns:
        matches = re.findall(pattern, html.lower())
        for match in matches[:10]:
            prices.append(match.strip())

    return list(set(prices))[:15]


def scan_fiverr_category(category_name, query):
    """Scrape Fiverr search results for pricing data."""
    url = f"https://www.fiverr.com/search/gigs?query={quote(query)}&source=top-bar&search_in=everywhere&search-autocomplete-original-term={quote(query)}"
    html = fetch_url(url, min_delay=3.0)
    if not html:
        return {"source": "fiverr", "category": category_name, "query": query,
                "status": "fetch_failed", "gigs": [], "scan_date": datetime.now().isoformat(),
                "price_min": None, "price_max": None, "price_median": None, "price_count": 0}

    # Extract gig pricing from HTML
    gig_prices = []
    # Pattern: "Starting at $X" or price displays
    starting_at = re.findall(r'(?:starting\s+at|from)\s+\$(\d+)', html.lower())
    price_tags = re.findall(r'class="[^"]*price[^"]*"[^>]*>\s*\$(\d+)', html.lower())
    all_prices = starting_at + price_tags

    for p in all_prices[:20]:
        try:
            gig_prices.append(int(p))
        except ValueError:
            continue

    # Extract seller levels
    levels = re.findall(r'(top rated|level [12]|new seller)', html.lower())
    level_counts = defaultdict(int)
    for lv in levels:
        level_counts[lv] += 1

    result = {
        "source": "fiverr",
        "category": category_name,
        "query": query,
        "status": "ok" if gig_prices else "no_prices_found",
        "price_min": min(gig_prices) if gig_prices else None,
        "price_max": max(gig_prices) if gig_prices else None,
        "price_median": sorted(gig_prices)[len(gig_prices)//2] if gig_prices else None,
        "price_count": len(gig_prices),
        "seller_levels": dict(level_counts),
        "gig_prices": gig_prices[:20],
        "scan_date": datetime.now().isoformat(),
    }
    return result


def scan_upwork_category(category_name, query):
    """Scrape Upwork search results for freelancer rates."""
    url = f"https://www.upwork.com/search/profiles/?q={quote(query)}"
    html = fetch_url(url, min_delay=3.0)
    if not html:
        return {"source": "upwork", "category": category_name, "query": query,
                "status": "fetch_failed", "rates": [], "rate_min": None, "rate_max": None,
                "rate_median": None, "rate_count": 0, "scan_date": datetime.now().isoformat()}

    # Extract hourly rates
    rates = []
    rate_matches = re.findall(r'\$(\d+(?:\.\d{2})?)\s*/\s*hr', html)
    for r in rate_matches[:30]:
        try:
            rates.append(float(r))
        except ValueError:
            continue

    # Extract success rates
    success_rates = re.findall(r'(\d{1,3})%\s*(?:job\s+)?success', html.lower())

    result = {
        "source": "upwork",
        "category": category_name,
        "query": query,
        "status": "ok" if rates else "no_rates_found",
        "rate_min": min(rates) if rates else None,
        "rate_max": max(rates) if rates else None,
        "rate_median": sorted(rates)[len(rates)//2] if rates else None,
        "rate_count": len(rates),
        "hourly_rates": rates[:20],
        "avg_success_rate": (sum(int(s) for s in success_rates) / len(success_rates)) if success_rates else None,
        "scan_date": datetime.now().isoformat(),
    }
    return result


def scan_agency_pricing(agency):
    """Check agency website for pricing signals."""
    html = fetch_url(agency["url"], min_delay=2.0)
    if not html:
        return {"name": agency["name"], "url": agency["url"],
                "status": "fetch_failed", "scan_date": datetime.now().isoformat()}

    prices = extract_pricing_from_html(html, [])

    # Check for pricing page
    has_pricing_page = bool(re.search(r'href="[^"]*pric', html.lower()))

    # Check for free trial/demo
    has_free_trial = bool(re.search(r'free\s*(trial|demo|consultation|quote|estimate)', html.lower()))

    # Check for case studies
    has_case_studies = bool(re.search(r'case\s*stud(?:y|ies)', html.lower()))

    return {
        "name": agency["name"],
        "url": agency["url"],
        "category": agency["category"],
        "status": "ok",
        "prices_found": prices[:10],
        "has_pricing_page": has_pricing_page,
        "has_free_trial": has_free_trial,
        "has_case_studies": has_case_studies,
        "scan_date": datetime.now().isoformat(),
    }


def scan_all_services(history):
    """Scan Fiverr, Upwork, and agency pricing."""
    log("\n" + "=" * 60)
    log("SERVICE COMPETITOR PRICING SCAN")
    log("=" * 60)

    service_results = history.get("services", {})

    # Fiverr
    log("\n--- FIVERR ---")
    fiverr_data = []
    for cat_name, cat_data in SERVICE_CATEGORIES.items():
        log(f"  Scanning: {cat_name}")
        result = scan_fiverr_category(cat_name, cat_data["fiverr_query"])
        fiverr_data.append(result)
        if result["price_min"] is not None:
            log(f"    Prices: ${result['price_min']} - ${result['price_max']} (median: ${result['price_median']})")
        else:
            log(f"    {result['status']}")
    service_results["fiverr"] = fiverr_data

    # Upwork
    log("\n--- UPWORK ---")
    upwork_data = []
    for cat_name, cat_data in SERVICE_CATEGORIES.items():
        log(f"  Scanning: {cat_name}")
        result = scan_upwork_category(cat_name, cat_data["upwork_query"])
        upwork_data.append(result)
        if result["rate_min"] is not None:
            log(f"    Rates: ${result['rate_min']}/hr - ${result['rate_max']}/hr (median: ${result['rate_median']}/hr)")
        else:
            log(f"    {result['status']}")
    service_results["upwork"] = upwork_data

    # Agencies
    log("\n--- AGENCIES ---")
    agency_data = []
    for agency in COMPETITOR_AGENCIES:
        log(f"  Checking: {agency['name']}")
        result = scan_agency_pricing(agency)
        agency_data.append(result)
        if result.get("prices_found"):
            log(f"    Prices found: {', '.join(result['prices_found'][:5])}")
        log(f"    Pricing page: {'Yes' if result.get('has_pricing_page') else 'No'} | "
            f"Free trial: {'Yes' if result.get('has_free_trial') else 'No'} | "
            f"Case studies: {'Yes' if result.get('has_case_studies') else 'No'}")
    service_results["agencies"] = agency_data

    history["services"] = service_results
    log("\nService pricing scan complete.")
    return history


# ---------------------------------------------------------------------------
# C) CONTENT COMPETITOR ANALYSIS (Twitter, Gumroad, Substack)
# ---------------------------------------------------------------------------
COMPETITOR_TWITTER_ACCOUNTS = [
    {"handle": "levelsio", "niche": "indie_hacking"},
    {"handle": "taborbayfern", "niche": "solopreneur"},
    {"handle": "dannypostmaa", "niche": "indie_hacking"},
    {"handle": "marc_louvion", "niche": "indie_hacking"},
    {"handle": "tdinh_me", "niche": "indie_hacking"},
    {"handle": "arlogilbert", "niche": "saas"},
    {"handle": "paborenstein", "niche": "indie_hacking"},
    {"handle": "cloakdagger_", "niche": "design"},
    {"handle": "sweatystartup", "niche": "service_business"},
    {"handle": "SahilBloom", "niche": "content_creator"},
    {"handle": "JamesonCamp", "niche": "apps"},
    {"handle": "iamkelyanme", "niche": "apps"},
    {"handle": "hnshah", "niche": "saas"},
    {"handle": "thepatwalls", "niche": "indie_hacking"},
    {"handle": "aaborot", "niche": "automation"},
    {"handle": "naval", "niche": "philosophy"},
    {"handle": "jackbutcher", "niche": "productized"},
    {"handle": "david_perell", "niche": "writing"},
    {"handle": "Julian", "niche": "writing"},
    {"handle": "dickiebush", "niche": "writing"},
]

COMPETITOR_GUMROAD_STORES = [
    {"name": "Pieter Levels", "url": "https://levelsio.gumroad.com", "niche": "indie_hacking"},
    {"name": "Marc Lou", "url": "https://marclou.gumroad.com", "niche": "indie_hacking"},
    {"name": "Danny Postma", "url": "https://dannypostma.gumroad.com", "niche": "indie_hacking"},
    {"name": "Tibo", "url": "https://tibo.gumroad.com", "niche": "saas"},
    {"name": "Ship Fast", "url": "https://shipfa.st", "niche": "boilerplates"},
    {"name": "Indie Page", "url": "https://indiepage.gumroad.com", "niche": "indie_hacking"},
    {"name": "Starter Story", "url": "https://starterstory.gumroad.com", "niche": "business"},
    {"name": "Growth Design", "url": "https://growth.design", "niche": "design"},
]

COMPETITOR_NEWSLETTERS = [
    {"name": "Starter Story", "url": "https://starterstory.substack.com", "niche": "business"},
    {"name": "Indie Hackers", "url": "https://indiehackers.substack.com", "niche": "indie_hacking"},
    {"name": "The Bootstrapped Founder", "url": "https://thebootstrappedfounder.substack.com", "niche": "indie_hacking"},
    {"name": "Lenny's Newsletter", "url": "https://www.lennysnewsletter.com", "niche": "product"},
    {"name": "Growth in Reverse", "url": "https://growthinreverse.substack.com", "niche": "content"},
    {"name": "Justin Welsh", "url": "https://justinwelsh.substack.com", "niche": "solopreneur"},
    {"name": "Dan Koe", "url": "https://dankoe.substack.com", "niche": "creator_economy"},
    {"name": "Nathan Barry", "url": "https://nathanbarry.substack.com", "niche": "creator_economy"},
]


def scan_twitter_profile(handle):
    """Estimate Twitter engagement from Nitter or public page."""
    # Try multiple Nitter instances (public, no auth)
    nitter_instances = [
        f"https://nitter.privacydev.net/{handle}",
        f"https://nitter.poast.org/{handle}",
    ]

    for nitter_url in nitter_instances:
        html = fetch_url(nitter_url, min_delay=2.0)
        if html and len(html) > 1000:
            break
    else:
        html = None

    result = {
        "handle": handle,
        "status": "fetch_failed",
        "followers": None,
        "following": None,
        "tweets": None,
        "recent_engagement_avg": None,
        "posting_frequency": None,
        "scan_date": datetime.now().isoformat(),
    }

    if not html:
        return result

    result["status"] = "ok"

    # Extract follower count
    follower_match = re.search(r'(\d[\d,]*)\s*Follower', html)
    if follower_match:
        result["followers"] = int(follower_match.group(1).replace(",", ""))

    following_match = re.search(r'(\d[\d,]*)\s*Following', html)
    if following_match:
        result["following"] = int(following_match.group(1).replace(",", ""))

    tweet_count_match = re.search(r'(\d[\d,]*)\s*(?:Tweets?|Posts?)', html)
    if tweet_count_match:
        result["tweets"] = int(tweet_count_match.group(1).replace(",", ""))

    # Extract engagement from recent posts (likes, retweets, replies)
    likes = re.findall(r'icon-heart[^>]*>\s*</span>\s*<span[^>]*>(\d[\d,]*)', html)
    retweets = re.findall(r'icon-retweet[^>]*>\s*</span>\s*<span[^>]*>(\d[\d,]*)', html)
    replies = re.findall(r'icon-comment[^>]*>\s*</span>\s*<span[^>]*>(\d[\d,]*)', html)

    engagement_counts = []
    for like_str in likes[:20]:
        try:
            engagement_counts.append(int(like_str.replace(",", "")))
        except ValueError:
            continue

    if engagement_counts:
        result["recent_engagement_avg"] = round(sum(engagement_counts) / len(engagement_counts), 1)

    # Estimate posting frequency from timestamps
    timestamps = re.findall(r'(\w+ \d+, \d{4})', html)
    if len(timestamps) >= 2:
        result["posting_frequency"] = f"~{len(timestamps)} posts visible"

    return result


def scan_gumroad_store(store):
    """Scrape Gumroad store for products and pricing."""
    html = fetch_url(store["url"], min_delay=2.5)
    if not html:
        return {"name": store["name"], "url": store["url"], "niche": store["niche"],
                "status": "fetch_failed", "products": [], "scan_date": datetime.now().isoformat()}

    # Extract product prices
    products = []
    price_matches = re.findall(r'\$(\d+(?:\.\d{2})?)', html)
    product_names = re.findall(r'class="[^"]*product[^"]*name[^"]*"[^>]*>([^<]+)', html)

    # Estimate sales from "X sales" patterns
    sales_matches = re.findall(r'(\d[\d,]*)\s*(?:sales|sold|ratings?|reviews?)', html.lower())
    total_sales_est = 0
    for s in sales_matches:
        try:
            total_sales_est += int(s.replace(",", ""))
        except ValueError:
            continue

    prices = []
    for p in price_matches[:20]:
        try:
            val = float(p)
            if 1 <= val <= 10000:
                prices.append(val)
        except ValueError:
            continue

    return {
        "name": store["name"],
        "url": store["url"],
        "niche": store["niche"],
        "status": "ok",
        "product_count": len(product_names) or len(prices),
        "prices": prices[:15],
        "price_min": min(prices) if prices else None,
        "price_max": max(prices) if prices else None,
        "price_avg": round(sum(prices) / len(prices), 2) if prices else None,
        "estimated_total_sales": total_sales_est,
        "estimated_revenue": round(total_sales_est * (sum(prices) / len(prices) if prices else 0), 2),
        "scan_date": datetime.now().isoformat(),
    }


def scan_substack_newsletter(newsletter):
    """Estimate Substack subscriber count from public page."""
    html = fetch_url(newsletter["url"], min_delay=2.0)
    if not html:
        return {"name": newsletter["name"], "url": newsletter["url"],
                "status": "fetch_failed", "scan_date": datetime.now().isoformat()}

    # Extract subscriber count
    sub_count = None
    sub_match = re.search(r'(\d[\d,]*)\s*subscriber', html.lower())
    if sub_match:
        sub_count = int(sub_match.group(1).replace(",", ""))

    # Check posting frequency
    post_dates = re.findall(r'(\w+ \d+, \d{4})', html)
    recent_posts = len(post_dates)

    # Check if paid
    is_paid = bool(re.search(r'paid|subscribe|premium|founding member', html.lower()))

    # Check free vs paid posts ratio
    free_posts = len(re.findall(r'free', html.lower()))
    paid_posts = len(re.findall(r'paid\s+(?:only|subscriber)', html.lower()))

    return {
        "name": newsletter["name"],
        "url": newsletter["url"],
        "niche": newsletter["niche"],
        "status": "ok",
        "estimated_subscribers": sub_count,
        "recent_post_count": recent_posts,
        "has_paid_tier": is_paid,
        "scan_date": datetime.now().isoformat(),
    }


def scan_all_content(history):
    """Scan Twitter accounts, Gumroad stores, Substack newsletters."""
    log("\n" + "=" * 60)
    log("CONTENT COMPETITOR ANALYSIS")
    log("=" * 60)

    content_results = history.get("content", {})

    # Twitter
    log("\n--- TWITTER/X ACCOUNTS ---")
    twitter_data = []
    for account in COMPETITOR_TWITTER_ACCOUNTS:
        log(f"  @{account['handle']} ({account['niche']})")
        result = scan_twitter_profile(account["handle"])
        result["niche"] = account["niche"]
        twitter_data.append(result)
        if result.get("followers"):
            log(f"    {result['followers']:,} followers | Avg engagement: {result.get('recent_engagement_avg', 'N/A')}")
        else:
            log(f"    {result['status']}")
    content_results["twitter"] = twitter_data

    # Gumroad
    log("\n--- GUMROAD STORES ---")
    gumroad_data = []
    for store in COMPETITOR_GUMROAD_STORES:
        log(f"  {store['name']}")
        result = scan_gumroad_store(store)
        gumroad_data.append(result)
        if result.get("prices"):
            log(f"    {result['product_count']} products | ${result['price_min']}-${result['price_max']} | "
                f"Est sales: {result['estimated_total_sales']}")
        else:
            log(f"    {result['status']}")
    content_results["gumroad"] = gumroad_data

    # Substack
    log("\n--- SUBSTACK NEWSLETTERS ---")
    newsletter_data = []
    for nl in COMPETITOR_NEWSLETTERS:
        log(f"  {nl['name']}")
        result = scan_substack_newsletter(nl)
        newsletter_data.append(result)
        if result.get("estimated_subscribers"):
            log(f"    ~{result['estimated_subscribers']:,} subscribers | Paid: {'Yes' if result.get('has_paid_tier') else 'No'}")
        else:
            log(f"    {result['status']}")
    content_results["newsletters"] = newsletter_data

    history["content"] = content_results
    log("\nContent competitor scan complete.")
    return history


# ---------------------------------------------------------------------------
# D) GAP ANALYSIS
# ---------------------------------------------------------------------------
def run_gap_analysis(history):
    """Analyze gaps across apps, services, and content."""
    log("\n" + "=" * 60)
    log("GAP ANALYSIS")
    log("=" * 60)

    gaps = {
        "feature_gaps": [],
        "pricing_gaps": [],
        "content_gaps": [],
        "platform_gaps": [],
        "analysis_date": datetime.now().isoformat(),
    }

    # Feature gaps: what do top-rated competitors have that we might not?
    app_data = history.get("apps", {})
    top_features = defaultdict(list)
    pricing_by_cat = defaultdict(list)

    for app_key, app_info in app_data.items():
        if not app_info.get("snapshots"):
            continue
        latest = app_info["snapshots"][-1]
        cat = latest.get("category", "unknown")

        # Collect features from release notes
        for feat in latest.get("recentFeatures", []):
            top_features[cat].append(feat)

        # Collect pricing
        price = latest.get("price", 0)
        pricing_by_cat[cat].append({
            "name": latest.get("trackName", ""),
            "price": price,
            "formatted": latest.get("formattedPrice", "Free"),
            "rating": latest.get("averageUserRating", 0),
            "rating_count": latest.get("userRatingCount", 0),
        })

    # Feature gap analysis
    log("\n--- FEATURE GAPS ---")
    common_features_by_cat = {
        "faith": ["offline mode", "daily devotional", "audio prayers", "community", "scripture", "meditation",
                   "prayer journal", "bible reading plan", "push notifications", "widget"],
        "screen_time": ["app blocking", "usage stats", "focus mode", "scheduled blocks", "whitelist",
                        "family sharing", "real-time tracking", "website blocking", "break reminders", "widget"],
        "study": ["flashcards", "ai tutor", "spaced repetition", "collaboration", "practice tests",
                  "study plans", "offline mode", "handwriting", "voice notes", "progress tracking"],
        "fitness": ["workout plans", "exercise library", "progress photos", "social features", "wearable sync",
                    "nutrition tracking", "rest timer", "custom exercises", "workout history", "apple watch"],
        "productivity": ["pomodoro", "habit tracking", "calendar sync", "widgets", "apple watch",
                        "shortcuts", "templates", "collaboration", "dark mode", "cross-platform"],
        "sleep": ["sleep sounds", "smart alarm", "sleep stories", "breathing exercises", "sleep tracking",
                  "sleep score", "snore detection", "heart rate", "apple watch", "widget"],
        "journal": ["daily prompts", "mood tracking", "photo journal", "templates", "export",
                    "encryption", "search", "tags", "calendar view", "streak tracking"],
        "adhd": ["flexible streaks", "body double timer", "variable rewards", "flexible frequency",
                 "no shame resets", "focus mode", "dopamine hooks", "simple UI", "reminders", "widget"],
    }

    for cat, features in common_features_by_cat.items():
        gaps["feature_gaps"].append({
            "category": cat,
            "must_have_features": features[:5],
            "nice_to_have_features": features[5:],
            "competitor_feature_mentions": top_features.get(cat, [])[:10],
        })
        log(f"  {cat}: {len(features)} expected features tracked")

    # Pricing gap analysis
    log("\n--- PRICING GAPS ---")
    for cat, apps in pricing_by_cat.items():
        free_count = sum(1 for a in apps if a["price"] == 0)
        paid_apps = [a for a in apps if a["price"] > 0]
        paid_prices = [a["price"] for a in paid_apps]

        gap = {
            "category": cat,
            "total_tracked": len(apps),
            "free_count": free_count,
            "paid_count": len(paid_apps),
            "price_range": f"${min(paid_prices):.2f}-${max(paid_prices):.2f}" if paid_prices else "all free",
            "avg_price": round(sum(paid_prices) / len(paid_prices), 2) if paid_prices else 0,
            "opportunity": "",
        }

        # Identify pricing opportunities
        if free_count > len(apps) * 0.7:
            gap["opportunity"] = "Market is mostly free. Freemium with premium upsell is the play."
        elif paid_prices and max(paid_prices) < 5:
            gap["opportunity"] = "Low price ceiling. Differentiate on value, not price."
        elif paid_prices and min(paid_prices) > 5:
            gap["opportunity"] = "No budget option exists. Undercut on price to capture volume."

        gaps["pricing_gaps"].append(gap)
        log(f"  {cat}: {free_count} free / {len(paid_apps)} paid | "
            f"{'$' + str(round(sum(paid_prices)/len(paid_prices),2)) + ' avg' if paid_prices else 'N/A'}")

    # Content format gaps
    log("\n--- CONTENT FORMAT GAPS ---")
    content_data = history.get("content", {})

    our_formats = ["twitter", "landing_pages", "apps"]
    competitor_formats = set()

    for tw in content_data.get("twitter", []):
        if tw.get("followers") and tw["followers"] > 10000:
            competitor_formats.add("twitter_high_following")
    for gm in content_data.get("gumroad", []):
        if gm.get("product_count") and gm["product_count"] > 0:
            competitor_formats.add("digital_products_gumroad")
    for nl in content_data.get("newsletters", []):
        if nl.get("estimated_subscribers") and nl["estimated_subscribers"] > 1000:
            competitor_formats.add("newsletter_substack")
            if nl.get("has_paid_tier"):
                competitor_formats.add("paid_newsletter")

    all_possible_formats = [
        "twitter_high_following", "youtube_channel", "podcast", "newsletter_substack",
        "paid_newsletter", "digital_products_gumroad", "online_course", "discord_community",
        "reddit_presence", "linkedin_newsletter", "tiktok", "instagram_reels",
        "blog_seo", "affiliate_program", "saas_product", "mobile_app",
    ]

    missing_formats = [f for f in all_possible_formats if f not in our_formats]
    gaps["content_gaps"] = {
        "competitor_active_formats": list(competitor_formats),
        "all_possible_formats": all_possible_formats,
        "we_are_missing": missing_formats,
        "priority_formats": [f for f in missing_formats if f in competitor_formats],
    }

    for fmt in gaps["content_gaps"]["priority_formats"]:
        log(f"  MISSING (competitors have): {fmt}")

    # Platform gaps
    log("\n--- PLATFORM GAPS ---")
    platforms_competitors_use = [
        "ios_app_store", "google_play", "web_app", "macos_app",
        "chrome_extension", "apple_watch", "android_widget",
        "slack_integration", "notion_integration", "zapier_integration",
    ]
    our_platforms = ["ios_app_store", "web_app"]
    platform_gaps = [p for p in platforms_competitors_use if p not in our_platforms]
    gaps["platform_gaps"] = {
        "competitor_platforms": platforms_competitors_use,
        "our_platforms": our_platforms,
        "missing": platform_gaps,
    }

    for p in platform_gaps:
        log(f"  MISSING platform: {p}")

    history["gaps"] = gaps
    log("\nGap analysis complete.")
    return history


# ---------------------------------------------------------------------------
# OUTPUT: CSV + Report
# ---------------------------------------------------------------------------
def write_intel_csv(history):
    """Write structured competitive intelligence to CSV."""
    safe_path(INTEL_CSV)
    rows = []

    # App data
    for app_key, app_info in history.get("apps", {}).items():
        if not app_info.get("snapshots"):
            continue
        latest = app_info["snapshots"][-1]
        rows.append({
            "type": "app",
            "category": latest.get("category", ""),
            "name": latest.get("trackName", ""),
            "price": latest.get("formattedPrice", ""),
            "rating": latest.get("averageUserRating", ""),
            "rating_count": latest.get("userRatingCount", ""),
            "version": latest.get("version", ""),
            "last_updated": latest.get("currentVersionReleaseDate", "")[:10],
            "positive_sentiment": latest.get("positive_sentiment_score", ""),
            "negative_sentiment": latest.get("negative_sentiment_score", ""),
            "source": "itunes_api",
            "url": "",
            "metric_1": latest.get("sellerName", ""),
            "metric_2": latest.get("primaryGenreName", ""),
            "notes": "; ".join(latest.get("recentFeatures", [])[:3]),
            "scan_date": latest.get("scan_date", ""),
        })

    # Service data
    for fiverr in history.get("services", {}).get("fiverr", []):
        rows.append({
            "type": "service_fiverr",
            "category": fiverr.get("category", ""),
            "name": f"Fiverr: {fiverr.get('query', '')}",
            "price": f"${fiverr.get('price_min', '?')}-${fiverr.get('price_max', '?')}",
            "rating": "",
            "rating_count": fiverr.get("price_count", ""),
            "version": "",
            "last_updated": "",
            "positive_sentiment": "",
            "negative_sentiment": "",
            "source": "fiverr",
            "url": "",
            "metric_1": f"Median: ${fiverr.get('price_median', '?')}",
            "metric_2": str(fiverr.get("seller_levels", "")),
            "notes": fiverr.get("status", ""),
            "scan_date": fiverr.get("scan_date", ""),
        })

    for upwork in history.get("services", {}).get("upwork", []):
        rows.append({
            "type": "service_upwork",
            "category": upwork.get("category", ""),
            "name": f"Upwork: {upwork.get('query', '')}",
            "price": f"${upwork.get('rate_min', '?')}-${upwork.get('rate_max', '?')}/hr",
            "rating": "",
            "rating_count": upwork.get("rate_count", ""),
            "version": "",
            "last_updated": "",
            "positive_sentiment": "",
            "negative_sentiment": "",
            "source": "upwork",
            "url": "",
            "metric_1": f"Median: ${upwork.get('rate_median', '?')}/hr",
            "metric_2": f"Avg success: {upwork.get('avg_success_rate', '?')}%",
            "notes": upwork.get("status", ""),
            "scan_date": upwork.get("scan_date", ""),
        })

    # Content data
    for tw in history.get("content", {}).get("twitter", []):
        rows.append({
            "type": "content_twitter",
            "category": tw.get("niche", ""),
            "name": f"@{tw.get('handle', '')}",
            "price": "",
            "rating": "",
            "rating_count": tw.get("followers", ""),
            "version": "",
            "last_updated": "",
            "positive_sentiment": "",
            "negative_sentiment": "",
            "source": "twitter",
            "url": f"https://x.com/{tw.get('handle', '')}",
            "metric_1": f"Engagement avg: {tw.get('recent_engagement_avg', 'N/A')}",
            "metric_2": f"Following: {tw.get('following', 'N/A')}",
            "notes": tw.get("posting_frequency", ""),
            "scan_date": tw.get("scan_date", ""),
        })

    for gm in history.get("content", {}).get("gumroad", []):
        rows.append({
            "type": "content_gumroad",
            "category": gm.get("niche", ""),
            "name": gm.get("name", ""),
            "price": f"${gm.get('price_min', '?')}-${gm.get('price_max', '?')}" if gm.get("prices") else "",
            "rating": "",
            "rating_count": gm.get("estimated_total_sales", ""),
            "version": "",
            "last_updated": "",
            "positive_sentiment": "",
            "negative_sentiment": "",
            "source": "gumroad",
            "url": gm.get("url", ""),
            "metric_1": f"{gm.get('product_count', 0)} products",
            "metric_2": f"Est revenue: ${gm.get('estimated_revenue', 0)}",
            "notes": "",
            "scan_date": gm.get("scan_date", ""),
        })

    fieldnames = [
        "type", "category", "name", "price", "rating", "rating_count",
        "version", "last_updated", "positive_sentiment", "negative_sentiment",
        "source", "url", "metric_1", "metric_2", "notes", "scan_date",
    ]

    with open(INTEL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log(f"\nWrote {len(rows)} rows to {INTEL_CSV}")
    return len(rows)


def generate_report(history):
    """Generate markdown report."""
    today = datetime.now().strftime("%b%d").upper()
    report_path = safe_path(OPS_DIR / f"COMPETITIVE_INTEL_REPORT_{today}.md")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"# Competitive Intelligence Report",
        f"Generated: {ts}",
        "",
    ]

    # --- APP SECTION ---
    lines.append("## App Competitors")
    lines.append("")
    app_data = history.get("apps", {})

    for cat_name in APP_CATEGORIES:
        cat_apps = []
        for app_key, app_info in app_data.items():
            if not app_info.get("snapshots"):
                continue
            latest = app_info["snapshots"][-1]
            if latest.get("category") == cat_name:
                cat_apps.append(latest)

        if not cat_apps:
            continue

        cat_apps.sort(key=lambda x: x.get("userRatingCount", 0), reverse=True)
        lines.append(f"### {cat_name.replace('_', ' ').title()} ({len(cat_apps)} tracked)")
        lines.append("")
        lines.append(f"| App | Price | Rating | Reviews | Version | Updated |")
        lines.append(f"|-----|-------|--------|---------|---------|---------|")

        for app in cat_apps[:15]:
            name = app.get("trackName", "?")[:25]
            price = app.get("formattedPrice", "?")
            rating = f"{app.get('averageUserRating', 0):.1f}"
            count = f"{app.get('userRatingCount', 0):,}"
            ver = app.get("version", "?")
            updated = app.get("currentVersionReleaseDate", "?")[:10]
            lines.append(f"| {name} | {price} | {rating} | {count} | {ver} | {updated} |")

        lines.append("")

    # --- SERVICE SECTION ---
    lines.append("## Service Pricing")
    lines.append("")

    services = history.get("services", {})
    if services.get("fiverr"):
        lines.append("### Fiverr Pricing")
        lines.append("")
        lines.append("| Category | Min | Max | Median | Gigs Found |")
        lines.append("|----------|-----|-----|--------|------------|")
        for f_data in services["fiverr"]:
            cat = f_data.get("category", "?")
            p_min = f"${f_data.get('price_min', '?')}" if f_data.get("price_min") else "N/A"
            p_max = f"${f_data.get('price_max', '?')}" if f_data.get("price_max") else "N/A"
            p_med = f"${f_data.get('price_median', '?')}" if f_data.get("price_median") else "N/A"
            cnt = f_data.get("price_count", 0)
            lines.append(f"| {cat} | {p_min} | {p_max} | {p_med} | {cnt} |")
        lines.append("")

    if services.get("upwork"):
        lines.append("### Upwork Hourly Rates")
        lines.append("")
        lines.append("| Category | Min/hr | Max/hr | Median/hr | Profiles |")
        lines.append("|----------|--------|--------|-----------|----------|")
        for u_data in services["upwork"]:
            cat = u_data.get("category", "?")
            r_min = f"${u_data.get('rate_min', '?')}" if u_data.get("rate_min") else "N/A"
            r_max = f"${u_data.get('rate_max', '?')}" if u_data.get("rate_max") else "N/A"
            r_med = f"${u_data.get('rate_median', '?')}" if u_data.get("rate_median") else "N/A"
            cnt = u_data.get("rate_count", 0)
            lines.append(f"| {cat} | {r_min} | {r_max} | {r_med} | {cnt} |")
        lines.append("")

    if services.get("agencies"):
        lines.append("### Agency Competitors")
        lines.append("")
        lines.append("| Agency | Category | Pricing Page | Free Trial | Case Studies | Prices Found |")
        lines.append("|--------|----------|-------------|------------|-------------|-------------|")
        for ag in services["agencies"]:
            name = ag.get("name", "?")
            cat = ag.get("category", "?")
            pp = "Yes" if ag.get("has_pricing_page") else "No"
            ft = "Yes" if ag.get("has_free_trial") else "No"
            cs = "Yes" if ag.get("has_case_studies") else "No"
            prices = ", ".join(ag.get("prices_found", [])[:3]) or "None found"
            lines.append(f"| {name} | {cat} | {pp} | {ft} | {cs} | {prices} |")
        lines.append("")

    # --- CONTENT SECTION ---
    lines.append("## Content Competitors")
    lines.append("")

    content = history.get("content", {})
    if content.get("twitter"):
        lines.append("### Twitter/X Accounts")
        lines.append("")
        lines.append("| Handle | Niche | Followers | Avg Engagement |")
        lines.append("|--------|-------|-----------|----------------|")
        tw_sorted = sorted(content["twitter"],
                           key=lambda x: x.get("followers") or 0, reverse=True)
        for tw in tw_sorted:
            handle = f"@{tw.get('handle', '?')}"
            niche = tw.get("niche", "?")
            followers = f"{tw['followers']:,}" if tw.get("followers") else "N/A"
            eng = tw.get("recent_engagement_avg", "N/A")
            lines.append(f"| {handle} | {niche} | {followers} | {eng} |")
        lines.append("")

    if content.get("gumroad"):
        lines.append("### Gumroad Stores")
        lines.append("")
        lines.append("| Store | Niche | Products | Price Range | Est Sales | Est Revenue |")
        lines.append("|-------|-------|----------|-------------|-----------|-------------|")
        for gm in content["gumroad"]:
            name = gm.get("name", "?")
            niche = gm.get("niche", "?")
            prods = gm.get("product_count", 0)
            pr = f"${gm.get('price_min', '?')}-${gm.get('price_max', '?')}" if gm.get("prices") else "N/A"
            sales = f"{gm.get('estimated_total_sales', 0):,}"
            rev = f"${gm.get('estimated_revenue', 0):,.0f}"
            lines.append(f"| {name} | {niche} | {prods} | {pr} | {sales} | {rev} |")
        lines.append("")

    if content.get("newsletters"):
        lines.append("### Substack Newsletters")
        lines.append("")
        lines.append("| Newsletter | Niche | Est Subscribers | Paid Tier |")
        lines.append("|------------|-------|-----------------|-----------|")
        for nl in content["newsletters"]:
            name = nl.get("name", "?")
            niche = nl.get("niche", "?")
            subs = f"{nl['estimated_subscribers']:,}" if nl.get("estimated_subscribers") else "N/A"
            paid = "Yes" if nl.get("has_paid_tier") else "No"
            lines.append(f"| {name} | {niche} | {subs} | {paid} |")
        lines.append("")

    # --- GAP ANALYSIS SECTION ---
    gaps = history.get("gaps", {})
    if gaps:
        lines.append("## Gap Analysis")
        lines.append("")

        if gaps.get("pricing_gaps"):
            lines.append("### Pricing Gaps")
            lines.append("")
            for pg in gaps["pricing_gaps"]:
                lines.append(f"**{pg['category']}**: {pg['free_count']} free / {pg['paid_count']} paid | "
                             f"Range: {pg['price_range']} | Avg: ${pg['avg_price']}")
                if pg.get("opportunity"):
                    lines.append(f"  - Opportunity: {pg['opportunity']}")
            lines.append("")

        if gaps.get("content_gaps"):
            cg = gaps["content_gaps"]
            lines.append("### Content Format Gaps")
            lines.append("")
            lines.append("Competitors actively use these formats we are missing:")
            lines.append("")
            for fmt in cg.get("priority_formats", []):
                lines.append(f"- {fmt.replace('_', ' ')}")
            lines.append("")

        if gaps.get("platform_gaps"):
            pg = gaps["platform_gaps"]
            lines.append("### Platform Gaps")
            lines.append("")
            lines.append("Platforms competitors are on that we are not:")
            lines.append("")
            for p in pg.get("missing", []):
                lines.append(f"- {p.replace('_', ' ')}")
            lines.append("")

        if gaps.get("feature_gaps"):
            lines.append("### Feature Gaps by Category")
            lines.append("")
            for fg in gaps["feature_gaps"]:
                lines.append(f"**{fg['category']}**:")
                lines.append(f"  Must-have: {', '.join(fg['must_have_features'])}")
                lines.append(f"  Nice-to-have: {', '.join(fg['nice_to_have_features'])}")
                if fg.get("competitor_feature_mentions"):
                    lines.append(f"  Recent competitor features: {'; '.join(fg['competitor_feature_mentions'][:5])}")
                lines.append("")

    # Summary stats
    lines.append("## Summary Stats")
    lines.append("")
    app_count = len(history.get("apps", {}))
    lines.append(f"- Apps tracked: {app_count}")
    lines.append(f"- App categories: {len(APP_CATEGORIES)}")
    lines.append(f"- Service categories: {len(SERVICE_CATEGORIES)}")
    lines.append(f"- Twitter accounts monitored: {len(COMPETITOR_TWITTER_ACCOUNTS)}")
    lines.append(f"- Gumroad stores tracked: {len(COMPETITOR_GUMROAD_STORES)}")
    lines.append(f"- Newsletters tracked: {len(COMPETITOR_NEWSLETTERS)}")
    lines.append(f"- Agencies monitored: {len(COMPETITOR_AGENCIES)}")
    lines.append("")

    report_text = "\n".join(lines)
    with open(report_path, "w") as f:
        f.write(report_text)

    log(f"\nReport written to {report_path}")
    return str(report_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="PrintMaxx Competitive Intelligence Engine - deep competitive analysis"
    )
    parser.add_argument("--scan-all", action="store_true", help="Run all scans (apps, services, content, gaps)")
    parser.add_argument("--apps", action="store_true", help="Scan app competitors only (iTunes API)")
    parser.add_argument("--services", action="store_true", help="Scan service pricing only (Fiverr/Upwork/Agencies)")
    parser.add_argument("--content", action="store_true", help="Scan content competitors only (Twitter/Gumroad/Substack)")
    parser.add_argument("--gaps", action="store_true", help="Run gap analysis on existing data")
    parser.add_argument("--report", action="store_true", help="Generate markdown report from latest data")
    parser.add_argument("--csv", action="store_true", help="Export to COMPETITIVE_INTEL.csv")

    args = parser.parse_args()

    if not any([args.scan_all, args.apps, args.services, args.content, args.gaps, args.report, args.csv]):
        parser.print_help()
        return

    history = load_history()

    log("=" * 60)
    log("PRINTMAXX COMPETITIVE INTELLIGENCE ENGINE")
    log(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    if args.scan_all or args.apps:
        history = scan_all_apps(history)
        save_history(history)

    if args.scan_all or args.services:
        history = scan_all_services(history)
        save_history(history)

    if args.scan_all or args.content:
        history = scan_all_content(history)
        save_history(history)

    if args.scan_all or args.gaps:
        history = run_gap_analysis(history)
        save_history(history)

    if args.scan_all or args.csv:
        write_intel_csv(history)

    if args.scan_all or args.report:
        report_path = generate_report(history)
        print(f"\nReport: {report_path}")

    log(f"\nDone. History saved to {HISTORY_FILE}")
    log(f"CSV: {INTEL_CSV}")


if __name__ == "__main__":
    main()
