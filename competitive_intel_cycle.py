#!/usr/bin/env python3

from __future__ import annotations
"""
Competitive Intel Cycle - CLEAN + ANALYZE + STORE + ALERT
Runs as part of the Competitive Intel autonomy venture (every 2h).
Processes output from:
  - competitor_monitor.py --scan  (App Store version changes)
  - competitive_intelligence_engine.py --apps (App Store ratings/pricing)
  - reddit_deep_scraper.py --daily (Reddit alpha signals)
Outputs:
  - LEDGER/COMPETITIVE_INTEL.csv (new rows appended)
  - LEDGER/COMPETITOR_CHANGES.csv (version changes + alerts)
  - AUTOMATIONS/logs/competitive_intel_cycle.log
  - AUTOMATIONS/agent/autonomy/results/auto_scraping_competitive_intel_9788/cycle_NNN.json
"""

import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEDGER = PROJECT_ROOT / "LEDGER"
LOGS = PROJECT_ROOT / "AUTOMATIONS" / "logs"
RESULTS_DIR = PROJECT_ROOT / "AUTOMATIONS" / "agent" / "autonomy" / "results" / "auto_scraping_competitive_intel_9788"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

COMPETITIVE_INTEL_CSV = LEDGER / "COMPETITIVE_INTEL.csv"
COMPETITOR_CHANGES_CSV = LEDGER / "COMPETITOR_CHANGES.csv"
ALPHA_STAGING_CSV = LEDGER / "ALPHA_STAGING.csv"
COMPETITOR_HISTORY = PROJECT_ROOT / "AUTOMATIONS" / "logs" / "competitor_history.json"
ALPHA_STAGING_REDDIT = sorted((PROJECT_ROOT / "AUTOMATIONS" / "reddit_scraper_output").glob("reddit_*.json"))

CYCLE_LOG = LOGS / "competitive_intel_cycle.log"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(CYCLE_LOG, "a") as f:
        f.write(line + "\n")

def safe_path(p):
    r = Path(p).resolve()
    if not str(r).startswith(str(PROJECT_ROOT)):
        raise ValueError(f"BLOCKED: {r}")
    return r

# ── PHASE 1: CLEAN ──────────────────────────────────────────────────────────

def load_existing_intel_keys():
    """Load set of dedup keys from existing rows. Matches both URL-based and name-based keys."""
    keys = set()
    if COMPETITIVE_INTEL_CSV.exists():
        with open(COMPETITIVE_INTEL_CSV, newline='', errors='replace') as f:
            for row in csv.DictReader(f):
                # Name-based key (original format)
                keys.add(f"{row.get('type','')}__{row.get('name','')}__{(row.get('scan_date') or '')[:10]}")
                # URL-based key (matches store_intel_rows writer key)
                url = row.get('url', '')
                if url:
                    keys.add(f"reddit_signal__{url}__{(row.get('scan_date') or '')[:10]}")
    return keys

def load_competitor_changes():
    """Read competitor_history.json and extract version changes since last cycle."""
    if not COMPETITOR_HISTORY.exists():
        return []
    with open(COMPETITOR_HISTORY) as f:
        data = json.load(f)
    changes = []
    today = datetime.now().strftime("%Y-%m-%d")
    for app_key, app_data in data.items():
        # Structure: {app_key: {'name':..., 'niche':..., 'snapshots':[...]}}
        snapshots = app_data.get('snapshots', []) if isinstance(app_data, dict) else app_data
        if len(snapshots) < 2:
            continue
        latest = snapshots[-1]
        prev = snapshots[-2]
        v_new = latest.get('version') or ''
        v_old = prev.get('version') or ''
        if v_new == v_old:
            continue
        scan_date = latest.get('scan_timestamp') or latest.get('lastScanDate') or latest.get('scan_date', '')
        if not scan_date.startswith(today):
            continue
        name = app_data.get('name', app_key) if isinstance(app_data, dict) else app_key
        category = app_data.get('niche') or latest.get('category', '?')
        changes.append({
            'app': name,
            'from_version': v_old,
            'to_version': v_new,
            'price': latest.get('formattedPrice') or latest.get('price', 'Free'),
            'rating': latest.get('averageUserRating') or latest.get('rating', ''),
            'ratings_count': latest.get('userRatingCount') or latest.get('ratings_count', ''),
            'category': category,
            'scan_date': scan_date or datetime.now().isoformat(),
        })
    return changes

def _is_alpha_format(post):
    """Detect whether a post dict is alpha-processed format vs raw Reddit post."""
    return 'alpha_id' in post or 'tactic' in post

def load_reddit_top_signals():
    """Load ALL Reddit scrapes from today and return top competitive signals.

    Handles two formats:
    - Raw Reddit post: keys = title, score, selftext, num_comments, subreddit, url, post_id
    - Alpha-processed: keys = alpha_id, tactic, category, roi_potential, source, source_url, notes
    """
    if not ALPHA_STAGING_REDDIT:
        return []
    # Load ALL files from today, not just the latest — signals spread across multiple runs
    today = datetime.now().strftime("%Y%m%d")
    today_files = [f for f in ALPHA_STAGING_REDDIT if today in f.name]
    files_to_load = today_files if today_files else [ALPHA_STAGING_REDDIT[-1]]
    posts = []
    seen_ids = set()
    for fp in files_to_load:
        with open(fp, errors='replace') as f:
            batch = json.load(f)
        for item in batch:
            uid = item.get('alpha_id') or item.get('post_id') or item.get('source_url', '')
            if uid not in seen_ids:
                seen_ids.add(uid)
                posts.append(item)
    competitive_keywords = [
        'competitor', 'alternative', 'vs ', 'versus', 'compared to', 'pricing',
        'subscription', 'revenue', 'MRR', 'ARR', 'raised', 'funding', 'launch',
        'launched', 'new app', 'built this', 'shipped', 'open source', 'free tier',
        'pivot', 'acquired', 'shut down', 'dead', 'discontinued'
    ]
    signals = []
    for post in posts:
        if _is_alpha_format(post):
            # Alpha-processed format from background_reddit_scraper.py
            tactic = (post.get('tactic') or '').lower()
            notes = (post.get('notes') or '').lower()
            category = (post.get('category') or '').lower()
            combined = tactic + ' ' + notes + ' ' + category
            keyword_hits = sum(1 for kw in competitive_keywords if kw in combined)
            roi_raw = post.get('roi_potential', 'MEDIUM')
            # Map roi_potential directly; require at least 1 keyword OR HIGH+ roi
            roi = roi_raw if roi_raw in ('HIGHEST', 'HIGH') else 'MEDIUM'
            if keyword_hits >= 1 or roi_raw in ('HIGHEST', 'HIGH'):
                source = post.get('source', '')
                url = post.get('source_url', '')
                post_id = post.get('alpha_id', url)
                # Extract score from notes field ("Score: 60, Comments: 50")
                score_match = re.search(r'Score:\s*(\d+)', post.get('notes', ''))
                comments_match = re.search(r'Comments:\s*(\d+)', post.get('notes', ''))
                score = int(score_match.group(1)) if score_match else 0
                comments = int(comments_match.group(1)) if comments_match else 0
                # Recalculate roi based on keywords + score now that we have them
                if keyword_hits >= 3 or score >= 500 or roi_raw == 'HIGHEST':
                    roi = 'HIGHEST'
                elif keyword_hits >= 2 or score >= 100 or roi_raw == 'HIGH':
                    roi = 'HIGH'
                signals.append({
                    'title': post.get('tactic', '')[:120],
                    'subreddit': source.lstrip('r/').split('/')[0] if source.startswith('r/') else source,
                    'score': score,
                    'comments': comments,
                    'url': url,
                    'keyword_hits': keyword_hits,
                    'roi': roi,
                    'post_id': post_id,
                })
        else:
            # Raw Reddit post format
            title = (post.get('title') or '').lower()
            text = (post.get('selftext') or '').lower()
            combined = title + ' ' + text
            keyword_hits = sum(1 for kw in competitive_keywords if kw in combined)
            score = post.get('score', 0)
            comments = post.get('num_comments', 0)
            if keyword_hits >= 1 and (score >= 50 or comments >= 5):
                roi = 'HIGHEST' if (keyword_hits >= 3 or score >= 500) else 'HIGH' if (keyword_hits >= 2 or score >= 100) else 'MEDIUM'
                signals.append({
                    'title': post.get('title', '')[:120],
                    'subreddit': post.get('subreddit', ''),
                    'score': score,
                    'comments': comments,
                    'url': f"https://reddit.com{post.get('url','')}" if not post.get('url','').startswith('http') else post.get('url',''),
                    'keyword_hits': keyword_hits,
                    'roi': roi,
                    'post_id': post.get('post_id', ''),
                })
    signals.sort(key=lambda x: (x['roi'] == 'HIGHEST', x['score']), reverse=True)
    return signals[:20]

# ── PHASE 2: ANALYZE ────────────────────────────────────────────────────────

def score_version_change(change):
    """Score how significant a version change is for competitive intel."""
    v_old = change['from_version'] or ''
    v_new = change['to_version'] or ''
    # Count segment jumps
    def parse_version(v):
        parts = re.findall(r'\d+', v)
        return [int(p) for p in parts] if parts else [0]
    old_parts = parse_version(v_old)
    new_parts = parse_version(v_new)
    # Major version jump = HIGHEST, minor = HIGH, patch = MEDIUM
    if len(old_parts) >= 1 and len(new_parts) >= 1:
        if new_parts[0] > old_parts[0]:
            return 'HIGHEST', 'Major version bump - likely significant feature release'
        if len(old_parts) >= 2 and len(new_parts) >= 2:
            diff = new_parts[1] - old_parts[1]
            if diff >= 5:
                return 'HIGH', f'Large minor jump (+{diff}) - rapid iteration signal'
            if diff >= 2:
                return 'HIGH', f'Minor jump (+{diff}) - active development'
            return 'MEDIUM', 'Small update'
    return 'MEDIUM', 'Version change detected'

def analyze_app_rating_context(change):
    """Assess competitive threat from app ratings."""
    try:
        rating = float(change.get('rating') or 0)
        count = int((change.get('ratings_count') or '0').replace(',', ''))
    except (ValueError, AttributeError):
        return 'UNKNOWN'
    if rating >= 4.8 and count >= 50000:
        return 'TOP_TIER_THREAT'
    if rating >= 4.5 and count >= 10000:
        return 'STRONG_COMPETITOR'
    if rating >= 4.0 and count >= 1000:
        return 'MODERATE_COMPETITOR'
    return 'WEAK_COMPETITOR'

# ── PHASE 3: STORE ───────────────────────────────────────────────────────────

def ensure_competitor_changes_csv():
    if not COMPETITOR_CHANGES_CSV.exists():
        with open(COMPETITOR_CHANGES_CSV, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['scan_date','app','category','from_version','to_version',
                        'price','rating','ratings_count','roi_score','threat_level',
                        'analysis_note','source'])

def store_version_changes(changes, existing_keys):
    """Append new version change rows to COMPETITOR_CHANGES_CSV."""
    ensure_competitor_changes_csv()
    new_rows = 0
    with open(COMPETITOR_CHANGES_CSV, 'a', newline='') as f:
        w = csv.writer(f)
        for c in changes:
            key = f"version_change__{c['app']}__{c['scan_date'][:10]}"
            if key in existing_keys:
                continue
            roi, note = score_version_change(c)
            threat = analyze_app_rating_context(c)
            w.writerow([
                c['scan_date'], c['app'], c['category'],
                c['from_version'], c['to_version'],
                c['price'], c['rating'], c['ratings_count'],
                roi, threat, note, 'competitor_monitor'
            ])
            existing_keys.add(key)
            new_rows += 1
    return new_rows

def store_intel_rows(changes, reddit_signals, existing_keys):
    """Append processed intel to COMPETITIVE_INTEL.csv."""
    today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    header_needed = not COMPETITIVE_INTEL_CSV.exists()
    new_rows = 0
    with open(COMPETITIVE_INTEL_CSV, 'a', newline='') as f:
        w = csv.writer(f)
        if header_needed:
            w.writerow(['type','category','name','price','rating','rating_count',
                        'version','last_updated','positive_sentiment','negative_sentiment',
                        'source','url','metric_1','metric_2','notes','scan_date'])
        # Store Reddit signals as intel rows
        for sig in reddit_signals:
            # Use URL as the stable dedup key (matches load_existing_intel_keys)
            key = f"reddit_signal__{sig['url']}__{today[:10]}"
            if key in existing_keys:
                continue
            w.writerow([
                'reddit_signal', 'competitive_intel', sig['title'][:80],
                '', '', sig['score'], '', today,
                sig['keyword_hits'], sig['comments'],
                f"reddit/{sig['subreddit']}", sig['url'],
                sig['roi'], sig['keyword_hits'],
                f"r/{sig['subreddit']} score:{sig['score']} comments:{sig['comments']}",
                today
            ])
            existing_keys.add(key)
            new_rows += 1
    return new_rows

# ── PHASE 4: ALERT ───────────────────────────────────────────────────────────

def generate_alerts(changes, reddit_signals):
    """Build alert payload for high-value findings."""
    alerts = []

    # App version change alerts
    for c in changes:
        roi, note = score_version_change(c)
        threat = analyze_app_rating_context(c)
        if roi in ('HIGHEST', 'HIGH') or threat == 'TOP_TIER_THREAT':
            alerts.append({
                'type': 'VERSION_CHANGE',
                'priority': roi,
                'app': c['app'],
                'category': c['category'],
                'change': f"{c['from_version']} -> {c['to_version']}",
                'threat_level': threat,
                'note': note,
                'action': f"Check {c['app']} App Store page for new features. Update gap analysis.",
            })

    # Reddit competitive signal alerts
    for sig in reddit_signals[:5]:
        if sig['roi'] in ('HIGHEST', 'HIGH'):
            alerts.append({
                'type': 'REDDIT_SIGNAL',
                'priority': sig['roi'],
                'title': sig['title'],
                'subreddit': sig['subreddit'],
                'score': sig['score'],
                'url': sig['url'],
                'action': 'Review for competitor pricing/feature intelligence.',
            })

    return alerts

# ── MAIN CYCLE ───────────────────────────────────────────────────────────────

def main():
    cycle_start = datetime.now().isoformat()
    log("=" * 60)
    log("COMPETITIVE INTEL CYCLE START")
    log("=" * 60)

    # CLEAN
    log("[CLEAN] Loading existing intel keys for dedup...")
    existing_keys = load_existing_intel_keys()
    # Also load COMPETITOR_CHANGES keys
    if COMPETITOR_CHANGES_CSV.exists():
        with open(COMPETITOR_CHANGES_CSV, newline='', errors='replace') as f:
            for row in csv.DictReader(f):
                key = f"version_change__{row.get('app','')}__{row.get('scan_date','')[:10]}"
                existing_keys.add(key)
    log(f"[CLEAN] {len(existing_keys)} existing keys loaded")

    log("[CLEAN] Loading competitor version changes...")
    changes = load_competitor_changes()
    log(f"[CLEAN] {len(changes)} version changes found today")

    log("[CLEAN] Loading Reddit competitive signals...")
    reddit_signals = load_reddit_top_signals()
    log(f"[CLEAN] {len(reddit_signals)} Reddit signals scored")

    # ANALYZE
    log("[ANALYZE] Scoring version changes...")
    scored = []
    for c in changes:
        roi, note = score_version_change(c)
        threat = analyze_app_rating_context(c)
        log(f"  {c['app']}: {roi} | {threat} | {note}")
        scored.append({**c, 'roi': roi, 'threat': threat, 'note': note})

    log("[ANALYZE] Top Reddit signals:")
    for sig in reddit_signals[:5]:
        log(f"  [{sig['roi']}] r/{sig['subreddit']} score:{sig['score']} | {sig['title'][:70]}")

    # STORE
    log("[STORE] Appending version changes to COMPETITOR_CHANGES.csv...")
    n1 = store_version_changes(changes, existing_keys)
    log(f"[STORE] {n1} new version change rows written")

    log("[STORE] Appending signals to COMPETITIVE_INTEL.csv...")
    n2 = store_intel_rows(changes, reddit_signals, existing_keys)
    log(f"[STORE] {n2} new intel rows written")

    # ALERT
    log("[ALERT] Generating alerts...")
    alerts = generate_alerts(changes, reddit_signals)
    log(f"[ALERT] {len(alerts)} alerts generated")
    for a in alerts:
        priority_marker = "!!" if a['priority'] == 'HIGHEST' else "!"
        log(f"  [{priority_marker}] {a['type']} | {a.get('app', a.get('title',''))[:50]} | {a.get('action','')[:60]}")

    # Write cycle result
    result = {
        'cycle_start': cycle_start,
        'cycle_end': datetime.now().isoformat(),
        'version_changes_detected': len(changes),
        'reddit_signals_found': len(reddit_signals),
        'new_intel_rows_written': n1 + n2,
        'alerts_generated': len(alerts),
        'alerts': alerts,
        'scored_changes': scored,
        'top_reddit_signals': reddit_signals[:5],
    }
    # Find cycle number
    existing_cycles = sorted(RESULTS_DIR.glob("cycle_*.json"))
    cycle_num = len(existing_cycles) + 1
    cycle_file = RESULTS_DIR / f"cycle_{cycle_num:03d}.json"
    with open(safe_path(cycle_file), 'w') as f:
        json.dump(result, f, indent=2, default=str)
    log(f"[DONE] Cycle {cycle_num} result saved: {cycle_file}")
    log(f"[DONE] Total new rows: {n1 + n2} | Alerts: {len(alerts)}")

    return result

if __name__ == '__main__':
    main()
