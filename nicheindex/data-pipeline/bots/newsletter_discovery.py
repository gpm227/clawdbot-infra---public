#!/usr/bin/env python3
"""
newsletter_discovery.py — Discover newsletters from public directories.

Sources:
  1. Ghost Explore (server-rendered, 20+ categories, actual publication URLs)
  2. InboxReads API (name + vertical, no external URLs — queued for manual domain resolution)

Writes candidates to subscription_queue (status='candidate').
Posts top discoveries to Discord #discovery-queue.
Auto-triggers when queue depth < 500.

Usage:
  python3 newsletter_discovery.py                     # full run, all sources
  python3 newsletter_discovery.py --source ghost      # single source
  python3 newsletter_discovery.py --source inboxreads
  python3 newsletter_discovery.py --limit 100         # max candidates to write
  python3 newsletter_discovery.py --dry-run            # no DB writes, no Discord
  python3 newsletter_discovery.py --force              # run even if queue is healthy
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '') or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY required")
    sys.exit(1)

DB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
}

DISCORD_WEBHOOK = os.environ.get('DISCORD_WEBHOOK_DISCOVERY_QUEUE', '')
MAX_CANDIDATES_PER_RUN = 500
QUEUE_LOW_THRESHOLD = 500

HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize_domain(url_or_domain: str) -> str:
    """Extract clean domain from URL or domain string."""
    s = url_or_domain.strip().lower()
    if not s:
        return ''
    if not s.startswith('http'):
        s = 'https://' + s
    try:
        parsed = urlparse(s)
        domain = parsed.netloc or parsed.path.split('/')[0]
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return ''


def load_known_domains() -> set:
    """Load all domains already in subscription_queue + email_subscriptions."""
    known = set()

    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscription_queue",
        headers=DB_HEADERS,
        params={'select': 'domain'},
    )
    if resp.status_code == 200:
        for row in resp.json():
            known.add(row['domain'].lower().strip())

    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/email_subscriptions",
        headers=DB_HEADERS,
        params={'select': 'homepage_url'},
    )
    if resp.status_code == 200:
        for row in resp.json():
            url = row.get('homepage_url', '')
            if url:
                d = normalize_domain(url)
                if d:
                    known.add(d)

    print(f"  Loaded {len(known)} known domains for dedup")
    return known


def check_queue_depth() -> int:
    """Return count of pending + candidate items in subscription_queue."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscription_queue",
        headers={**DB_HEADERS, 'Prefer': 'count=exact'},
        params={'select': 'id', 'status': 'in.(candidate,pending)', 'limit': '0'},
    )
    if resp.status_code in (200, 206):
        count_header = resp.headers.get('content-range', '*/0')
        return int(count_header.split('/')[-1])
    return 0


# ---------------------------------------------------------------------------
# Source 1: Ghost Explore
# Fast: category pages are server-rendered, each card has a link to /p/slug
# which redirects to the actual publication URL.
# ---------------------------------------------------------------------------
GHOST_CATEGORIES = [
    'tech', 'business', 'news', 'culture', 'education', 'travel',
    'finance', 'entertainment', 'health', 'productivity', 'design',
    'science', 'food-drink', 'music', 'literature', 'climate',
    'personal', 'programming', 'art', 'sport',
]


def scrape_ghost_explore(known_domains: set, max_candidates: int) -> list:
    """Scrape Ghost Explore directory. Fast: just parse category listing pages."""
    candidates = []
    seen = set()

    for category in GHOST_CATEGORIES:
        if len(candidates) >= max_candidates:
            break

        url = f"https://explore.ghost.org/{category}"
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=15)
            if resp.status_code != 200:
                print(f"    {category}: HTTP {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Each publication card is an <a> linking to /p/[slug]
            cards = soup.select('a[href*="/p/"]')
            found_this_page = 0

            for card in cards:
                if len(candidates) >= max_candidates:
                    break

                href = card.get('href', '')
                if '/p/' not in href:
                    continue

                # Extract name
                name_el = card.select_one('h3, h2, h4')
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # Follow the /p/ link to get the actual domain
                # Ghost /p/ pages redirect or show the actual publication URL
                pub_url = href if href.startswith('http') else f"https://explore.ghost.org{href}"

                try:
                    # Use HEAD with redirect to find actual domain
                    detail = requests.get(pub_url, headers=HTTP_HEADERS, timeout=10, allow_redirects=True)
                    if detail.status_code != 200:
                        continue

                    detail_soup = BeautifulSoup(detail.text, 'html.parser')

                    # Look for "Visit site" or external link button
                    domain = ''

                    # Method 1: Look for explicit visit/website links
                    for a in detail_soup.select('a[href]'):
                        h = a.get('href', '')
                        if not h.startswith('http') or 'ghost.org' in h:
                            continue
                        text = a.get_text(strip=True).lower()
                        if any(kw in text for kw in ['visit', 'view site', 'go to', 'website']):
                            domain = normalize_domain(h)
                            break

                    # Method 2: og:url meta tag
                    if not domain:
                        og = detail_soup.select_one('meta[property="og:url"]')
                        if og:
                            content = og.get('content', '')
                            if content and 'ghost.org' not in content:
                                domain = normalize_domain(content)

                    # Method 3: First external link that's not social media
                    if not domain:
                        social = {'twitter.com', 'x.com', 'facebook.com', 'instagram.com',
                                  'linkedin.com', 'youtube.com', 'github.com', 'ghost.org'}
                        for a in detail_soup.select('a[href^="http"]'):
                            h = a.get('href', '')
                            d = normalize_domain(h)
                            if d and not any(s in d for s in social):
                                domain = d
                                break

                    time.sleep(0.2)  # Be polite
                except Exception:
                    continue

                if not domain:
                    continue

                if domain in known_domains or domain in seen:
                    continue
                seen.add(domain)

                candidates.append({
                    'name': name[:200],
                    'domain': domain,
                    'vertical': category.replace('-', ' ').title(),
                    'esp': 'ghost',
                    'source_url': url,
                })
                found_this_page += 1

            print(f"    {category}: {found_this_page} new")
            time.sleep(0.5)

        except Exception as e:
            print(f"    {category}: ERROR — {e}")
            continue

    print(f"  Ghost Explore total: {len(candidates)} new candidates")
    return candidates


# ---------------------------------------------------------------------------
# Source 2: InboxReads API
# Newsletter listings with name + topic. No external domain available.
# We store with domain='inboxreads:{url_name}' placeholder for manual resolution.
# ---------------------------------------------------------------------------
INBOXREADS_TOPICS = [
    'tech', 'business', 'marketing', 'ai', 'startup', 'culture',
    'news', 'finance', 'education', 'productivity', 'politics',
    'health', 'sports', 'design', 'science', 'entertainment',
]


def scrape_inboxreads(known_domains: set, max_candidates: int) -> list:
    """Scrape InboxReads via their API for newsletter names + verticals."""
    candidates = []
    seen_names = set()

    api_url = "https://api.inboxreads.co/newsletters"

    # Scrape trending pages
    for page in range(1, 30):
        if len(candidates) >= max_candidates:
            break

        try:
            resp = requests.get(api_url, headers=HTTP_HEADERS, timeout=10,
                                params={'page': page, 'limit': 20, 'sort': 'trending'})
            if resp.status_code != 200:
                break

            data = resp.json()
            if not data:
                break

            newsletters = data if isinstance(data, list) else data.get('newsletters', data.get('data', []))
            if not newsletters:
                break

            for nl in newsletters:
                if len(candidates) >= max_candidates:
                    break

                name = nl.get('name', '').strip()
                if not name or name.lower() in seen_names:
                    continue

                url_name = nl.get('url_name', '')
                topic = nl.get('topic', '')
                subs = nl.get('subscribers', 0)

                # We don't have the actual domain from InboxReads API
                # Use a placeholder domain that sponsor-collector can't process
                # These get flagged for manual domain resolution
                placeholder_domain = f"inboxreads--{url_name}"

                if placeholder_domain in known_domains:
                    continue

                seen_names.add(name.lower())

                candidates.append({
                    'name': name[:200],
                    'domain': placeholder_domain,
                    'vertical': topic.title() if topic else None,
                    'esp': None,
                    'source_url': f"https://inboxreads.co/n/{url_name}",
                })

            time.sleep(0.5)

        except Exception as e:
            print(f"    InboxReads page {page}: ERROR — {e}")
            break

    print(f"  InboxReads total: {len(candidates)} new candidates (need domain resolution)")
    return candidates


# ---------------------------------------------------------------------------
# Write to subscription_queue
# ---------------------------------------------------------------------------
def write_candidates(candidates: list, batch_label: str, dry_run: bool) -> int:
    """Insert candidates into subscription_queue. Returns count written."""
    if dry_run:
        print(f"  DRY RUN: would write {len(candidates)} candidates")
        return len(candidates)

    written = 0
    for c in candidates:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/subscription_queue",
            headers={**DB_HEADERS, 'Prefer': 'return=minimal'},
            json={
                'name': c['name'],
                'domain': c['domain'],
                'vertical': c.get('vertical'),
                'esp': c.get('esp'),
                'source_url': c.get('source_url'),
                'status': 'candidate',
                'batch': batch_label,
            },
        )
        if resp.status_code in (200, 201):
            written += 1
        elif resp.status_code == 409 or 'duplicate' in (resp.text or '').lower():
            pass  # Domain already exists
        else:
            print(f"    DB ERROR for {c['domain']}: {resp.status_code}")

    return written


# ---------------------------------------------------------------------------
# Discord posting
# ---------------------------------------------------------------------------
def post_to_discord(candidates: list, batch_label: str, queue_depth: int):
    """Post top discoveries to Discord #discovery-queue."""
    if not DISCORD_WEBHOOK:
        print("  No Discord webhook configured, skipping")
        return

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Split by source
    ghost = [c for c in candidates if c.get('esp') == 'ghost']
    inboxreads = [c for c in candidates if 'inboxreads--' in c.get('domain', '')]

    lines = [f"**Newsletter Discovery** — {now}\n"]
    lines.append(f"Queue: {queue_depth} items | New: {len(candidates)} candidates\n")

    if ghost:
        lines.append(f"**Ghost Explore** ({len(ghost)} found)")
        for c in ghost[:8]:
            lines.append(f"  • {c['name']} — `{c['domain']}` ({c.get('vertical', '?')})")
        if len(ghost) > 8:
            lines.append(f"  ... +{len(ghost) - 8} more")
        lines.append("")

    if inboxreads:
        lines.append(f"**InboxReads** ({len(inboxreads)} found, need domain resolution)")
        for c in inboxreads[:5]:
            lines.append(f"  • {c['name']} ({c.get('vertical', '?')})")
        if len(inboxreads) > 5:
            lines.append(f"  ... +{len(inboxreads) - 5} more")

    message = '\n'.join(lines)

    chunks = [message[i:i+1990] for i in range(0, len(message), 1990)]
    for chunk in chunks:
        payload = json.dumps({"content": chunk}).encode()
        req = urllib.request.Request(DISCORD_WEBHOOK, data=payload,
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "NicheIndex-Bot/1.0"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"  Discord failed: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(args):
    now = datetime.now(timezone.utc)
    batch_label = f"discovery-{now.strftime('%Y-%m-%d')}"

    queue_depth = check_queue_depth()
    print(f"\nQueue depth: {queue_depth} (pending + candidates)")

    if queue_depth >= QUEUE_LOW_THRESHOLD and not args.force:
        print(f"Queue healthy ({queue_depth} >= {QUEUE_LOW_THRESHOLD}). Use --force to run anyway.")
        return

    known_domains = load_known_domains()

    all_candidates = []
    remaining = args.limit or MAX_CANDIDATES_PER_RUN

    sources = {
        'ghost': scrape_ghost_explore,
        'inboxreads': scrape_inboxreads,
    }

    if args.source:
        if args.source not in sources:
            print(f"Unknown source: {args.source}. Options: {list(sources.keys())}")
            return
        sources = {args.source: sources[args.source]}

    for source_name, scraper_fn in sources.items():
        if remaining <= 0:
            break
        print(f"\nScraping {source_name}...")
        candidates = scraper_fn(known_domains, remaining)
        for c in candidates:
            known_domains.add(c['domain'])
        all_candidates.extend(candidates)
        remaining -= len(candidates)

    print(f"\nTotal new candidates: {len(all_candidates)}")

    if not all_candidates:
        print("No new candidates found.")
        return

    written = write_candidates(all_candidates, batch_label, args.dry_run)
    print(f"Written: {written}")

    if not args.dry_run:
        new_depth = check_queue_depth()
        post_to_discord(all_candidates, batch_label, new_depth)

    # Summary
    by_source = {}
    for c in all_candidates:
        src = 'ghost' if c.get('esp') == 'ghost' else 'inboxreads'
        by_source[src] = by_source.get(src, 0) + 1

    print("\n--- Summary ---")
    for src, count in sorted(by_source.items()):
        print(f"  {src}: {count}")
    print(f"  Total: {len(all_candidates)} found, {written} written")


def main():
    parser = argparse.ArgumentParser(description='Discover newsletters from public directories')
    parser.add_argument('--dry-run', action='store_true', help='No DB writes or Discord posts')
    parser.add_argument('--source', choices=['ghost', 'inboxreads'], help='Scrape single source')
    parser.add_argument('--limit', type=int, help=f'Max candidates (default: {MAX_CANDIDATES_PER_RUN})')
    parser.add_argument('--force', action='store_true', help='Run even if queue is healthy')
    args = parser.parse_args()

    run(args)


if __name__ == '__main__':
    main()
