#!/usr/bin/env python3
"""
newsletter_discovery.py — Discover newsletters from public directories.

Sources (priority order):
  1. InboxReads (server-rendered, paginated, 168+ pages)
  2. Ghost Explore (server-rendered, category pages with infinite-scroll JSON)

Writes candidates to subscription_queue (status='candidate').
Posts top discoveries to Discord #discovery-queue.
Auto-triggers when queue depth < 500.

Usage:
  python3 newsletter_discovery.py                  # full run, all sources
  python3 newsletter_discovery.py --source inboxreads  # single source
  python3 newsletter_discovery.py --source ghost
  python3 newsletter_discovery.py --limit 100      # max candidates to write
  python3 newsletter_discovery.py --dry-run         # no DB writes, no Discord
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent.parent.parent.parent / '.env')

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_SERVICE_ROLE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY required")
    sys.exit(1)

DB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
}

DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK_DISCOVERY_QUEUE', '')
MAX_CANDIDATES_PER_RUN = 500
QUEUE_LOW_THRESHOLD = 500

HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}


# ---------------------------------------------------------------------------
# Dedup: load existing domains from subscription_queue + email_subscriptions
# ---------------------------------------------------------------------------
def load_known_domains() -> set:
    """Load all domains we already know about."""
    known = set()

    # subscription_queue domains
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscription_queue",
        headers=DB_HEADERS,
        params={'select': 'domain'},
    )
    if resp.status_code == 200:
        for row in resp.json():
            known.add(row['domain'].lower().strip())

    # email_subscriptions homepage_url → domain
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/email_subscriptions",
        headers=DB_HEADERS,
        params={'select': 'homepage_url'},
    )
    if resp.status_code == 200:
        for row in resp.json():
            url = row.get('homepage_url', '')
            if url:
                try:
                    d = urlparse(url).netloc.lower()
                    if d:
                        known.add(d)
                except Exception:
                    pass

    print(f"  Loaded {len(known)} known domains for dedup")
    return known


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
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return ''


def check_queue_depth() -> int:
    """Return count of pending items in subscription_queue."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscription_queue",
        headers={**DB_HEADERS, 'Prefer': 'count=exact'},
        params={'select': 'id', 'status': 'in.(candidate,pending)', 'limit': '0'},
    )
    if resp.status_code == 200:
        count = resp.headers.get('content-range', '*/0').split('/')[-1]
        return int(count)
    return 0


# ---------------------------------------------------------------------------
# Source 1: InboxReads
# ---------------------------------------------------------------------------
INBOXREADS_CATEGORIES = [
    'tech', 'business', 'marketing', 'ai', 'startup', 'culture',
    'news', 'finance', 'education', 'productivity', 'politics',
    'development', 'health', 'sports', 'writing', 'design',
    'science', 'lifestyle', 'leadership', 'psychology', 'entertainment',
    'cryptocurrency', 'data-science', 'climate-change', 'space',
]


def scrape_inboxreads(known_domains: set, max_candidates: int) -> list:
    """Scrape InboxReads trending + category pages."""
    candidates = []
    seen_domains = set()

    # Scrape trending pages first (highest quality signal)
    pages_to_scrape = [f"https://inboxreads.co/trending?page={p}" for p in range(1, 20)]

    # Then category "best" pages
    for cat in INBOXREADS_CATEGORIES:
        pages_to_scrape.append(f"https://inboxreads.co/{cat}")

    for url in pages_to_scrape:
        if len(candidates) >= max_candidates:
            break

        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Each newsletter is in a .nl card
            cards = soup.select('.nl')
            if not cards:
                # Try alternate selectors
                cards = soup.select('[class*="newsletter"]')

            for card in cards:
                if len(candidates) >= max_candidates:
                    break

                # Extract name
                name_el = card.select_one('.nl-title a, .nl-name a, h3 a, h2 a')
                if not name_el:
                    name_el = card.select_one('a[href*="/n/"]')
                if not name_el:
                    continue

                name = name_el.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # Extract link to newsletter page
                href = name_el.get('href', '')
                if not href:
                    continue

                # Extract description
                desc_el = card.select_one('.nl-description, .nl-tagline, p')
                description = desc_el.get_text(strip=True) if desc_el else ''

                # We need to visit the newsletter page to get the actual domain
                # For now, use the InboxReads slug as a placeholder
                # The actual homepage URL is on the individual newsletter page
                newsletter_url = href if href.startswith('http') else f"https://inboxreads.co{href}"

                # Try to extract domain from the card if available
                domain = ''
                link_els = card.select('a[href]')
                for link in link_els:
                    h = link.get('href', '')
                    if h and not 'inboxreads.co' in h and h.startswith('http'):
                        domain = normalize_domain(h)
                        break

                # If no external domain found, we'll need to fetch the newsletter page
                if not domain and '/n/' in newsletter_url:
                    try:
                        detail_resp = requests.get(newsletter_url, headers=HTTP_HEADERS, timeout=10)
                        if detail_resp.status_code == 200:
                            detail_soup = BeautifulSoup(detail_resp.text, 'html.parser')
                            # Look for "Visit" or external link
                            for a in detail_soup.select('a[href]'):
                                h = a.get('href', '')
                                text = a.get_text(strip=True).lower()
                                if h.startswith('http') and 'inboxreads.co' not in h:
                                    if any(w in text for w in ['visit', 'website', 'subscribe', 'home']):
                                        domain = normalize_domain(h)
                                        break
                            # Fallback: any external link in the main content
                            if not domain:
                                for a in detail_soup.select('.nl-website a, .nl-link a, a[rel="nofollow"]'):
                                    h = a.get('href', '')
                                    if h.startswith('http') and 'inboxreads.co' not in h:
                                        domain = normalize_domain(h)
                                        break
                        time.sleep(0.5)  # Be polite
                    except Exception:
                        pass

                if not domain:
                    continue

                # Dedup
                if domain in known_domains or domain in seen_domains:
                    continue
                seen_domains.add(domain)

                # Determine vertical from URL/category
                vertical = ''
                for cat in INBOXREADS_CATEGORIES:
                    if cat in url:
                        vertical = cat.replace('-', ' ').title()
                        break

                candidates.append({
                    'name': name[:200],
                    'domain': domain,
                    'vertical': vertical or None,
                    'esp': None,  # Unknown from InboxReads
                    'source_url': url,
                })

            time.sleep(1)  # Rate limit between pages

        except Exception as e:
            print(f"  ERROR scraping {url}: {e}")
            continue

    print(f"  InboxReads: {len(candidates)} new candidates")
    return candidates


# ---------------------------------------------------------------------------
# Source 2: Ghost Explore
# ---------------------------------------------------------------------------
GHOST_CATEGORIES = [
    'tech', 'business', 'news', 'culture', 'education', 'travel',
    'finance', 'entertainment', 'health', 'productivity', 'design',
    'science', 'food-drink', 'music', 'literature', 'climate',
    'personal', 'programming', 'art', 'sport',
]


def scrape_ghost_explore(known_domains: set, max_candidates: int) -> list:
    """Scrape Ghost Explore directory pages."""
    candidates = []
    seen_domains = set()

    for category in GHOST_CATEGORIES:
        if len(candidates) >= max_candidates:
            break

        url = f"https://explore.ghost.org/{category}"
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Ghost Explore uses <a> cards linking to /p/[slug]
            cards = soup.select('a[href*="/p/"]')

            for card in cards:
                if len(candidates) >= max_candidates:
                    break

                href = card.get('href', '')
                if not href or '/p/' not in href:
                    continue

                # Extract name from h3 or h2 inside the card
                name_el = card.select_one('h3, h2')
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # We need to visit the publication page to get the actual domain
                pub_url = href if href.startswith('http') else f"https://explore.ghost.org{href}"

                try:
                    detail_resp = requests.get(pub_url, headers=HTTP_HEADERS, timeout=10, allow_redirects=True)
                    if detail_resp.status_code == 200:
                        detail_soup = BeautifulSoup(detail_resp.text, 'html.parser')

                        # Look for the actual publication URL
                        domain = ''
                        # Ghost explore pages typically have a "Visit" link
                        for a in detail_soup.select('a[href]'):
                            h = a.get('href', '')
                            text = a.get_text(strip=True).lower()
                            if h.startswith('http') and 'ghost.org' not in h and 'explore.ghost' not in h:
                                if any(w in text for w in ['visit', 'view', 'website', 'go to']):
                                    domain = normalize_domain(h)
                                    break
                        # Fallback: look for canonical or og:url
                        if not domain:
                            canonical = detail_soup.select_one('link[rel="canonical"]')
                            if canonical:
                                canon_url = canonical.get('href', '')
                                if 'ghost.org' not in canon_url:
                                    domain = normalize_domain(canon_url)
                        if not domain:
                            og_url = detail_soup.select_one('meta[property="og:url"]')
                            if og_url:
                                og = og_url.get('content', '')
                                if 'ghost.org' not in og:
                                    domain = normalize_domain(og)
                        # Last resort: any external link
                        if not domain:
                            for a in detail_soup.select('a[href^="http"]'):
                                h = a.get('href', '')
                                if 'ghost.org' not in h and 'twitter.com' not in h and 'facebook.com' not in h:
                                    domain = normalize_domain(h)
                                    break

                    time.sleep(0.3)
                except Exception:
                    continue

                if not domain:
                    continue

                # Dedup
                if domain in known_domains or domain in seen_domains:
                    continue
                seen_domains.add(domain)

                vertical = category.replace('-', ' ').title()

                candidates.append({
                    'name': name[:200],
                    'domain': domain,
                    'vertical': vertical,
                    'esp': 'ghost',
                    'source_url': url,
                })

            time.sleep(1)

        except Exception as e:
            print(f"  ERROR scraping Ghost {category}: {e}")
            continue

    print(f"  Ghost Explore: {len(candidates)} new candidates")
    return candidates


# ---------------------------------------------------------------------------
# Write candidates to subscription_queue
# ---------------------------------------------------------------------------
def write_candidates(candidates: list, batch_label: str, dry_run: bool) -> int:
    """Insert candidates into subscription_queue. Returns count written."""
    if dry_run:
        print(f"  DRY RUN: would write {len(candidates)} candidates")
        return 0

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
        elif resp.status_code == 409:
            pass  # Domain already exists (unique constraint)
        else:
            print(f"  DB ERROR for {c['domain']}: {resp.status_code} {resp.text[:100]}")

    return written


# ---------------------------------------------------------------------------
# Discord posting
# ---------------------------------------------------------------------------
def post_to_discord(candidates: list, batch_label: str, queue_depth: int):
    """Post top discoveries to Discord #discovery-queue."""
    if not DISCORD_WEBHOOK:
        print("  No Discord webhook configured, skipping post")
        return

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = [f"**Newsletter Discovery** — {now} ({batch_label})\n"]
    lines.append(f"Queue depth: {queue_depth} (pending + candidates)")
    lines.append(f"New candidates: {len(candidates)}\n")

    # Show top 10 by source
    shown = 0
    for c in candidates[:10]:
        esp_tag = f" [{c['esp']}]" if c.get('esp') else ""
        vert_tag = f" ({c['vertical']})" if c.get('vertical') else ""
        lines.append(f"  • **{c['name']}** — {c['domain']}{vert_tag}{esp_tag}")
        shown += 1

    if len(candidates) > 10:
        lines.append(f"  ... and {len(candidates) - 10} more")

    message = '\n'.join(lines)

    # Discord max 2000 chars
    import urllib.request
    chunks = [message[i:i+1990] for i in range(0, len(message), 1990)]
    for chunk in chunks:
        payload = json.dumps({"content": chunk}).encode()
        req = urllib.request.Request(DISCORD_WEBHOOK, data=payload,
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "NicheIndex-Bot/1.0"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"  Discord post failed: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(args):
    now = datetime.now(timezone.utc)
    batch_label = f"discovery-{now.strftime('%Y-%m-%d')}"

    # Check queue depth
    queue_depth = check_queue_depth()
    print(f"\nQueue depth: {queue_depth} (pending + candidates)")

    if queue_depth >= QUEUE_LOW_THRESHOLD and not args.force:
        print(f"Queue is healthy ({queue_depth} >= {QUEUE_LOW_THRESHOLD}). Use --force to run anyway.")
        return

    # Load known domains for dedup
    known_domains = load_known_domains()

    all_candidates = []
    remaining = args.limit or MAX_CANDIDATES_PER_RUN

    sources = {
        'inboxreads': scrape_inboxreads,
        'ghost': scrape_ghost_explore,
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
        # Add to known so next source deduplicates
        for c in candidates:
            known_domains.add(c['domain'])
        all_candidates.extend(candidates)
        remaining -= len(candidates)

    print(f"\nTotal new candidates: {len(all_candidates)}")

    if not all_candidates:
        print("No new candidates found.")
        return

    # Write to DB
    written = write_candidates(all_candidates, batch_label, args.dry_run)
    print(f"Written to subscription_queue: {written}")

    # Post to Discord
    if not args.dry_run:
        new_depth = check_queue_depth()
        post_to_discord(all_candidates, batch_label, new_depth)

    # Summary
    by_source = {}
    for c in all_candidates:
        src = 'inboxreads' if 'inboxreads' in (c.get('source_url') or '') else 'ghost'
        by_source[src] = by_source.get(src, 0) + 1

    print("\n--- Summary ---")
    for src, count in by_source.items():
        print(f"  {src}: {count}")
    print(f"  Total: {len(all_candidates)} found, {written} written")


def main():
    parser = argparse.ArgumentParser(description='Discover newsletters from public directories')
    parser.add_argument('--dry-run', action='store_true', help='No DB writes or Discord posts')
    parser.add_argument('--source', choices=['inboxreads', 'ghost'], help='Scrape single source')
    parser.add_argument('--limit', type=int, help=f'Max candidates (default: {MAX_CANDIDATES_PER_RUN})')
    parser.add_argument('--force', action='store_true', help='Run even if queue is healthy')
    args = parser.parse_args()

    run(args)


if __name__ == '__main__':
    main()
