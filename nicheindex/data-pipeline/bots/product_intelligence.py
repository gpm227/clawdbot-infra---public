#!/usr/bin/env python3
"""
NicheIndex Product Intelligence Bot
Weekly intelligence: SQL signals + LLM narration + proposed directives.
Posts to #product-insights. Logs to bot_interactions. Proposes to bot_directives.

Uses SUPABASE_READONLY_DB_URL for all reads.
Uses DATABASE_URL for bot_interactions + bot_directives writes.
Uses ANTHROPIC_API_KEY for LLM narration (optional — degrades gracefully).
"""

import os
import sys
import json
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
READONLY_DSN = os.environ.get("SUPABASE_READONLY_DB_URL", "")
WRITE_DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_PRODUCT_INSIGHTS", os.environ.get("DISCORD_WEBHOOK_URL", ""))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Significance thresholds — filter noise, surface real signals
THRESHOLDS = {
    "category_momentum_pct": 5.0,        # Only surface if >5% week-over-week growth
    "breakout_growth_pct": 20.0,          # Only if >20% individual growth
    "engagement_gap_multiplier": 2.0,     # Only if 2x+ category median
    "velocity_deviation_pct": 25.0,       # Only if >25% deviation from last week
    "unsurfaced_coverage_pct": 95.0,      # Only if >95% coverage
    "new_entrants_min": 3,                # Only if 3+ new pubs in a niche
}

MAX_DIRECTIVES = 3  # Cap per run — ranked by impact


def get_readonly_conn():
    if not READONLY_DSN:
        print("ERROR: SUPABASE_READONLY_DB_URL not set", file=sys.stderr)
        sys.exit(1)
    return psycopg2.connect(READONLY_DSN)


def get_write_conn():
    if not WRITE_DSN:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    return psycopg2.connect(WRITE_DSN)


# ---------------------------------------------------------------------------
# JSON serialization helper (handles Decimal, datetime)
# ---------------------------------------------------------------------------
def json_serial(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# ---------------------------------------------------------------------------
# Signal 1: Category Momentum
# ---------------------------------------------------------------------------
def signal_category_momentum(cur) -> list[dict]:
    """Which niches grew fastest this week?"""
    cur.execute("""
        SELECT category_name, subscriber_growth_7d_pct, subscriber_growth_7d_abs
        FROM niche_trends
        WHERE computed_at >= now() - interval '8 days'
        AND subscriber_growth_7d_pct IS NOT NULL
        ORDER BY subscriber_growth_7d_pct DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    results = []
    for r in rows:
        pct = float(r["subscriber_growth_7d_pct"] or 0)
        if abs(pct) >= THRESHOLDS["category_momentum_pct"]:
            results.append({
                "signal_type": "category_momentum",
                "metric": f"{r['category_name']} grew {pct:.1f}% this week",
                "value": pct,
                "abs_growth": int(r["subscriber_growth_7d_abs"] or 0),
                "threshold": THRESHOLDS["category_momentum_pct"],
                "significant": True,
                "category": r["category_name"],
            })
    return results


# ---------------------------------------------------------------------------
# Signal 2: Breakout Newsletter
# ---------------------------------------------------------------------------
def signal_breakout_newsletter(cur) -> list[dict]:
    """Which single newsletter grew fastest?"""
    cur.execute("""
        SELECT fastest_growing_pub_name, fastest_growing_pub_growth_pct,
               fastest_growing_pub_id, category_name
        FROM niche_trends
        WHERE computed_at >= now() - interval '8 days'
        AND fastest_growing_pub_growth_pct IS NOT NULL
        ORDER BY fastest_growing_pub_growth_pct DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    results = []
    for r in rows:
        pct = float(r["fastest_growing_pub_growth_pct"] or 0)
        if pct >= THRESHOLDS["breakout_growth_pct"]:
            results.append({
                "signal_type": "breakout_newsletter",
                "metric": f"{r['fastest_growing_pub_name']} grew {pct:.1f}% in {r['category_name']}",
                "value": pct,
                "pub_name": r["fastest_growing_pub_name"],
                "pub_id": r["fastest_growing_pub_id"],
                "category": r["category_name"],
                "threshold": THRESHOLDS["breakout_growth_pct"],
                "significant": True,
            })
    return results


# ---------------------------------------------------------------------------
# Signal 3: Engagement Gap
# ---------------------------------------------------------------------------
def signal_engagement_gap(cur) -> list[dict]:
    """Niches with high engagement relative to their size."""
    cur.execute("""
        SELECT p.category_name,
               round(avg(pa.avg_engagement), 1) AS niche_avg_engagement,
               round(avg(p.free_subscriber_count), 0) AS niche_avg_subs,
               count(*) AS pub_count
        FROM publication_activity pa
        JOIN publications p ON p.id = pa.publication_id
        WHERE p.is_inactive != true
        AND pa.avg_engagement IS NOT NULL
        AND pa.avg_engagement > 0
        GROUP BY p.category_name
        HAVING count(*) >= 5
        ORDER BY avg(pa.avg_engagement) / NULLIF(avg(p.free_subscriber_count), 0) DESC NULLS LAST
        LIMIT 10
    """)
    rows = cur.fetchall()

    # Get overall median engagement
    cur.execute("""
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_engagement) AS median
        FROM publication_activity
        WHERE avg_engagement IS NOT NULL AND avg_engagement > 0
    """)
    median_row = cur.fetchone()
    overall_median = float(median_row["median"] or 1) if median_row else 1.0

    results = []
    for r in rows:
        niche_avg = float(r["niche_avg_engagement"] or 0)
        multiplier = niche_avg / overall_median if overall_median > 0 else 0
        if multiplier >= THRESHOLDS["engagement_gap_multiplier"]:
            results.append({
                "signal_type": "engagement_gap",
                "metric": f"{r['category_name']}: {niche_avg:.0f} avg engagement ({multiplier:.1f}x median)",
                "value": multiplier,
                "niche_avg_engagement": niche_avg,
                "overall_median": overall_median,
                "category": r["category_name"],
                "pub_count": r["pub_count"],
                "threshold": THRESHOLDS["engagement_gap_multiplier"],
                "significant": True,
            })
    return results


# ---------------------------------------------------------------------------
# Signal 4: Velocity Benchmarks
# ---------------------------------------------------------------------------
def signal_velocity_benchmarks(cur) -> list[dict]:
    """Median content_velocity per niche — flag outliers."""
    cur.execute("""
        SELECT p.category_name,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY pa.content_velocity) AS median_velocity,
               count(*) AS pub_count
        FROM publication_activity pa
        JOIN publications p ON p.id = pa.publication_id
        WHERE p.is_inactive != true
        AND pa.content_velocity IS NOT NULL
        AND pa.content_velocity > 0
        GROUP BY p.category_name
        HAVING count(*) >= 5
        ORDER BY percentile_cont(0.5) WITHIN GROUP (ORDER BY pa.content_velocity) DESC
        LIMIT 10
    """)
    rows = cur.fetchall()

    # Get overall median
    cur.execute("""
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY content_velocity) AS median
        FROM publication_activity
        WHERE content_velocity IS NOT NULL AND content_velocity > 0
    """)
    median_row = cur.fetchone()
    overall_median = float(median_row["median"] or 1) if median_row else 1.0

    results = []
    for r in rows:
        niche_med = float(r["median_velocity"] or 0)
        deviation_pct = abs(niche_med - overall_median) / overall_median * 100 if overall_median > 0 else 0
        if deviation_pct >= THRESHOLDS["velocity_deviation_pct"]:
            direction = "above" if niche_med > overall_median else "below"
            results.append({
                "signal_type": "velocity_benchmark",
                "metric": f"{r['category_name']}: {niche_med:.1f} posts/mo ({deviation_pct:.0f}% {direction} median)",
                "value": niche_med,
                "deviation_pct": deviation_pct,
                "direction": direction,
                "overall_median": overall_median,
                "category": r["category_name"],
                "pub_count": r["pub_count"],
                "threshold": THRESHOLDS["velocity_deviation_pct"],
                "significant": True,
            })
    return results


# ---------------------------------------------------------------------------
# Signal 5: Unsurfaced Data Check
# ---------------------------------------------------------------------------
def signal_unsurfaced_data(cur) -> list[dict]:
    """Columns with high coverage not shown in product."""
    # Columns NOT currently surfaced anywhere in the product UI
    UNSURFACED_COLUMNS = [
        ("publication_activity", "avg_engagement"),
        ("publication_activity", "avg_restacks"),
        ("publication_activity", "content_velocity"),
        ("publication_activity", "pct_paid_posts"),
    ]

    results = []
    total_active_q = "SELECT count(*) AS total FROM publications WHERE is_inactive != true"
    cur.execute(total_active_q)
    total_active = cur.fetchone()["total"] or 1

    for table, col in UNSURFACED_COLUMNS:
        cur.execute(f"""
            SELECT count(*) AS covered,
                   round(stddev({col})::numeric, 2) AS variance
            FROM {table} t
            JOIN publications p ON p.id = t.publication_id
            WHERE p.is_inactive != true
            AND {col} IS NOT NULL
        """)
        row = cur.fetchone()
        covered = row["covered"] or 0
        variance = float(row["variance"] or 0)
        coverage_pct = covered / total_active * 100

        if coverage_pct >= THRESHOLDS["unsurfaced_coverage_pct"] and variance > 0:
            results.append({
                "signal_type": "unsurfaced_data",
                "metric": f"{table}.{col}: {coverage_pct:.1f}% coverage, not in product",
                "value": coverage_pct,
                "column": col,
                "table": table,
                "covered": covered,
                "total": total_active,
                "variance": variance,
                "threshold": THRESHOLDS["unsurfaced_coverage_pct"],
                "significant": True,
            })
    return results


# ---------------------------------------------------------------------------
# Signal 6: New Entrant Signal
# ---------------------------------------------------------------------------
def signal_new_entrants(cur) -> list[dict]:
    """Niches with the most new pubs this week."""
    cur.execute("""
        SELECT category_name, new_publications_7d
        FROM niche_trends
        WHERE computed_at >= now() - interval '8 days'
        AND new_publications_7d IS NOT NULL
        ORDER BY new_publications_7d DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    results = []
    for r in rows:
        count = int(r["new_publications_7d"] or 0)
        if count >= THRESHOLDS["new_entrants_min"]:
            results.append({
                "signal_type": "new_entrants",
                "metric": f"{r['category_name']}: {count} new newsletters this week",
                "value": count,
                "category": r["category_name"],
                "threshold": THRESHOLDS["new_entrants_min"],
                "significant": True,
            })
    return results


# ---------------------------------------------------------------------------
# Daily Scorecard (quality x quantity check)
# ---------------------------------------------------------------------------
def compute_scorecard(cur) -> dict:
    """Compute the daily health scorecard — flywheel + intelligence quality."""
    scorecard = {}

    # === FLYWHEEL (acquisition health) ===

    # Subscription queue health
    cur.execute("""
        SELECT
            count(*) FILTER (WHERE status = 'pending') AS pending,
            count(*) FILTER (WHERE status = 'subscribed') AS subscribed,
            count(*) FILTER (WHERE status = 'failed') AS failed,
            count(*) FILTER (WHERE status = 'candidate') AS candidates
        FROM subscription_queue
    """)
    q = cur.fetchone()
    scorecard["queue_pending"] = q["pending"]
    scorecard["queue_subscribed"] = q["subscribed"]
    scorecard["queue_failed"] = q["failed"]
    scorecard["queue_candidates"] = q["candidates"]

    # Active email subscriptions + inbox volume
    cur.execute("SELECT count(*) AS n FROM email_subscriptions WHERE status = 'subscribed'")
    scorecard["active_subscriptions"] = cur.fetchone()["n"]

    cur.execute("SELECT count(*) AS n FROM inbound_emails WHERE created_at >= now() - interval '7 days'")
    scorecard["inbox_volume_7d"] = cur.fetchone()["n"]

    # === INTELLIGENCE QUALITY (Substack engagement + growth) ===

    # Median engagement across active pubs (likes per post)
    cur.execute("""
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY pa.like_count) AS median_likes,
               avg(pa.like_count) AS avg_likes
        FROM publication_activity pa
        JOIN publications p ON p.id = pa.publication_id
        WHERE p.is_inactive != true AND pa.like_count > 0
    """)
    eng = cur.fetchone()
    scorecard["median_engagement"] = round(float(eng["median_likes"] or 0), 1)
    scorecard["avg_engagement"] = round(float(eng["avg_likes"] or 0), 1)

    # 7-day subscriber growth across all active pubs
    cur.execute("""
        SELECT sum(subscriber_growth_7d_abs) AS total_growth,
               avg(subscriber_growth_7d_pct) AS avg_growth_pct
        FROM niche_trends
        WHERE computed_at >= now() - interval '8 days'
        AND subscriber_growth_7d_abs IS NOT NULL
    """)
    growth = cur.fetchone()
    scorecard["total_sub_growth_7d"] = int(growth["total_growth"] or 0)
    scorecard["avg_sub_growth_pct_7d"] = round(float(growth["avg_growth_pct"] or 0), 2)

    # === INFRASTRUCTURE ===

    # Pipeline success rate (last 24h)
    cur.execute("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status = 'completed') AS passed
        FROM pipeline_runs
        WHERE started_at >= now() - interval '24 hours'
    """)
    row = cur.fetchone()
    scorecard["pipeline_success_pct"] = round(row["passed"] / max(row["total"], 1) * 100, 1)
    scorecard["pipeline_runs_24h"] = row["total"]

    # Active publications (context, not a target)
    cur.execute("SELECT count(*) AS n FROM publications WHERE is_inactive != true")
    scorecard["active_pubs"] = cur.fetchone()["n"]

    # NI Rating coverage
    cur.execute("""
        SELECT count(*) FILTER (WHERE ni_rating IS NOT NULL) AS rated,
               count(*) FILTER (WHERE ni_rating_mode = 'full') AS full_rated,
               count(*) AS total
        FROM publications WHERE is_inactive != true
    """)
    row = cur.fetchone()
    scorecard["rated_pubs"] = row["rated"]
    scorecard["full_rated_pubs"] = row["full_rated"]
    scorecard["rating_coverage_pct"] = round(row["rated"] / max(row["total"], 1) * 100, 1)

    return scorecard


# ---------------------------------------------------------------------------
# Check for RED alerts (safety override — bypasses all caps)
# ---------------------------------------------------------------------------
def check_red_alerts(cur) -> list[dict]:
    """Check for unresolved RED alerts. If any exist, report before insights."""
    cur.execute("""
        SELECT alert_type, job_name, message, created_at
        FROM bot_alerts
        WHERE resolved = false AND severity = 'red'
        ORDER BY created_at DESC
        LIMIT 10
    """)
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# LLM Narration (degrades gracefully if API unavailable)
# ---------------------------------------------------------------------------
def generate_narration(signals: list[dict], scorecard: dict, red_alerts: list[dict]) -> str:
    """Use Claude to generate a 3-5 bullet executive summary."""
    if not ANTHROPIC_API_KEY:
        return "_LLM narration unavailable (no API key). Data section above is complete._"

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        signal_text = "\n".join(f"- {s['metric']}" for s in signals)
        scorecard_text = json.dumps(scorecard, indent=2, default=json_serial)
        red_text = "\n".join(f"- RED: {a['job_name']}: {a['message']}" for a in red_alerts) if red_alerts else "None"

        prompt = f"""You are the Product Intelligence analyst for NicheIndex, a newsletter intelligence platform.

Below are this week's significant data signals, the daily scorecard, and any RED alerts.

SIGNALS:
{signal_text if signal_text else "No significant signals this week."}

SCORECARD:
{scorecard_text}

RED ALERTS:
{red_text}

WHAT MATTERS (flywheel health, not vanity metrics):
- Active subscriptions: growing toward 500+ (more subs = more inbox data = better intelligence)
- Inbox volume: growing toward 500+ emails/week (the raw fuel for content scoring)
- Queue depth: pending > 200 means flywheel is fed; < 50 means discovery bot needs to run
- Pipeline success rate: >95%
- Non-Substack pubs: growing (Substack is the base; net-new platforms are the moat)
- Active pub count is context, NOT a target — 16K Substack pubs is a nice base, not a goal

Write exactly 3-5 bullets:
1. Flywheel health: are subscriptions and inbox volume growing? (cite numbers)
2. Where are we lagging vs winning? (be specific — name the metric and gap)
3-5. Specific Y/N action items for the bot army. Each must be concrete and actionable.
   Format: "Y/N: [action] — [one sentence reason]"
   Examples: "Y: Enable inbox-scorer — 84 unscored emails waiting, content quality dimension blocked"
   "N: Skip media-kit-collector — sponsor intelligence is 3 months out, focus on coverage first"

Be direct. No hedging. Numbers over adjectives. Under 200 words total."""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text

    except Exception as e:
        return f"_LLM narration failed: {e}. Data section above is complete._"


# ---------------------------------------------------------------------------
# Directive generation
# ---------------------------------------------------------------------------
def generate_directives(signals: list[dict], scorecard: dict, red_alerts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Propose max 3 directives ranked by impact. Returns (top_directives, overflow)."""
    all_directives = []

    # RED alerts override everything
    if red_alerts:
        for alert in red_alerts:
            all_directives.append({
                "source": "product-intelligence",
                "target_job": alert.get("job_name"),
                "action": f"Investigate RED alert: {alert['message'][:100]}",
                "context": f"Unresolved RED alert since {alert['created_at']}. Pipeline health is priority.",
                "priority": "critical",
            })
        return all_directives[:MAX_DIRECTIVES], all_directives[MAX_DIRECTIVES:]

    # Scorecard-driven directives (quality x quantity)
    if scorecard.get("inbox_volume_7d", 0) < 100 and scorecard.get("active_subscriptions", 0) < 50:
        all_directives.append({
            "source": "product-intelligence",
            "target_job": "sponsor-collector",
            "action": "Trigger sponsor-collector run to increase newsletter subscriptions",
            "context": f"Only {scorecard.get('active_subscriptions', 0)} active subscriptions, {scorecard.get('inbox_volume_7d', 0)} emails this week. Target: 500+/week.",
            "priority": "high",
        })

    if scorecard.get("queue_pending", 0) < 50:
        all_directives.append({
            "source": "product-intelligence",
            "target_job": "newsletter-discovery",
            "action": "Queue running low — trigger discovery bot to refill",
            "context": f"Only {scorecard.get('queue_pending', 0)} pending in subscription_queue. Need 200+ to keep flywheel fed.",
            "priority": "high",
        })

    if scorecard.get("full_rated_pubs", 0) == 0 and scorecard.get("inbox_volume_7d", 0) > 50:
        all_directives.append({
            "source": "product-intelligence",
            "target_job": "inbox-scorer",
            "action": "Enable inbox-scorer — emails available but not being scored",
            "context": f"{scorecard.get('inbox_volume_7d', 0)} emails in last 7d but 0 full-rated pubs. Content quality dimension blocked.",
            "priority": "high",
        })

    # Signal-driven directives
    for s in signals:
        if s["signal_type"] == "unsurfaced_data" and s["value"] > 98:
            all_directives.append({
                "source": "product-intelligence",
                "target_job": None,
                "action": f"Surface {s['column']} in product — {s['value']:.0f}% coverage ready",
                "context": f"{s['table']}.{s['column']} has {s['covered']}/{s['total']} coverage with meaningful variance. Not shown anywhere in product UI.",
                "priority": "normal",
            })

    top = all_directives[:MAX_DIRECTIVES]
    overflow = all_directives[MAX_DIRECTIVES:]
    return top, overflow


# ---------------------------------------------------------------------------
# Discord formatting
# ---------------------------------------------------------------------------
def format_discord_report(signals: list[dict], scorecard: dict,
                          narration: str, directives: list[dict],
                          red_alerts: list[dict]) -> str:
    """Format the full weekly intelligence report for Discord."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = [f"**Weekly Product Intelligence** — {now}\n"]

    # RED alerts first (safety override)
    if red_alerts:
        lines.append("**PIPELINE HEALTH ISSUE**")
        for a in red_alerts:
            lines.append(f"  RED: {a['job_name']}: {a['message'][:120]}")
        lines.append("")

    # Scorecard
    lines.append("**FLYWHEEL**")
    lines.append(f"  Queue: {scorecard.get('queue_pending', '?')} pending → {scorecard.get('queue_subscribed', '?')} subscribed, {scorecard.get('queue_failed', '?')} failed, {scorecard.get('queue_candidates', '?')} candidates")
    lines.append(f"  Subscriptions: {scorecard.get('active_subscriptions', '?')} active")
    lines.append(f"  Inbox: {scorecard.get('inbox_volume_7d', '?')} emails this week")
    lines.append("")
    lines.append("**INTELLIGENCE**")
    lines.append(f"  Engagement: {scorecard.get('median_engagement', '?')} median / {scorecard.get('avg_engagement', '?')} avg likes per post")
    lines.append(f"  Growth (7d): {scorecard.get('total_sub_growth_7d', '?'):,} total subs gained, {scorecard.get('avg_sub_growth_pct_7d', '?')}% avg")
    lines.append(f"  Pubs: {scorecard.get('active_pubs', '?'):,} active | Pipeline: {scorecard.get('pipeline_success_pct', '?')}% ({scorecard.get('pipeline_runs_24h', '?')} runs)")
    lines.append("")

    # Signals
    if signals:
        lines.append("**SIGNALS**")
        for s in signals:
            lines.append(f"  {s['metric']}")
        lines.append("")
    else:
        lines.append("**SIGNALS:** No significant signals this week. All systems nominal.\n")

    # LLM narration
    lines.append("**EXECUTIVE SUMMARY**")
    lines.append(narration)
    lines.append("")

    # Directives
    if directives:
        lines.append("**PROPOSED ACTIONS** (react with thumbs up/down)")
        for i, d in enumerate(directives, 1):
            priority_tag = f"[{d['priority'].upper()}]" if d['priority'] != 'normal' else ""
            target = f" → {d['target_job']}" if d['target_job'] else ""
            lines.append(f"  {i}. {priority_tag} {d['action']}{target}")
            if d.get('context'):
                lines.append(f"     _{d['context'][:150]}_")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Discord posting
# ---------------------------------------------------------------------------
def send_discord(message: str):
    if not DISCORD_WEBHOOK:
        print("WARNING: No Discord webhook. Printing to stdout.", file=sys.stderr)
        print(message)
        return
    import urllib.request
    # Discord max is 2000 chars — split if needed
    chunks = [message[i:i+1990] for i in range(0, len(message), 1990)]
    for chunk in chunks:
        payload = json.dumps({"content": chunk}).encode()
        req = urllib.request.Request(DISCORD_WEBHOOK, data=payload,
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "NicheIndex-Bot/1.0"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"WARNING: Discord failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Persistence: bot_interactions + bot_directives
# ---------------------------------------------------------------------------
def log_interaction(write_conn, signals, scorecard, narration, directives, red_alerts):
    """Log this run to bot_interactions (training data)."""
    cur = write_conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bot_interactions (bot_name, interaction_type, observation)
            VALUES ('product-intelligence', 'insight', %s)
        """, (json.dumps({
            "signals": signals,
            "scorecard": scorecard,
            "narration": narration,
            "directives": [d["action"] for d in directives],
            "red_alerts": len(red_alerts),
            "run_at": datetime.now(timezone.utc).isoformat(),
        }, default=json_serial),))
        write_conn.commit()
    except Exception as e:
        print(f"WARNING: Failed to log interaction: {e}", file=sys.stderr)
        write_conn.rollback()


def write_directives(write_conn, directives: list[dict]):
    """Write proposed directives to bot_directives table."""
    if not directives:
        return
    cur = write_conn.cursor()
    try:
        for d in directives:
            cur.execute("""
                INSERT INTO bot_directives (source, target_job, action, context, priority, status)
                VALUES (%s, %s, %s, %s, %s, 'proposed')
            """, (d["source"], d["target_job"], d["action"], d.get("context"), d["priority"]))
        write_conn.commit()
        print(f"Wrote {len(directives)} directives to bot_directives")
    except Exception as e:
        print(f"WARNING: Failed to write directives: {e}", file=sys.stderr)
        write_conn.rollback()


# ---------------------------------------------------------------------------
# Overflow: log signals that didn't make the cut
# ---------------------------------------------------------------------------
def log_overflow_directives(write_conn, overflow: list[dict]):
    """Log overflow directives as 'logged' status (available but not pushed to Discord)."""
    if not overflow:
        return
    cur = write_conn.cursor()
    try:
        for d in overflow:
            cur.execute("""
                INSERT INTO bot_directives (source, target_job, action, context, priority, status)
                VALUES (%s, %s, %s, %s, %s, 'logged')
            """, (d["source"], d.get("target_job"), d["action"], d.get("context"), d.get("priority", "low")))
        write_conn.commit()
    except Exception as e:
        print(f"WARNING: Failed to log overflow: {e}", file=sys.stderr)
        write_conn.rollback()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"=== PRODUCT INTELLIGENCE at {datetime.now(timezone.utc).isoformat()} ===")

    ro_conn = get_readonly_conn()
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check RED alerts first (safety override)
    try:
        red_alerts = check_red_alerts(cur)
        if red_alerts:
            print(f"RED ALERTS: {len(red_alerts)} unresolved")
    except Exception as e:
        print(f"  RED alert check failed: {e}", file=sys.stderr)
        red_alerts = []
        ro_conn.rollback()

    # Compute all 6 signals
    all_signals = []
    signal_fns = [
        signal_category_momentum,
        signal_breakout_newsletter,
        signal_engagement_gap,
        signal_velocity_benchmarks,
        signal_unsurfaced_data,
        signal_new_entrants,
    ]
    for fn in signal_fns:
        try:
            results = fn(cur)
            all_signals.extend(results)
            print(f"  {fn.__name__}: {len(results)} significant")
        except Exception as e:
            print(f"  {fn.__name__}: ERROR — {e}", file=sys.stderr)
            ro_conn.rollback()

    # Scorecard
    try:
        scorecard = compute_scorecard(cur)
        print(f"  Scorecard computed: {scorecard.get('active_pubs', '?')} active pubs")
    except Exception as e:
        print(f"  Scorecard: ERROR — {e}", file=sys.stderr)
        scorecard = {}
        ro_conn.rollback()

    ro_conn.close()

    # LLM narration
    narration = generate_narration(all_signals, scorecard, red_alerts)
    print(f"  Narration: {len(narration)} chars")

    # Generate directives (max 3 proposed, rest overflow)
    directives, overflow = generate_directives(all_signals, scorecard, red_alerts)
    print(f"  Directives: {len(directives)} proposed, {len(overflow)} overflow")

    # Format and send
    report = format_discord_report(all_signals, scorecard, narration, directives, red_alerts)
    send_discord(report)

    # Persist
    write_conn = None
    try:
        write_conn = get_write_conn()
        log_interaction(write_conn, all_signals, scorecard, narration, directives, red_alerts)
        write_directives(write_conn, directives)
        log_overflow_directives(write_conn, overflow)
    except Exception as e:
        print(f"WARNING: Persistence failed: {e}", file=sys.stderr)
    finally:
        if write_conn:
            write_conn.close()

    print("Product Intelligence complete.")


if __name__ == "__main__":
    main()
