#!/usr/bin/env python3
"""
NicheIndex Daily Health Digest — ICP-aligned scorecards.
Posts decision-readiness coverage, ICP 3 pipeline velocity, and gap analysis.

Measures what % of the index can deliver the aha moment for each ICP:
  ICP 1: "Can a reader find a good newsletter right now?"
  ICP 2: "Can a creator get competitive intelligence worth $59?"
  ICP 3: "Are we on track for 500 email subs by June?"

Uses SUPABASE_READONLY_DB_URL for reads, DATABASE_URL for bot_interactions writes.
"""

import os
import sys
import json
import traceback
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

READONLY_DSN = os.environ.get("SUPABASE_READONLY_DB_URL", "")
WRITE_DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DIGEST", os.environ.get("DISCORD_WEBHOOK_URL", ""))

# Targets — what "healthy" looks like
TARGETS = {
    "icp1_pct": 90,
    "icp2_pct": 75,
    "icp3_pubs": 200,
}


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


def traffic_light(value, green_threshold, yellow_threshold, higher_is_better=True):
    if higher_is_better:
        if value >= green_threshold:
            return "GREEN"
        elif value >= yellow_threshold:
            return "YELLOW"
        return "RED"
    else:
        if value <= green_threshold:
            return "GREEN"
        elif value <= yellow_threshold:
            return "YELLOW"
        return "RED"


def progress_bar(fraction, width=10):
    filled = int(round(fraction * width))
    filled = max(0, min(width, filled))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


# ---------------------------------------------------------------------------
# Section 1: Pipeline Health
# ---------------------------------------------------------------------------

def compute_pipeline(cur):
    metrics = {}

    # Failures in last 24h
    cur.execute("""
        SELECT job_name, error_text
        FROM pipeline_runs
        WHERE started_at >= now() - interval '24 hours'
          AND status = 'failed'
        ORDER BY started_at DESC
    """)
    failures = cur.fetchall()
    metrics["failure_count"] = len(failures)
    metrics["failures"] = [
        {"job": f["job_name"], "error": (f.get("error_text") or "")[:80]}
        for f in failures
    ]

    # Completions in last 24h
    cur.execute("""
        SELECT count(*) AS completed
        FROM pipeline_runs
        WHERE started_at >= now() - interval '24 hours'
          AND status = 'completed'
    """)
    metrics["completed"] = cur.fetchone()["completed"] or 0

    # Check derived-metrics staleness
    cur.execute("""
        SELECT started_at FROM pipeline_runs
        WHERE job_name = 'derived-metrics' AND status = 'completed'
        ORDER BY started_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        metrics["derived_metrics_last"] = row["started_at"]
        cur.execute("SELECT %s < now() - interval '26 hours' AS stale", (row["started_at"],))
        metrics["derived_metrics_stale"] = cur.fetchone()["stale"]
    else:
        metrics["derived_metrics_last"] = None
        metrics["derived_metrics_stale"] = True

    metrics["light"] = traffic_light(
        metrics["failure_count"], 0, 2, higher_is_better=False
    )

    return metrics


# ---------------------------------------------------------------------------
# Section 2: ICP Scorecards — Decision-Ready Coverage
# ---------------------------------------------------------------------------

def compute_icp1(cur):
    """ICP 1 Discovery Ready — can this pub render a useful DiscoveryCard?"""
    cur.execute("""
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE
                p.name IS NOT NULL
                AND p.description IS NOT NULL AND length(p.description) > 30
                AND p.category_name IS NOT NULL
                AND pa.last_post_date IS NOT NULL
                AND pa.last_post_date > now() - interval '90 days'
                AND pa.content_velocity IS NOT NULL
            ) AS ready
        FROM publications p
        LEFT JOIN (
            SELECT DISTINCT ON (publication_id) publication_id, last_post_date, content_velocity
            FROM publication_activity
            ORDER BY publication_id, last_post_date DESC NULLS LAST
        ) pa ON pa.publication_id = p.id
        WHERE p.is_inactive != true
    """)
    row = cur.fetchone()
    total = row["total"] or 1
    ready = row["ready"] or 0
    pct = round(ready / total * 100, 1)

    # Get per-criterion breakdown for gap analysis
    cur.execute("""
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE p.description IS NULL OR length(p.description) <= 30) AS missing_description,
            count(*) FILTER (WHERE pa.last_post_date IS NULL OR pa.last_post_date <= now() - interval '90 days') AS missing_alive,
            count(*) FILTER (WHERE pa.content_velocity IS NULL) AS missing_velocity,
            count(*) FILTER (WHERE p.category_name IS NULL) AS missing_category
        FROM publications p
        LEFT JOIN (
            SELECT DISTINCT ON (publication_id) publication_id, last_post_date, content_velocity
            FROM publication_activity
            ORDER BY publication_id, last_post_date DESC NULLS LAST
        ) pa ON pa.publication_id = p.id
        WHERE p.is_inactive != true
    """)
    gaps = cur.fetchone()

    return {
        "pct": pct,
        "ready": ready,
        "total": total,
        "light": traffic_light(pct, TARGETS["icp1_pct"], TARGETS["icp1_pct"] - 20),
        "gaps": {
            "missing_description": gaps["missing_description"],
            "missing_alive": gaps["missing_alive"],
            "missing_velocity": gaps["missing_velocity"],
            "missing_category": gaps["missing_category"],
        },
    }


def compute_icp2(cur):
    """ICP 2 Intelligence Ready — can this pub power competitive analysis?"""
    cur.execute("""
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE
                p.ni_rating IS NOT NULL
                AND p.growth_30d IS NOT NULL
                AND p.niche IS NOT NULL
                AND pa.content_velocity IS NOT NULL
                AND snap.snap_count >= 7
            ) AS ready
        FROM publications p
        LEFT JOIN (
            SELECT DISTINCT ON (publication_id) publication_id, content_velocity
            FROM publication_activity
            ORDER BY publication_id, last_post_date DESC NULLS LAST
        ) pa ON pa.publication_id = p.id
        LEFT JOIN (
            SELECT publication_id, count(*) AS snap_count
            FROM publication_snapshots
            GROUP BY publication_id
        ) snap ON snap.publication_id = p.id
        WHERE p.is_inactive != true
    """)
    row = cur.fetchone()
    total = row["total"] or 1
    ready = row["ready"] or 0
    pct = round(ready / total * 100, 1)

    # Per-criterion breakdown
    cur.execute("""
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE p.ni_rating IS NULL) AS missing_ni_rating,
            count(*) FILTER (WHERE p.growth_30d IS NULL) AS missing_growth,
            count(*) FILTER (WHERE p.niche IS NULL) AS missing_niche,
            count(*) FILTER (WHERE pa.content_velocity IS NULL) AS missing_velocity
        FROM publications p
        LEFT JOIN (
            SELECT DISTINCT ON (publication_id) publication_id, content_velocity
            FROM publication_activity
            ORDER BY publication_id, last_post_date DESC NULLS LAST
        ) pa ON pa.publication_id = p.id
        WHERE p.is_inactive != true
    """)
    gaps = cur.fetchone()

    return {
        "pct": pct,
        "ready": ready,
        "total": total,
        "light": traffic_light(pct, TARGETS["icp2_pct"], TARGETS["icp2_pct"] - 25),
        "gaps": {
            "missing_ni_rating": gaps["missing_ni_rating"],
            "missing_growth": gaps["missing_growth"],
            "missing_niche": gaps["missing_niche"],
            "missing_velocity": gaps["missing_velocity"],
        },
    }


def compute_icp3(cur):
    """ICP 3 Sponsor Intel Ready — pubs with defensible ad intelligence."""
    # Pubs with ads_ratio detected
    cur.execute("""
        SELECT count(*) AS ads_detected
        FROM publications
        WHERE is_inactive != true AND ni_ads_ratio IS NOT NULL
    """)
    ads_detected = cur.fetchone()["ads_detected"]

    # Active email subscriptions
    cur.execute("""
        SELECT count(*) AS active_subs
        FROM email_subscriptions
        WHERE status = 'subscribed'
    """)
    active_subs = cur.fetchone()["active_subs"]

    # Pubs with both ads_ratio AND active email subscription
    cur.execute("""
        SELECT count(DISTINCT p.id) AS full_ready
        FROM publications p
        JOIN email_subscriptions es ON es.homepage_url ILIKE '%' || p.subdomain || '%'
            AND es.status = 'subscribed'
        WHERE p.is_inactive != true
            AND p.ni_ads_ratio IS NOT NULL
    """)
    full_ready = cur.fetchone()["full_ready"]

    # Queue depth
    cur.execute("""
        SELECT
            count(*) FILTER (WHERE status = 'pending') AS pending,
            count(*) FILTER (WHERE status = 'candidate') AS candidates
        FROM subscription_queue
    """)
    queue = cur.fetchone()

    # Signup velocity (14-day rolling)
    cur.execute("""
        SELECT
            count(*) AS attempts,
            count(*) FILTER (WHERE status = 'subscribed') AS successes
        FROM email_subscriptions
        WHERE created_at >= now() - interval '14 days'
    """)
    signups_14d = cur.fetchone()
    attempts_14d = signups_14d["attempts"] or 0
    successes_14d = signups_14d["successes"] or 0
    rate_14d = round(successes_14d / attempts_14d * 100) if attempts_14d > 0 else 0

    # Inbound emails (7d)
    cur.execute("""
        SELECT count(*) AS cnt
        FROM inbound_emails
        WHERE created_at >= now() - interval '7 days'
    """)
    inbound_7d = cur.fetchone()["cnt"]

    # Weeks to 500 projection
    weekly_rate = successes_14d / 2.0 if successes_14d > 0 else 0
    remaining = max(500 - active_subs, 0)
    weeks_to_500 = round(remaining / weekly_rate) if weekly_rate > 0 else None

    return {
        "ads_detected": ads_detected,
        "active_subs": active_subs,
        "full_ready": full_ready,
        "light": traffic_light(full_ready, TARGETS["icp3_pubs"], TARGETS["icp3_pubs"] // 4),
        "queue_pending": queue["pending"],
        "queue_candidates": queue["candidates"],
        "attempts_14d": attempts_14d,
        "successes_14d": successes_14d,
        "rate_14d": rate_14d,
        "inbound_7d": inbound_7d,
        "weeks_to_500": weeks_to_500,
    }


# ---------------------------------------------------------------------------
# Section 3: Gap Analysis
# ---------------------------------------------------------------------------

def compute_gaps(icp1, icp2, icp3):
    """Rank the top gaps across all ICPs by impact."""
    gaps = []

    # ICP 1 gaps
    for field, count in icp1["gaps"].items():
        if count > 0:
            label = field.replace("missing_", "").replace("_", " ")
            # Estimate ICP 1 % impact: this many pubs failing this criterion
            impact = round(count / icp1["total"] * 100, 1)
            gaps.append({
                "icp": "ICP 1",
                "description": f'{count:,} pubs missing {label}',
                "impact_pct": impact,
                "field": field,
            })

    # ICP 2 gaps
    for field, count in icp2["gaps"].items():
        if count > 0:
            label = field.replace("missing_", "").replace("_", " ")
            impact = round(count / icp2["total"] * 100, 1)
            gaps.append({
                "icp": "ICP 2",
                "description": f'{count:,} pubs missing {label}',
                "impact_pct": impact,
                "field": field,
            })

    # ICP 3 gaps (different — these are pipeline blockers, not per-pub fields)
    if icp3["active_subs"] < 500:
        gaps.append({
            "icp": "ICP 3",
            "description": f'Email corpus: {icp3["active_subs"]}/500 subs'
                           f' ({icp3["weeks_to_500"]}w at current rate)'
                           if icp3["weeks_to_500"]
                           else f'Email corpus: {icp3["active_subs"]}/500 subs (rate too low to project)',
            "impact_pct": 100 - round(icp3["active_subs"] / 500 * 100, 1),
            "field": "email_corpus",
        })

    queue_total = icp3["queue_pending"] + icp3["queue_candidates"]
    if queue_total < 100:
        gaps.append({
            "icp": "ICP 3",
            "description": f'Queue low: {queue_total} remaining. Needs CSV import.',
            "impact_pct": 80,
            "field": "queue_depth",
        })

    # Sort by impact, take top 3
    gaps.sort(key=lambda g: g["impact_pct"], reverse=True)
    return gaps[:3]


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_digest(pipeline, icp1, icp2, icp3, gaps):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"**Daily Health** -- {now_str}\n"]

    # Pipeline
    if pipeline:
        light = pipeline["light"]
        lines.append(f"**[{light}] PIPELINE** (24h)")
        lines.append(f"  {pipeline['completed']} completed, {pipeline['failure_count']} failed")
        if pipeline["failure_count"] > 0:
            for f in pipeline["failures"][:3]:
                err = f" -- {f['error']}" if f["error"] else ""
                lines.append(f"  FAIL: {f['job']}{err}")
        if pipeline.get("derived_metrics_stale"):
            if pipeline.get("derived_metrics_last"):
                lines.append(f"  ** Derived metrics stale (last: {pipeline['derived_metrics_last']})")
            else:
                lines.append(f"  ** Derived metrics never run")
    else:
        lines.append("**PIPELINE** -- query failed")

    # Coverage
    lines.append("")
    lines.append("**COVERAGE -- Decision-Ready %**")

    if icp1:
        bar = progress_bar(icp1["pct"] / 100)
        target = TARGETS["icp1_pct"]
        lines.append(f"  ICP 1 (Discovery):    {icp1['pct']}% {bar}  target: {target}%")

    if icp2:
        bar = progress_bar(icp2["pct"] / 100)
        target = TARGETS["icp2_pct"]
        lines.append(f"  ICP 2 (Intelligence): {icp2['pct']}% {bar}  target: {target}%")

    if icp3:
        target = TARGETS["icp3_pubs"]
        bar = progress_bar(min(icp3["full_ready"] / max(target, 1), 1.0))
        lines.append(f"  ICP 3 (Sponsor):      {icp3['full_ready']} pubs ready {bar}  target: {target}")

    # ICP 3 Pipeline
    if icp3:
        lines.append("")
        lines.append("**ICP 3 PIPELINE**")
        lines.append(f"  Queue: {icp3['queue_pending']} pending / {icp3['queue_candidates']} candidate")
        lines.append(f"  Signups (14d): {icp3['successes_14d']}/{icp3['attempts_14d']} ({icp3['rate_14d']}%)")
        lines.append(f"  Inbound emails (7d): {icp3['inbound_7d']}")
        lines.append(f"  Ads detected on: {icp3['ads_detected']} pubs")
        if icp3["weeks_to_500"]:
            lines.append(f"  At current rate: ~{icp3['weeks_to_500']} weeks to 500 subs")
        else:
            lines.append(f"  At current rate: too slow to project")

    # Gaps
    if gaps:
        lines.append("")
        lines.append(f"**GAPS** (top {len(gaps)} by impact)")
        for i, gap in enumerate(gaps, 1):
            lines.append(f"  {i}. {gap['description']} ({gap['icp']})")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Discord
# ---------------------------------------------------------------------------

def send_discord(message):
    if not DISCORD_WEBHOOK:
        print(message)
        return

    import urllib.request

    chunks = []
    current = ""
    for line in message.split("\n"):
        candidate = current + "\n" + line if current else line
        if len(candidate) > 1990:
            if current:
                chunks.append(current)
            current = line[:1990]
        else:
            current = candidate
    if current:
        chunks.append(current)

    for chunk in chunks:
        payload = json.dumps({"content": chunk}).encode()
        req = urllib.request.Request(
            DISCORD_WEBHOOK,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "NicheIndex-Bot/1.0",
            },
        )
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"WARNING: Discord failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"=== DAILY HEALTH at {datetime.now(timezone.utc).isoformat()} ===")

    ro_conn = get_readonly_conn()
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    pipeline = None
    icp1 = None
    icp2 = None
    icp3 = None
    observation = {}

    for section_name, compute_fn in [
        ("pipeline", lambda: compute_pipeline(cur)),
        ("icp1", lambda: compute_icp1(cur)),
        ("icp2", lambda: compute_icp2(cur)),
        ("icp3", lambda: compute_icp3(cur)),
    ]:
        try:
            result = compute_fn()
            observation[section_name] = result
            if section_name == "pipeline":
                pipeline = result
            elif section_name == "icp1":
                icp1 = result
            elif section_name == "icp2":
                icp2 = result
            elif section_name == "icp3":
                icp3 = result
        except Exception as e:
            print(f"WARNING: {section_name} failed: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            observation[f"{section_name}_error"] = str(e)
            ro_conn.rollback()

    ro_conn.close()

    # Compute gaps
    gaps = []
    if icp1 and icp2 and icp3:
        gaps = compute_gaps(icp1, icp2, icp3)
        observation["gaps"] = gaps

    report = format_digest(pipeline, icp1, icp2, icp3, gaps)
    send_discord(report)

    # Log to bot_interactions
    try:
        write_conn = get_write_conn()
        w_cur = write_conn.cursor()
        w_cur.execute("""
            INSERT INTO bot_interactions (bot_name, interaction_type, observation)
            VALUES ('daily-health', 'digest', %s)
        """, (json.dumps(observation, default=str),))
        write_conn.commit()
        write_conn.close()
    except Exception as e:
        print(f"WARNING: Could not log interaction: {e}", file=sys.stderr)

    print("Daily health complete.")


if __name__ == "__main__":
    main()
