#!/usr/bin/env python3
"""
NicheIndex Daily Health Digest
Posts pipeline health, data coverage, and crawler performance to Discord.
Pure SQL -> template -> Discord. NO LLM. Stateless.

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

# Expected daily jobs that must complete within 26h to be considered healthy
EXPECTED_DAILY_JOBS = ["daily-census", "rss-pulse"]

THRESHOLDS = {
    # Pipeline failure counts -> traffic light
    "pipeline_green": 0,       # 0 failures = green
    "pipeline_yellow": 2,      # 1-2 = yellow
    # pipeline_red: 3+

    # ICP 1 discovery coverage %
    "icp1_green": 90,
    "icp1_yellow": 70,

    # ICP 2 intelligence coverage %
    "icp2_green": 60,
    "icp2_yellow": 40,

    # ICP 3 subscriber count
    "icp3_green": 300,
    "icp3_yellow": 100,

    # Max failures shown before "and N more"
    "max_failures_shown": 5,

    # Minimum batch attempts to qualify for best/worst
    "min_batch_attempts": 3,
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
    """Return GREEN/YELLOW/RED based on thresholds."""
    if higher_is_better:
        if value >= green_threshold:
            return "green"
        elif value >= yellow_threshold:
            return "yellow"
        else:
            return "red"
    else:
        # Lower is better (e.g., failure count)
        if value <= green_threshold:
            return "green"
        elif value <= yellow_threshold:
            return "yellow"
        else:
            return "red"


def progress_bar(fraction, width=10):
    """Return a text progress bar like [########--]."""
    filled = int(round(fraction * width))
    filled = max(0, min(width, filled))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def compute_pipeline(cur):
    """Section 1: Pipeline health for last 24h."""
    metrics = {}

    # Failures in last 24h
    cur.execute("""
        SELECT job_name, error_message
        FROM pipeline_runs
        WHERE started_at >= now() - interval '24 hours'
          AND status = 'failed'
        ORDER BY started_at DESC
    """)
    failures = cur.fetchall()
    metrics["failure_count"] = len(failures)
    metrics["failures"] = [
        {"job": f["job_name"], "error": (f.get("error_message") or "")[:80]}
        for f in failures
    ]

    # Completions in last 24h
    cur.execute("""
        SELECT count(*) AS completed
        FROM pipeline_runs
        WHERE started_at >= now() - interval '24 hours'
          AND status = 'completed'
    """)
    row = cur.fetchone()
    metrics["completed"] = row["completed"] if row else 0

    # Missing expected jobs (no success in 26h)
    cur.execute("""
        SELECT job_name, max(started_at) AS last_success
        FROM pipeline_runs
        WHERE status = 'completed'
          AND job_name = ANY(%s)
        GROUP BY job_name
    """, (EXPECTED_DAILY_JOBS,))
    found = {r["job_name"]: r["last_success"] for r in cur.fetchall()}
    missing = []
    for job in EXPECTED_DAILY_JOBS:
        if job not in found:
            missing.append(job)
        else:
            # Check if last success is older than 26h — do it in Python
            # since we already have the timestamp
            cur.execute("""
                SELECT %s < now() - interval '26 hours' AS stale
            """, (found[job],))
            if cur.fetchone()["stale"]:
                missing.append(job)
    metrics["missing_jobs"] = missing

    # Traffic light
    total_issues = metrics["failure_count"] + len(missing)
    metrics["light"] = traffic_light(
        total_issues,
        THRESHOLDS["pipeline_green"],
        THRESHOLDS["pipeline_yellow"],
        higher_is_better=False
    )

    return metrics


def compute_coverage(cur):
    """Section 2: ICP coverage scorecards."""
    metrics = {}

    # ICP 1: Discovery completeness
    cur.execute("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE name IS NOT NULL
                   AND free_subscriber_count > 0
                   AND category_name IS NOT NULL
                   AND niche IS NOT NULL) AS complete
        FROM publications
        WHERE is_inactive != true
    """)
    row = cur.fetchone()
    total = row["total"] or 1
    complete = row["complete"] or 0
    pct = round(complete / total * 100, 1)
    light = traffic_light(pct, THRESHOLDS["icp1_green"], THRESHOLDS["icp1_yellow"])
    metrics["icp1"] = {"pct": pct, "total": total, "complete": complete, "light": light}

    # ICP 2: Intelligence completeness
    cur.execute("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE ni_rating IS NOT NULL
                   AND growth_7d IS NOT NULL
                   AND ni_audience_score IS NOT NULL
                   AND ni_engagement_proxy IS NOT NULL) AS complete
        FROM publications
        WHERE is_inactive != true
    """)
    row = cur.fetchone()
    total = row["total"] or 1
    complete = row["complete"] or 0
    pct = round(complete / total * 100, 1)
    light = traffic_light(pct, THRESHOLDS["icp2_green"], THRESHOLDS["icp2_yellow"])
    metrics["icp2"] = {"pct": pct, "total": total, "complete": complete, "light": light}

    # ICP 3: Sponsor pipeline (email subscriber count)
    cur.execute("""
        SELECT count(*) AS subs
        FROM email_subscriptions
        WHERE status = 'subscribed'
    """)
    row = cur.fetchone()
    subs = row["subs"] or 0
    light = traffic_light(subs, THRESHOLDS["icp3_green"], THRESHOLDS["icp3_yellow"])
    metrics["icp3"] = {"subs": subs, "light": light}

    return metrics


def compute_crawler(cur):
    """Section 3: Crawler batch performance (7d)."""
    metrics = {}

    # Overall signup success rate (7d)
    cur.execute("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status = 'subscribed') AS subscribed
        FROM email_subscriptions
        WHERE created_at >= now() - interval '7 days'
    """)
    row = cur.fetchone()
    total = row["total"] or 0
    subscribed = row["subscribed"] or 0
    metrics["signups_total"] = total
    metrics["signups_subscribed"] = subscribed
    metrics["signups_pct"] = round(subscribed / total * 100) if total > 0 else 0

    # Best and worst batch by success rate
    min_attempts = THRESHOLDS["min_batch_attempts"]
    cur.execute("""
        SELECT sq.vertical AS batch,
               count(*) AS attempts,
               count(*) FILTER (WHERE es.status = 'subscribed') AS successes
        FROM subscription_queue sq
        LEFT JOIN email_subscriptions es ON es.publication_id = sq.publication_id
        WHERE sq.processed_at >= now() - interval '7 days'
        GROUP BY sq.vertical
        HAVING count(*) >= %s
        ORDER BY count(*) FILTER (WHERE es.status = 'subscribed')::float / count(*) DESC
    """, (min_attempts,))
    batches = cur.fetchall()

    if batches:
        best = batches[0]
        worst = batches[-1]
        metrics["best_batch"] = {
            "name": best["batch"] or "unknown",
            "pct": round(best["successes"] / best["attempts"] * 100) if best["attempts"] > 0 else 0,
            "attempts": best["attempts"],
        }
        metrics["worst_batch"] = {
            "name": worst["batch"] or "unknown",
            "pct": round(worst["successes"] / worst["attempts"] * 100) if worst["attempts"] > 0 else 0,
            "attempts": worst["attempts"],
        }
    else:
        metrics["best_batch"] = None
        metrics["worst_batch"] = None

    return metrics


def format_health(pipeline, coverage, crawler):
    """Format all sections into a Discord message."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"**Daily Health** -- {now}\n"]

    # --- PIPELINE ---
    if pipeline:
        light = pipeline["light"].upper()
        lines.append(f"**[{light}] PIPELINE** (last 24h)")

        if pipeline["failure_count"] == 0 and not pipeline["missing_jobs"]:
            lines.append("  All jobs completed successfully")
        else:
            shown = pipeline["failures"][:THRESHOLDS["max_failures_shown"]]
            for f in shown:
                err = f" — {f['error']}" if f["error"] else ""
                lines.append(f"  FAIL: {f['job']}{err}")
            overflow = pipeline["failure_count"] - len(shown)
            if overflow > 0:
                lines.append(f"  ...and {overflow} more")
            if pipeline["missing_jobs"]:
                lines.append(f"  MISSING (no success in 26h): {', '.join(pipeline['missing_jobs'])}")

        lines.append(f"  Completed: {pipeline['completed']}")
    else:
        lines.append("**PIPELINE** — query failed")

    # --- COVERAGE ---
    lines.append("")
    if coverage:
        lines.append("**COVERAGE**")
        icp1 = coverage["icp1"]
        bar1 = progress_bar(icp1["pct"] / 100)
        lines.append(f"  ICP 1 (discovery):     {icp1['pct']}% {bar1} {icp1['light']}")

        icp2 = coverage["icp2"]
        bar2 = progress_bar(icp2["pct"] / 100)
        lines.append(f"  ICP 2 (intelligence):  {icp2['pct']}% {bar2} {icp2['light']}")

        icp3 = coverage["icp3"]
        # For ICP3, bar is fraction of green threshold
        bar3 = progress_bar(min(icp3["subs"] / THRESHOLDS["icp3_green"], 1.0))
        lines.append(f"  ICP 3 (sponsor):       {icp3['subs']} subs {bar3} {icp3['light']}")
    else:
        lines.append("**COVERAGE** — query failed")

    # --- CRAWLER ---
    lines.append("")
    if crawler:
        lines.append("**CRAWLER**")
        lines.append(f"  Signups (7d): {crawler['signups_subscribed']}/{crawler['signups_total']} ({crawler['signups_pct']}%)")
        if crawler["best_batch"]:
            b = crawler["best_batch"]
            lines.append(f"  Best batch:  {b['name']} ({b['pct']}% of {b['attempts']})")
        if crawler["worst_batch"]:
            w = crawler["worst_batch"]
            lines.append(f"  Worst batch: {w['name']} ({w['pct']}% of {w['attempts']})")
        if not crawler["best_batch"] and not crawler["worst_batch"]:
            lines.append(f"  No batches with >= {THRESHOLDS['min_batch_attempts']} attempts")
    else:
        lines.append("**CRAWLER** — query failed")

    return "\n".join(lines)


def send_discord(message: str):
    """Send message to Discord, splitting if over 1990 chars."""
    if not DISCORD_WEBHOOK:
        print(message)
        return

    import urllib.request

    # Split into chunks under 1990 chars on line boundaries
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


def main():
    print(f"=== DAILY HEALTH at {datetime.now(timezone.utc).isoformat()} ===")

    ro_conn = get_readonly_conn()
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Compute each section independently — failures in one don't block others
    pipeline = None
    coverage = None
    crawler = None
    observation = {}

    try:
        pipeline = compute_pipeline(cur)
        observation["pipeline"] = pipeline
    except Exception as e:
        print(f"WARNING: pipeline section failed: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        observation["pipeline_error"] = str(e)

    try:
        coverage = compute_coverage(cur)
        observation["coverage"] = coverage
    except Exception as e:
        print(f"WARNING: coverage section failed: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        observation["coverage_error"] = str(e)

    try:
        crawler = compute_crawler(cur)
        observation["crawler"] = crawler
    except Exception as e:
        print(f"WARNING: crawler section failed: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        observation["crawler_error"] = str(e)

    ro_conn.close()

    report = format_health(pipeline, coverage, crawler)
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
