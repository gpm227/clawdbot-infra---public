#!/usr/bin/env python3
"""
NicheIndex Weekly Pipeline Digest
Summarizes pipeline health for the past 7 days.
Posts to #pipeline-digest Discord channel. Logs to bot_interactions.

Uses SUPABASE_READONLY_DB_URL for reads, DATABASE_URL for bot_interactions writes.
"""

import os
import sys
import json
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

READONLY_DSN = os.environ.get("SUPABASE_READONLY_DB_URL", "")
WRITE_DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DIGEST", os.environ.get("DISCORD_WEBHOOK_URL", ""))


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


def compute_digest(ro_conn) -> dict:
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Per-job stats for past 7 days
    cur.execute("""
        SELECT job_name,
               count(*) FILTER (WHERE status = 'completed') AS completed,
               count(*) FILTER (WHERE status = 'failed') AS failed,
               count(*) AS total,
               round(avg(duration_seconds) FILTER (WHERE status = 'completed'), 1) AS avg_duration,
               round(max(duration_seconds) FILTER (WHERE status = 'completed'), 1) AS max_duration,
               sum(records_written) FILTER (WHERE status = 'completed') AS total_written
        FROM pipeline_runs
        WHERE started_at >= now() - interval '7 days'
        GROUP BY job_name
        ORDER BY job_name
    """)
    job_stats = cur.fetchall()

    # Unresolved alerts
    cur.execute("""
        SELECT count(*) AS open_alerts,
               count(*) FILTER (WHERE severity = 'red') AS red_alerts
        FROM bot_alerts
        WHERE resolved = false
    """)
    alert_stats = cur.fetchone()

    # Data growth
    cur.execute("""
        SELECT
            (SELECT count(*) FROM publications WHERE is_inactive != true) AS active_pubs,
            (SELECT count(*) FROM publication_snapshots
             WHERE created_at >= now() - interval '7 days') AS new_snapshots,
            (SELECT count(*) FROM publication_activity
             WHERE updated_at >= now() - interval '7 days') AS updated_activity
    """)
    data_stats = cur.fetchone()

    # Auto-remediations this week
    cur.execute("""
        SELECT count(*) AS remediations
        FROM bot_interactions
        WHERE bot_name = 'watchdog'
        AND interaction_type = 'remediation'
        AND created_at >= now() - interval '7 days'
    """)
    remediation_stats = cur.fetchone()

    # Acquisition flywheel stats
    cur.execute("""
        SELECT
            count(*) FILTER (WHERE status = 'pending') AS pending,
            count(*) FILTER (WHERE status = 'candidate') AS candidates,
            count(*) FILTER (WHERE status = 'subscribed') AS subscribed,
            count(*) FILTER (WHERE status = 'failed') AS failed,
            count(*) FILTER (WHERE status = 'rejected') AS rejected,
            count(*) FILTER (WHERE status = 'subscribed'
                AND processed_at >= now() - interval '7 days') AS subscribed_this_week
        FROM subscription_queue
    """)
    queue_stats = cur.fetchone()

    # Top verticals by queue entries
    cur.execute("""
        SELECT sq.vertical, count(*) AS cnt
        FROM subscription_queue sq
        WHERE sq.vertical IS NOT NULL
        GROUP BY sq.vertical
        ORDER BY cnt DESC
        LIMIT 5
    """)
    top_verticals = cur.fetchall()

    return {
        "job_stats": [dict(j) for j in job_stats],
        "alert_stats": dict(alert_stats) if alert_stats else {},
        "data_stats": dict(data_stats) if data_stats else {},
        "remediation_count": remediation_stats["remediations"] if remediation_stats else 0,
        "queue_stats": dict(queue_stats) if queue_stats else {},
        "top_verticals": [dict(v) for v in top_verticals] if top_verticals else [],
    }


def format_digest(data: dict) -> str:
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = [f"**Weekly Pipeline Digest** — week ending {now}\n"]

    lines.append("**JOB HEALTH**")
    total_runs = 0
    total_fails = 0
    for j in data["job_stats"]:
        total_runs += j["total"]
        total_fails += j["failed"]
        rate = round(j["completed"] / j["total"] * 100) if j["total"] > 0 else 0
        dur = f"{j['avg_duration']}s avg" if j["avg_duration"] else "no data"
        written = f", {j['total_written']} written" if j["total_written"] else ""
        fail_note = f" ({j['failed']} failed)" if j["failed"] > 0 else ""
        lines.append(f"  {j['job_name']}: {j['completed']}/{j['total']} runs ({rate}%){fail_note} — {dur}{written}")

    overall_rate = round((total_runs - total_fails) / total_runs * 100) if total_runs > 0 else 0
    lines.append(f"\n  Overall: {total_runs} runs, {overall_rate}% success rate")

    alert = data.get("alert_stats", {})
    open_a = alert.get("open_alerts", 0)
    red_a = alert.get("red_alerts", 0)
    lines.append(f"\n**ALERTS:** {open_a} open ({red_a} RED)")

    rem = data.get("remediation_count", 0)
    if rem > 0:
        lines.append(f"**AUTO-REMEDIATIONS:** {rem} stuck jobs auto-fixed this week")

    ds = data.get("data_stats", {})
    lines.append(f"\n**DATA**")
    lines.append(f"  Active publications: {ds.get('active_pubs', '?')}")
    lines.append(f"  New snapshots (7d): {ds.get('new_snapshots', '?')}")
    lines.append(f"  Activity updates (7d): {ds.get('updated_activity', '?')}")

    qs = data.get("queue_stats", {})
    if qs:
        lines.append(f"\n**ACQUISITION FLYWHEEL**")
        lines.append(f"  Queue: {qs.get('pending', 0)} pending, {qs.get('candidates', 0)} candidates, {qs.get('subscribed', 0)} subscribed, {qs.get('failed', 0)} failed, {qs.get('rejected', 0)} rejected")
        lines.append(f"  New subscriptions this week: {qs.get('subscribed_this_week', 0)}")
        verts = data.get("top_verticals", [])
        if verts:
            vert_parts = [f"{v['vertical']} ({v['cnt']})" for v in verts]
            lines.append(f"  Top verticals: {' · '.join(vert_parts)}")

    return "\n".join(lines)


def send_discord(message: str):
    if not DISCORD_WEBHOOK:
        print(message)
        return
    import urllib.request
    payload = json.dumps({"content": message[:2000]}).encode()
    req = urllib.request.Request(DISCORD_WEBHOOK, data=payload,
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "NicheIndex-Bot/1.0"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"WARNING: Discord failed: {e}", file=sys.stderr)


def main():
    print(f"=== PIPELINE DIGEST at {datetime.now(timezone.utc).isoformat()} ===")

    ro_conn = get_readonly_conn()
    data = compute_digest(ro_conn)
    ro_conn.close()

    report = format_digest(data)
    send_discord(report)

    try:
        write_conn = get_write_conn()
        cur = write_conn.cursor()
        cur.execute("""
            INSERT INTO bot_interactions (bot_name, interaction_type, observation)
            VALUES ('digest', 'digest', %s)
        """, (json.dumps(data, default=str),))
        write_conn.commit()
        write_conn.close()
    except Exception as e:
        print(f"WARNING: Could not log interaction: {e}", file=sys.stderr)

    print("Digest complete.")


if __name__ == "__main__":
    main()
