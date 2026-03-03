#!/usr/bin/env python3
"""
NicheIndex Infra Watchdog
Thin orchestrator: resolve → detect → dedup-insert → escalate.
All detection logic lives in detectors.sql. This file only wires it up.

Environment variables:
  SUPABASE_URL         - Supabase project URL
  SUPABASE_SERVICE_KEY - Service role key
  RESEND_API_KEY       - For RED alert emails
  WATCHDOG_EMAIL_TO    - Recipient email for RED alerts
"""

import os
import sys
from typing import Any

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Job Registry — must mirror JOB_REGISTRY.md exactly
# ---------------------------------------------------------------------------
JOB_REGISTRY = {
    "rss-pulse": {
        "lookback_hours": 13,
        "max_per_window": 1,
        "severity": "high",
    },
    "daily-census": {
        "lookback_hours": 6,
        "max_per_window": 1,
        "severity": "high",
        "window": ("20:00", "21:00"),
    },
    "growth-delta": {
        "lookback_hours": 26,
        "max_per_window": 1,
        "severity": "medium",
    },
    "inactive-detector": {
        "lookback_hours": 26,
        "max_per_window": 1,
        "severity": "medium",
    },
    "new-entrant-detector": {
        "lookback_hours": 26,
        "max_per_window": 1,
        "severity": "medium",
    },
    "sponsor-collector": {
        "lookback_hours": 48,
        "max_per_window": 2,
        "severity": "low",
    },
}

SEVERITY_MAP = {"high": "red", "medium": "yellow", "low": "yellow"}


def get_db():
    """Return a psycopg2 connection to Supabase Postgres."""
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.", file=sys.stderr)
        sys.exit(1)

    # Extract host from Supabase URL: https://xxx.supabase.co → db.xxx.supabase.co
    project_ref = url.replace("https://", "").replace(".supabase.co", "")
    host = f"db.{project_ref}.supabase.co"

    return psycopg2.connect(
        host=host,
        port=5432,
        dbname="postgres",
        user="postgres",
        password=key,
        sslmode="require",
    )


# ---------------------------------------------------------------------------
# Resolution (step 1) — run before detectors
# ---------------------------------------------------------------------------
def run_resolutions(conn):
    """Auto-resolve alerts that are no longer valid."""
    cur = conn.cursor()
    for job_name, config in JOB_REGISTRY.items():
        lookback = config["lookback_hours"]

        # Resolve missing_job
        cur.execute("""
            UPDATE bot_alerts SET resolved = true, resolved_at = now()
            WHERE alert_type = 'missing_job' AND job_name = %s AND resolved = false
            AND EXISTS (
                SELECT 1 FROM pipeline_runs
                WHERE job_name = %s AND status = 'completed'
                AND completed_at >= now() - make_interval(hours => %s)
            )
        """, (job_name, job_name, lookback))

        # Resolve stuck_job
        cur.execute("""
            UPDATE bot_alerts SET resolved = true, resolved_at = now()
            WHERE alert_type = 'stuck_job' AND job_name = %s AND resolved = false
            AND NOT EXISTS (
                SELECT 1 FROM pipeline_runs
                WHERE job_name = %s AND status = 'running'
            )
        """, (job_name, job_name))

        # Resolve failed_job
        cur.execute("""
            UPDATE bot_alerts SET resolved = true, resolved_at = now()
            WHERE alert_type = 'failed_job' AND job_name = %s AND resolved = false
            AND EXISTS (
                SELECT 1 FROM pipeline_runs
                WHERE job_name = %s AND status = 'completed'
                AND completed_at >= now() - make_interval(hours => %s)
                AND completed_at > coalesce(
                    (SELECT max(completed_at) FROM pipeline_runs
                     WHERE job_name = %s AND status = 'failed'),
                    '1970-01-01'::timestamptz
                )
            )
        """, (job_name, job_name, lookback, job_name))

    conn.commit()


# ---------------------------------------------------------------------------
# Detection (step 2) — run all 7 detectors
# ---------------------------------------------------------------------------
def run_all_detectors(conn) -> list[dict[str, Any]]:
    """Execute all detector queries and return a list of alerts."""
    alerts: list[dict[str, Any]] = []
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    registered_names = list(JOB_REGISTRY.keys())

    for job_name, config in JOB_REGISTRY.items():
        lookback = config["lookback_hours"]
        severity = SEVERITY_MAP.get(config["severity"], "yellow")
        max_pw = config["max_per_window"]

        # 1. Missing job
        cur.execute("""
            SELECT 'missing_job' AS alert_type, %s AS job_name, %s AS severity,
                   format('No completed run of %%s in the last %%sh.', %s, %s) AS message
            WHERE NOT EXISTS (
                SELECT 1 FROM pipeline_runs
                WHERE job_name = %s AND status = 'completed'
                AND completed_at >= now() - make_interval(hours => %s)
            )
        """, (job_name, severity, job_name, lookback, job_name, lookback))
        alerts.extend(cur.fetchall())

        # 2. Duplicate runs
        cur.execute("""
            SELECT 'duplicate_job' AS alert_type, %s AS job_name, 'yellow' AS severity,
                   format('%%s ran %%s times in the last %%sh (expected max %%s).', %s, cnt, %s, %s) AS message
            FROM (
                SELECT count(*) AS cnt FROM pipeline_runs
                WHERE job_name = %s AND status = 'completed'
                AND started_at >= now() - make_interval(hours => %s)
            ) sub WHERE cnt > %s
        """, (job_name, job_name, lookback, max_pw, job_name, lookback, max_pw))
        alerts.extend(cur.fetchall())

        # 3. Failed job (no recovery)
        cur.execute("""
            SELECT 'failed_job' AS alert_type, %s AS job_name, %s AS severity,
                   format('%%s failed at %%s: %%s', %s, f.completed_at,
                          coalesce(f.error_text, 'no error details')) AS message
            FROM pipeline_runs f
            WHERE f.job_name = %s AND f.status = 'failed'
            AND f.completed_at >= now() - make_interval(hours => %s)
            AND NOT EXISTS (
                SELECT 1 FROM pipeline_runs r
                WHERE r.job_name = %s AND r.status = 'completed'
                AND r.completed_at > f.completed_at
            )
            ORDER BY f.completed_at DESC LIMIT 1
        """, (job_name, severity, job_name, job_name, lookback, job_name))
        alerts.extend(cur.fetchall())

        # 5. Outside window (only for jobs with windows)
        window = config.get("window")
        if window:
            cur.execute("""
                SELECT 'outside_window' AS alert_type, %s AS job_name, 'yellow' AS severity,
                       format('%%s started at %%s Denver time, outside window %%s–%%s.',
                              %s, (p.started_at AT TIME ZONE 'America/Denver')::time, %s, %s) AS message
                FROM pipeline_runs p
                WHERE p.job_name = %s
                AND p.started_at >= now() - make_interval(hours => %s)
                AND (p.started_at AT TIME ZONE 'America/Denver')::time
                    NOT BETWEEN %s::time AND %s::time
                ORDER BY p.started_at DESC LIMIT 1
            """, (job_name, job_name, window[0], window[1], job_name, lookback, window[0], window[1]))
            alerts.extend(cur.fetchall())

        # 6. Runtime anomaly (3x median)
        cur.execute("""
            SELECT 'runtime_anomaly' AS alert_type, %s AS job_name, 'yellow' AS severity,
                   format('%%s took %%ss — %%sx the median baseline.',
                          %s, latest.duration_seconds,
                          round((latest.duration_seconds / baseline.median_dur)::numeric, 1)) AS message
            FROM (
                SELECT duration_seconds FROM pipeline_runs
                WHERE job_name = %s AND status = 'completed' AND duration_seconds IS NOT NULL
                ORDER BY completed_at DESC LIMIT 1
            ) latest
            CROSS JOIN (
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS median_dur
                FROM (
                    SELECT duration_seconds FROM pipeline_runs
                    WHERE job_name = %s AND status = 'completed' AND duration_seconds IS NOT NULL
                    ORDER BY completed_at DESC OFFSET 1 LIMIT 10
                ) hist
            ) baseline
            WHERE baseline.median_dur > 0
            AND latest.duration_seconds > baseline.median_dur * 3
        """, (job_name, job_name, job_name, job_name))
        alerts.extend(cur.fetchall())

        # 7. Stuck running job
        cur.execute("""
            SELECT 'stuck_job' AS alert_type, %s AS job_name, 'red' AS severity,
                   format('%%s has been running for %%s hours.',
                          %s, round(EXTRACT(EPOCH FROM (now() - p.started_at)) / 3600.0, 1)) AS message
            FROM pipeline_runs p
            CROSS JOIN (
                SELECT coalesce(
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds) * 3, 3600
                ) AS threshold_sec
                FROM (
                    SELECT duration_seconds FROM pipeline_runs
                    WHERE job_name = %s AND status = 'completed' AND duration_seconds IS NOT NULL
                    ORDER BY completed_at DESC LIMIT 10
                ) hist
            ) baseline
            WHERE p.job_name = %s AND p.status = 'running'
            AND EXTRACT(EPOCH FROM (now() - p.started_at)) > greatest(baseline.threshold_sec, 3600)
        """, (job_name, job_name, job_name, job_name))
        alerts.extend(cur.fetchall())

    # 4. Unknown job names (runs once, not per-job)
    cur.execute("""
        SELECT DISTINCT 'unknown_job' AS alert_type, p.job_name, 'yellow' AS severity,
               format('Unregistered job ''%%s'' appeared in pipeline_runs.', p.job_name) AS message
        FROM pipeline_runs p
        WHERE p.started_at >= now() - interval '48 hours'
        AND p.job_name != ALL(%s)
    """, (registered_names,))
    alerts.extend(cur.fetchall())

    return [dict(a) for a in alerts]


# ---------------------------------------------------------------------------
# Dedup insert (step 3)
# ---------------------------------------------------------------------------
def dedup_insert_alert(conn, alert: dict[str, Any]) -> bool:
    """Insert alert if no matching open alert exists. Return True if inserted."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO bot_alerts (alert_type, job_name, severity, message)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM bot_alerts
            WHERE alert_type = %s
            AND job_name IS NOT DISTINCT FROM %s
            AND resolved = false
        )
    """, (
        alert["alert_type"], alert.get("job_name"), alert["severity"], alert.get("message"),
        alert["alert_type"], alert.get("job_name"),
    ))
    conn.commit()
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Email escalation (step 4)
# ---------------------------------------------------------------------------
def send_email_alert(alert: dict[str, Any]):
    """Send RED alert via Resend."""
    try:
        import resend
    except ImportError:
        print("WARNING: resend package not installed. Skipping email.", file=sys.stderr)
        return

    resend.api_key = os.environ.get("RESEND_API_KEY", "")
    to_email = os.environ.get("WATCHDOG_EMAIL_TO", "")
    if not resend.api_key or not to_email:
        print("WARNING: RESEND_API_KEY or WATCHDOG_EMAIL_TO not set.", file=sys.stderr)
        return

    subject = f"[WATCHDOG RED] {alert.get('job_name', 'unknown')} – {alert['alert_type']}"
    html = f"<p><strong>{alert['alert_type']}</strong>: {alert.get('message', '')}</p>"

    resend.Emails.send({
        "from": "NicheIndex Watchdog <alerts@nicheindex.co>",
        "to": [to_email],
        "subject": subject,
        "html": html,
    })


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_watchdog():
    conn = get_db()

    # Step 1: auto-resolve cleared alerts
    run_resolutions(conn)

    # Step 2: detect anomalies
    alerts = run_all_detectors(conn)

    # Step 3: dedup insert + collect new REDs
    new_red = []
    for alert in alerts:
        inserted = dedup_insert_alert(conn, alert)
        if inserted and alert["severity"] == "red":
            new_red.append(alert)

    # Step 4: escalate REDs via email
    for alert in new_red:
        send_email_alert(alert)

    # Summary
    total_new = sum(1 for a in alerts if dedup_insert_alert(conn, a) is False or True)
    open_count_cur = conn.cursor()
    open_count_cur.execute("SELECT count(*) FROM bot_alerts WHERE resolved = false")
    open_count = open_count_cur.fetchone()[0]

    print(f"Watchdog run complete. Detected: {len(alerts)}. New REDs emailed: {len(new_red)}. Open alerts: {open_count}.")
    conn.close()


if __name__ == "__main__":
    run_watchdog()
