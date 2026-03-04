#!/usr/bin/env python3
"""
NicheIndex Infra Watchdog
Thin orchestrator: resolve → detect → dedup-insert → escalate.
All detection logic lives in detectors.sql. This file only wires it up.

Environment variables:
  DATABASE_URL          - Postgres connection string (write access)
  RESEND_API_KEY        - For RED alert emails
  WATCHDOG_EMAIL_TO     - Recipient email for RED alerts
  DISCORD_WEBHOOK_URL   - Discord webhook for all alerts (primary channel)
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
        "lookback_hours": 25,    # runs twice daily ~12h apart; 25h covers max gap
        "max_per_window": 2,     # runs AM + PM = 2 completions per 25h window
        "severity": "high",
    },
    "daily-census": {
        "lookback_hours": 26,    # can start late; run takes ~47min; 26h = safe buffer
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
    "niche-trends": {
        "lookback_hours": 26,
        "max_per_window": 1,
        "severity": "medium",
    },
    "sponsor-collector": {
        "lookback_hours": 48,
        "max_per_window": 2,
        "severity": "low",
    },
    "archive-crawl": {
        "lookback_hours": 48,
        "max_per_window": 1,
        "severity": "low",
    },
    "bulk-archive-crawl": {
        "lookback_hours": 72,
        "max_per_window": 1,
        "severity": "low",
    },
    "search-crawl": {
        "lookback_hours": 48,
        "max_per_window": 1,
        "severity": "low",
    },
    "publication-intake": {
        "lookback_hours": 48,
        "max_per_window": 2,
        "severity": "low",
    },
    "auto-rater": {
        "lookback_hours": 26,
        "max_per_window": 1,
        "severity": "medium",
    },
    "media-kit-collector": {
        "lookback_hours": 168,   # weekly
        "max_per_window": 1,
        "severity": "low",
    },
    "data-qa": {
        "lookback_hours": 26,    # daily post-census reconciliation
        "max_per_window": 1,
        "severity": "medium",
    },
    "pipeline-digest": {
        "lookback_hours": 168,   # weekly Monday 9am
        "max_per_window": 1,
        "severity": "low",
    },
}

SEVERITY_MAP = {"high": "red", "medium": "yellow", "low": "yellow"}


def get_db():
    """Return a psycopg2 connection via DATABASE_URL."""
    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        print("ERROR: DATABASE_URL must be set.", file=sys.stderr)
        sys.exit(1)
    return psycopg2.connect(dsn)


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

        # 7. Stuck running job — checkpoint-aware
        #    If last_checkpoint_at exists and is >30min old → stuck (no progress)
        #    If last_checkpoint_at is NULL and running >2h → stuck (old-style)
        #    If last_checkpoint_at is recent → busy, skip
        cur.execute("""
            SELECT 'stuck_job' AS alert_type, %s AS job_name, 'red' AS severity,
                   format('%%s has been running for %%s hours with no checkpoint progress for %%s min.',
                          %s,
                          round(EXTRACT(EPOCH FROM (now() - p.started_at)) / 3600.0, 1),
                          round(EXTRACT(EPOCH FROM (now() - coalesce(p.last_checkpoint_at, p.started_at))) / 60.0, 0)
                   ) AS message,
                   p.id AS run_id
            FROM pipeline_runs p
            WHERE p.job_name = %s AND p.status = 'running'
            AND (
                (p.last_checkpoint_at IS NOT NULL
                 AND EXTRACT(EPOCH FROM (now() - p.last_checkpoint_at)) > 1800)
                OR
                (p.last_checkpoint_at IS NULL
                 AND EXTRACT(EPOCH FROM (now() - p.started_at)) > 7200)
            )
        """, (job_name, job_name, job_name))
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
# Discord notification (step 4a — primary, all alerts)
# ---------------------------------------------------------------------------
def send_discord_alert(alert: dict[str, Any]):
    """Post alert to Discord via webhook. Primary notification channel."""
    import urllib.request
    import json as _json

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        print("WARNING: DISCORD_WEBHOOK_URL not set. Skipping Discord.", file=sys.stderr)
        return

    severity = alert["severity"].upper()
    color = 0xEF4444 if severity == "RED" else 0xF59E0B  # red / amber
    job = alert.get("job_name") or "unknown"

    payload = {
        "embeds": [{
            "title": f"[{severity}] {alert['alert_type']} — {job}",
            "description": alert.get("message", ""),
            "color": color,
        }]
    }
    try:
        data = _json.dumps(payload).encode()
        req = urllib.request.Request(webhook_url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        print(f"WARNING: Discord webhook failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Email escalation (step 4b — RED only, backup)
# ---------------------------------------------------------------------------
def send_email_alert(alert: dict[str, Any]):
    """Send RED alert via Resend (backup escalation)."""
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
# Auto-remediation (step 5) — fix stuck jobs automatically
# ---------------------------------------------------------------------------
def auto_remediate_stuck(conn, alert: dict[str, Any]):
    """Mark a stuck job as failed so the next scheduled run can retry."""
    run_id = alert.get("run_id")
    if not run_id:
        return
    cur = conn.cursor()
    cur.execute("""
        UPDATE pipeline_runs
        SET status = 'failed',
            completed_at = now(),
            error_text = 'Auto-remediated by watchdog: no checkpoint progress for 30+ min'
        WHERE id = %s AND status = 'running'
    """, (run_id,))
    conn.commit()
    if cur.rowcount > 0:
        print(f"  Auto-remediated stuck run {run_id} for {alert.get('job_name')}")


def log_bot_interaction(conn, bot_name: str, interaction_type: str,
                        observation: dict, proposed_action: str = None,
                        decision: str = None, outcome: dict = None):
    """Log a structured bot interaction for training data."""
    import json as _json
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bot_interactions (bot_name, interaction_type, observation,
                                          proposed_action, decision, outcome)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (bot_name, interaction_type,
              _json.dumps(observation), proposed_action, decision,
              _json.dumps(outcome) if outcome else None))
        conn.commit()
    except Exception as exc:
        print(f"WARNING: Failed to log bot interaction: {exc}", file=sys.stderr)
        conn.rollback()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_watchdog():
    conn = get_db()

    # Step 1: auto-resolve cleared alerts
    run_resolutions(conn)

    # Step 2: detect anomalies
    alerts = run_all_detectors(conn)

    # Step 3: dedup insert + collect newly-inserted alerts by severity
    new_alerts = []
    new_red = []
    for alert in alerts:
        inserted = dedup_insert_alert(conn, alert)
        if inserted:
            new_alerts.append(alert)
            if alert["severity"] == "red":
                new_red.append(alert)

    # Step 4a: Discord for all new alerts (primary)
    for alert in new_alerts:
        send_discord_alert(alert)

    # Step 4b: email for RED only (backup escalation)
    for alert in new_red:
        send_email_alert(alert)

    # Step 5: Auto-remediate stuck jobs
    for alert in new_alerts:
        if alert["alert_type"] == "stuck_job":
            auto_remediate_stuck(conn, alert)
            log_bot_interaction(
                conn,
                bot_name="watchdog",
                interaction_type="remediation",
                observation={
                    "alert_type": "stuck_job",
                    "job_name": alert.get("job_name"),
                    "message": alert.get("message"),
                },
                proposed_action="Mark stuck run as failed for next-cycle retry",
                decision="auto-executed",
                outcome={"action": "marked_failed", "run_id": str(alert.get("run_id", ""))},
            )

    # Log all non-stuck alerts as observations (training data)
    for alert in new_alerts:
        if alert["alert_type"] != "stuck_job":
            log_bot_interaction(
                conn,
                bot_name="watchdog",
                interaction_type="observation",
                observation={
                    "alert_type": alert["alert_type"],
                    "job_name": alert.get("job_name"),
                    "severity": alert["severity"],
                    "message": alert.get("message"),
                },
            )

    # Summary
    open_count_cur = conn.cursor()
    open_count_cur.execute("SELECT count(*) FROM bot_alerts WHERE resolved = false")
    open_count = open_count_cur.fetchone()[0]

    print(f"Watchdog run complete. Detected: {len(alerts)}. New alerts: {len(new_alerts)} ({len(new_red)} RED). Open alerts: {open_count}.")
    conn.close()


if __name__ == "__main__":
    run_watchdog()
