"""
Auto-fix module for data_qa.py.
Handles safe operational fixes that don't need human approval.
"""

import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

PIPELINE_DIR = Path('/home/ubuntu/nicheindex/pipeline')
PIPELINE_PYTHON = str(PIPELINE_DIR / '.venv/bin/python3')


def check_recent_decisions(write_conn, recommendation_text, hours=48):
    """Check if we already proposed this recommendation recently."""
    cur = write_conn.cursor()
    cur.execute("""
        SELECT id, approved FROM decision_log
        WHERE recommendation LIKE %s
          AND created_at > now() - interval '%s hours'
        ORDER BY created_at DESC LIMIT 1
    """, (f'%{recommendation_text[:80]}%', hours))
    row = cur.fetchone()
    if row:
        return {'id': row[0], 'approved': row[1]}
    return None


def execute_auto_fixes(ro_conn, write_conn):
    """Run safe auto-fixes and return a list of actions taken."""
    actions = []

    # Fix 1: Re-queue stuck subscription_queue entries
    cur = ro_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM subscription_queue
        WHERE status = 'processing'
          AND processed_at < now() - interval '2 hours'
    """)
    stuck = cur.fetchone()[0]
    if stuck > 0:
        wcur = write_conn.cursor()
        wcur.execute("""
            UPDATE subscription_queue
            SET status = 'pending', processed_at = now()
            WHERE status = 'processing'
              AND processed_at < now() - interval '2 hours'
        """)
        write_conn.commit()
        actions.append(f'Re-queued {stuck} stuck subscription_queue entries')
        log_auto_fix(write_conn, 're-queue-stuck', f'Re-queued {stuck} stuck entries')

    # Fix 2: Clear stale pipeline_runs that are stuck in 'running'
    cur.execute("""
        SELECT job_name, COUNT(*) FROM pipeline_runs
        WHERE status = 'running'
          AND started_at < now() - interval '3 hours'
        GROUP BY job_name
    """)
    stale_runs = cur.fetchall()
    for job_name, count in stale_runs:
        wcur = write_conn.cursor()
        wcur.execute("""
            UPDATE pipeline_runs
            SET status = 'failed', completed_at = now()
            WHERE job_name = %s AND status = 'running'
              AND started_at < now() - interval '3 hours'
        """, (job_name,))
        write_conn.commit()
        actions.append(f'Marked {count} stale {job_name} run(s) as failed')
        log_auto_fix(write_conn, 'clear-stale-run', f'Marked {count} stale {job_name} runs as failed')

    return actions


def log_auto_fix(write_conn, action_type, description):
    """Record an auto-fix in decision_log."""
    cur = write_conn.cursor()
    try:
        cur.execute("""
            INSERT INTO decision_log (bot_name, category, recommendation, action_taken, approved, approved_by, resolved_at)
            VALUES ('data-qa', 'auto-fix', %s, %s, true, 'auto', now())
        """, (description, action_type))
        write_conn.commit()
    except Exception:
        write_conn.rollback()


def filter_duplicate_recommendations(write_conn, recommendations):
    """Remove recommendations that were recently denied or are still pending."""
    filtered = []
    for rec in recommendations:
        recent = check_recent_decisions(write_conn, rec['recommendation'])
        if recent and recent['approved'] is False:
            continue  # Recently denied — don't re-propose
        if recent and recent['approved'] is None:
            continue  # Still pending — don't duplicate
        filtered.append(rec)
    return filtered
