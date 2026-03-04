#!/usr/bin/env python3
"""
NicheIndex Job Orchestrator
Schedules and chains pipeline jobs. Runs as a persistent daemon.

Schedule:
  08:00 MST → rss-pulse
  19:30 MST → rss-pulse
  20:00 MST → daily-census (then chains post-census sequence)

Post-census sequence (starts when census completes):
  niche-trends → publication-intake → growth-delta → inactive-detector → new-entrant-detector → auto-rater

Weekly (Sunday):
  18:00 MST → media-kit-collector

Each job runs as a subprocess. On failure: retries once after 5 min.
Second failure: lets watchdog handle it (it will fire failed_job RED alert).
"""

import subprocess
import time
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2

DENVER = ZoneInfo("America/Denver")
PIPELINE_DIR = Path(__file__).parent.parent.parent.parent / "nicheindex" / "pipeline"

SCRIPTS = {
    "rss-pulse":            PIPELINE_DIR / "rss_pulse.py",
    "daily-census":         PIPELINE_DIR / "crawl.py",
    "niche-trends":         PIPELINE_DIR / "deltas.py",
    "growth-delta":         PIPELINE_DIR / "growth_delta.py",
    "inactive-detector":    PIPELINE_DIR / "inactive_detector.py",
    "new-entrant-detector": PIPELINE_DIR / "new_entrant_detector.py",
    "publication-intake":   PIPELINE_DIR / "publication_intake.py",
    "auto-rater":           PIPELINE_DIR / "auto_rater.py",
}


def run_job(job_name: str, retry: bool = False) -> bool:
    """Run a pipeline job. Returns True on success."""
    script = SCRIPTS[job_name]
    attempt = "retry" if retry else "attempt 1"
    print(f"[orchestrator] Starting {job_name} ({attempt}) at {datetime.now(DENVER).strftime('%H:%M %Z')}")

    result = subprocess.run(
        ["python3", str(script)],
        cwd=str(PIPELINE_DIR),
        capture_output=False,  # let output stream to terminal
    )

    success = result.returncode == 0
    print(f"[orchestrator] {job_name} {'OK' if success else 'FAILED'}")
    return success


def run_with_retry(job_name: str, retry_delay_min: int = 5):
    """Run job, retry once on failure. Watchdog escalates after second failure."""
    if not run_job(job_name):
        print(f"[orchestrator] {job_name} failed — waiting {retry_delay_min}min then retrying")
        time.sleep(retry_delay_min * 60)
        run_job(job_name, retry=True)
        # Don't raise — let watchdog detect and alert if still failed


def wait_for_census_completion(timeout_min: int = 90) -> bool:
    """Poll pipeline_runs until census completes or timeout."""
    dsn = os.environ["DATABASE_URL"]
    deadline = time.time() + timeout_min * 60
    print(f"[orchestrator] Waiting for daily-census to complete (max {timeout_min}min)...")

    while time.time() < deadline:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT status FROM pipeline_runs
                    WHERE job_name = 'daily-census'
                    ORDER BY started_at DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row and row[0] in ('completed', 'skipped'):
                    print("[orchestrator] Census complete — starting post-census sequence")
                    return True
                if row and row[0] == 'failed':
                    print("[orchestrator] Census failed — aborting post-census sequence")
                    return False
        time.sleep(60)  # poll every minute

    print("[orchestrator] Census timed out — aborting post-census sequence")
    return False


def run_post_census_sequence():
    """Chain: niche-trends → growth-delta → inactive-detector → new-entrant-detector"""
    for job_name in ["niche-trends", "publication-intake", "growth-delta", "inactive-detector", "new-entrant-detector", "auto-rater"]:
        run_with_retry(job_name)
        time.sleep(5)  # brief pause between jobs


def main():
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BlockingScheduler(timezone=DENVER)

    # rss-pulse: 8am + 7:30pm MST
    scheduler.add_job(lambda: run_with_retry("rss-pulse"), CronTrigger(hour=8, minute=0))
    scheduler.add_job(lambda: run_with_retry("rss-pulse"), CronTrigger(hour=19, minute=30))

    # daily-census: 8pm MST, then chain post-census sequence
    def census_and_chain():
        run_with_retry("daily-census")
        if wait_for_census_completion():
            run_post_census_sequence()

    scheduler.add_job(census_and_chain, CronTrigger(hour=20, minute=0))

    # media-kit-collector: Sunday 6pm MST (weekly, before product-intelligence)
    # NOTE: runner script does not exist yet — SOUL file is the spec
    # Uncomment when media_kit_collector.py is built:
    # scheduler.add_job(lambda: run_with_retry("media-kit-collector"),
    #                   CronTrigger(day_of_week='sun', hour=18, minute=0))

    print(f"[orchestrator] Scheduler running. Denver time: {datetime.now(DENVER).strftime('%H:%M %Z')}")
    scheduler.start()


if __name__ == "__main__":
    main()
