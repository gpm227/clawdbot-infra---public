#!/usr/bin/env python3
"""
NicheIndex Job Orchestrator (YAML-driven)
Reads job definitions from ../jobs.yaml and schedules them via APScheduler.

Usage:
  python orchestrator.py              # Run as persistent daemon
  python orchestrator.py --list       # Show all scheduled jobs
  python orchestrator.py --run <job>  # Run a single job immediately (with retry)

Schedule and chain logic is defined entirely in jobs.yaml.
"""

import argparse
import subprocess
import sys
import time
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import yaml

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

YAML_PATH = Path(__file__).parent.parent / "jobs.yaml"


def load_config() -> dict:
    """Load and resolve jobs.yaml into an absolute-path config."""
    with open(YAML_PATH) as f:
        cfg = yaml.safe_load(f)

    yaml_dir = YAML_PATH.parent.resolve()
    cfg["_pipeline_dir"] = (yaml_dir / cfg["pipeline_dir"]).resolve()
    cfg["_bots_dir"] = (yaml_dir / cfg["bots_dir"]).resolve()
    cfg["_tz"] = ZoneInfo(cfg["timezone"])
    return cfg


def resolve_script(cfg: dict, job: dict) -> Path:
    """Return the absolute path to a job's script."""
    base = cfg["_pipeline_dir"] if job["location"] == "pipeline" else cfg["_bots_dir"]
    return base / job["script"]


def get_enabled_jobs(cfg: dict) -> dict:
    """Return only jobs where enabled is not False."""
    return {
        name: spec
        for name, spec in cfg["jobs"].items()
        if spec.get("enabled", True)
    }


def get_chain_jobs(cfg: dict, parent: str) -> list[tuple[str, dict]]:
    """Collect jobs chained after `parent`, sorted by chain_order."""
    chain = []
    for name, spec in get_enabled_jobs(cfg).items():
        if spec.get("chain_after") == parent:
            chain.append((name, spec))
    chain.sort(key=lambda x: x[1].get("chain_order", 999))
    return chain


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------

def run_job(cfg: dict, job_name: str, retry: bool = False) -> bool:
    """Run a pipeline job as a subprocess. Returns True on success."""
    tz = cfg["_tz"]
    jobs = cfg["jobs"]
    if job_name not in jobs:
        print(f"[orchestrator] Unknown job: {job_name}")
        return False

    script = resolve_script(cfg, jobs[job_name])
    # Use the script's parent dir as cwd so relative imports work
    cwd = str(script.parent)
    attempt = "retry" if retry else "attempt 1"
    print(f"[orchestrator] Starting {job_name} ({attempt}) at {datetime.now(tz).strftime('%H:%M %Z')}")

    result = subprocess.run(
        ["python3", str(script)],
        cwd=cwd,
        capture_output=False,  # let output stream to terminal
    )

    success = result.returncode == 0
    print(f"[orchestrator] {job_name} {'OK' if success else 'FAILED'}")
    return success


def run_with_retry(cfg: dict, job_name: str, retry_delay_min: int = 5):
    """Run job, retry once on failure. Watchdog escalates after second failure."""
    if not run_job(cfg, job_name):
        print(f"[orchestrator] {job_name} failed — waiting {retry_delay_min}min then retrying")
        time.sleep(retry_delay_min * 60)
        run_job(cfg, job_name, retry=True)
        # Don't raise — let watchdog detect and alert if still failed


# ---------------------------------------------------------------------------
# Chain execution
# ---------------------------------------------------------------------------

def wait_for_job_completion(cfg: dict, job_name: str, timeout_min: int = 90) -> bool:
    """Poll pipeline_runs until a job completes or timeout."""
    dsn = os.environ["DATABASE_URL"]
    tz = cfg["_tz"]
    deadline = time.time() + timeout_min * 60
    print(f"[orchestrator] Waiting for {job_name} to complete (max {timeout_min}min)...")

    while time.time() < deadline:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT status FROM pipeline_runs
                    WHERE job_name = %s
                    ORDER BY started_at DESC LIMIT 1
                """, (job_name,))
                row = cur.fetchone()
                if row and row[0] in ('completed', 'skipped'):
                    print(f"[orchestrator] {job_name} complete — starting chain")
                    return True
                if row and row[0] == 'failed':
                    print(f"[orchestrator] {job_name} failed — aborting chain")
                    return False
        time.sleep(60)  # poll every minute

    print(f"[orchestrator] {job_name} timed out — aborting chain")
    return False


def run_chain(cfg: dict, parent: str):
    """Run all jobs chained after `parent`, in chain_order."""
    chain = get_chain_jobs(cfg, parent)
    if not chain:
        return
    names = [name for name, _ in chain]
    print(f"[orchestrator] Post-{parent} chain: {' → '.join(names)}")
    for job_name, _ in chain:
        run_with_retry(cfg, job_name)
        time.sleep(5)  # brief pause between jobs


# ---------------------------------------------------------------------------
# Schedule builder
# ---------------------------------------------------------------------------

def build_schedule(cfg: dict):
    """Create APScheduler jobs from jobs.yaml config."""
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    tz = cfg["_tz"]
    scheduler = BlockingScheduler(timezone=tz)
    enabled_jobs = get_enabled_jobs(cfg)

    # Find which jobs are chain parents (have children chained after them)
    chain_parents = set()
    for name, spec in enabled_jobs.items():
        parent = spec.get("chain_after")
        if parent:
            chain_parents.add(parent)

    for name, spec in enabled_jobs.items():
        schedules = spec.get("schedule")
        if not schedules:
            continue  # chain-only jobs have no cron schedule

        is_chain_parent = name in chain_parents

        for cron_kwargs in schedules:
            # Convert any string values to proper types for APScheduler
            # e.g., hour: "*/4" stays as string — CronTrigger accepts it
            trigger = CronTrigger(**cron_kwargs, timezone=tz)

            if is_chain_parent:
                # Parent job: run it, wait for completion, then run chain
                def make_parent_fn(job_name=name):
                    def fn():
                        run_with_retry(cfg, job_name)
                        if wait_for_job_completion(cfg, job_name):
                            run_chain(cfg, job_name)
                    return fn
                scheduler.add_job(make_parent_fn(), trigger, id=f"{name}-{cron_kwargs}")
            else:
                # Regular cron job
                def make_fn(job_name=name):
                    return lambda: run_with_retry(cfg, job_name)
                scheduler.add_job(make_fn(), trigger, id=f"{name}-{cron_kwargs}")

    return scheduler


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def list_jobs(cfg: dict):
    """Print all jobs and their schedules."""
    enabled_jobs = get_enabled_jobs(cfg)
    all_jobs = cfg["jobs"]
    tz_name = cfg["timezone"]

    print(f"\nNicheIndex Job Schedule ({tz_name})")
    print("=" * 65)

    # Cron-scheduled jobs
    print("\nCron-scheduled:")
    for name, spec in all_jobs.items():
        schedules = spec.get("schedule")
        if not schedules:
            continue
        enabled = spec.get("enabled", True)
        status = "" if enabled else " [DISABLED]"
        for cron in schedules:
            parts = ", ".join(f"{k}={v}" for k, v in cron.items())
            print(f"  {name:<25} {parts}{status}")

    # Chain jobs
    print("\nPost-census chain:")
    chain = []
    for name, spec in all_jobs.items():
        if spec.get("chain_after") == "daily-census":
            chain.append((spec.get("chain_order", 999), name, spec))
    chain.sort()
    for order, name, spec in chain:
        enabled = spec.get("enabled", True)
        status = "" if enabled else " [DISABLED]"
        print(f"  {order}. {name}{status}")

    # Disabled jobs
    disabled = [n for n, s in all_jobs.items() if not s.get("enabled", True)]
    if disabled:
        print(f"\nDisabled ({len(disabled)}): {', '.join(disabled)}")

    print()


def main():
    parser = argparse.ArgumentParser(description="NicheIndex Job Orchestrator")
    parser.add_argument("--list", action="store_true", help="Show all scheduled jobs")
    parser.add_argument("--run", metavar="JOB", help="Run a single job immediately")
    args = parser.parse_args()

    cfg = load_config()
    tz = cfg["_tz"]

    if args.list:
        list_jobs(cfg)
        return

    if args.run:
        job_name = args.run
        if job_name not in cfg["jobs"]:
            print(f"[orchestrator] Unknown job: {job_name}")
            print(f"[orchestrator] Available: {', '.join(cfg['jobs'].keys())}")
            sys.exit(1)
        run_with_retry(cfg, job_name)
        return

    # Daemon mode
    scheduler = build_schedule(cfg)
    print(f"[orchestrator] Scheduler running. Denver time: {datetime.now(tz).strftime('%H:%M %Z')}")
    print(f"[orchestrator] Loaded {len(get_enabled_jobs(cfg))} enabled jobs from jobs.yaml")
    scheduler.start()


if __name__ == "__main__":
    main()
