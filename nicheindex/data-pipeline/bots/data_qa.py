#!/usr/bin/env python3
"""
NicheIndex Data QA Bot
Daily post-census reconciliation + target-based quality measurement.

Three check categories:
  1. Receipts vs Reality — do pipeline_runs match actual DB counts?
  2. Coverage & Freshness — are tables populated and current?
  3. Target Measurement — measure actuals vs data_targets.yaml per quality lens

Posts to #data-qa Discord channel. Logs to bot_interactions + decision_log.

Uses SUPABASE_READONLY_DB_URL for all reads (never writes to app tables).
Uses DATABASE_URL for bot_interactions and decision_log writes only.
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
READONLY_DSN = os.environ.get("SUPABASE_READONLY_DB_URL", "")
WRITE_DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DATA_QA", os.environ.get("DISCORD_WEBHOOK_URL", ""))

BOTS_DIR = Path(__file__).parent
DATA_DIR = BOTS_DIR.parent
TARGETS_FILE = DATA_DIR / "data_targets.yaml"

# Mapping: job_name -> [(target_table, count_column, date_column)]
JOB_TARGET_MAP = {
    "daily-census": [
        ("publications", "id", "updated_at"),
        ("publication_snapshots", "id", "created_at"),
    ],
    "archive-crawl": [
        ("publication_activity", "id", "updated_at"),
    ],
    "bulk-archive-crawl": [
        ("publication_activity", "id", "updated_at"),
    ],
    "niche-trends": [
        ("niche_trends", "id", "computed_at"),
    ],
    "search-crawl": [
        ("publications", "id", "first_seen_at"),
    ],
    "auto-rater": [
        ("publications", "id", "ni_rated_at"),
    ],
}

FRESHNESS_TABLES = [
    ("publications", "updated_at", 48),
    ("publication_snapshots", "created_at", 48),
    ("publication_activity", "updated_at", 72),
    ("niche_trends", "computed_at", 48),
    ("niche_scores", "computed_at", 168),
    ("subscription_queue", "discovered_at", 72),
]


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
# Check 1: Receipt vs Reality
# ---------------------------------------------------------------------------
def check_receipts_vs_reality(ro_conn) -> list[dict]:
    """Compare pipeline_runs records_written with actual DB counts."""
    results = []
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, job_name, started_at, completed_at, records_written, records_processed
        FROM pipeline_runs
        WHERE status = 'completed'
        AND completed_at >= now() - interval '26 hours'
        ORDER BY completed_at DESC
    """)
    runs = cur.fetchall()

    for run in runs:
        job = run["job_name"]
        targets = JOB_TARGET_MAP.get(job)
        if not targets:
            continue

        for table, count_col, date_col in targets:
            try:
                cur.execute(f"""
                    SELECT count(*) AS actual
                    FROM {table}
                    WHERE {date_col} >= %s AND {date_col} <= %s
                """, (run["started_at"], run["completed_at"] + timedelta(minutes=5)))
                row = cur.fetchone()
                actual = row["actual"] if row else 0
                reported = run["records_written"] or 0
                delta = reported - actual

                status = "match" if abs(delta) <= 1 else ("over" if delta > 0 else "under")
                results.append({
                    "job_name": job,
                    "table": table,
                    "reported": reported,
                    "actual": actual,
                    "delta": delta,
                    "status": status,
                    "run_id": str(run["id"]),
                    "completed_at": run["completed_at"].isoformat(),
                })
            except Exception as e:
                results.append({
                    "job_name": job,
                    "table": table,
                    "error": str(e),
                    "status": "error",
                })

    return results


# ---------------------------------------------------------------------------
# Check 2: Freshness
# ---------------------------------------------------------------------------
def check_freshness(ro_conn) -> list[dict]:
    results = []
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    for table, date_col, max_age_hours in FRESHNESS_TABLES:
        try:
            cur.execute(f"""
                SELECT max({date_col}) AS newest,
                       extract(epoch from (now() - max({date_col}))) / 3600.0 AS age_hours
                FROM {table}
            """)
            row = cur.fetchone()
            if row and row["newest"]:
                age_h = round(row["age_hours"], 1)
                passed = age_h <= max_age_hours
                results.append({
                    "table": table,
                    "newest": row["newest"].isoformat(),
                    "age_hours": age_h,
                    "max_age_hours": max_age_hours,
                    "passed": passed,
                })
            else:
                results.append({"table": table, "newest": None, "passed": False})
        except Exception as e:
            results.append({"table": table, "error": str(e), "passed": False})

    return results


# ---------------------------------------------------------------------------
# Check 3: Target Measurement (NEW — reads data_targets.yaml)
# ---------------------------------------------------------------------------
def load_targets() -> dict:
    """Load data_targets.yaml."""
    if not TARGETS_FILE.exists():
        print(f"WARNING: {TARGETS_FILE} not found. Skipping target checks.", file=sys.stderr)
        return {}
    with open(TARGETS_FILE) as f:
        return yaml.safe_load(f)


def measure_targets(ro_conn) -> list[dict]:
    """Measure each target from data_targets.yaml against actual DB state."""
    targets = load_targets()
    if not targets:
        return []

    results = []
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    for lens_name, checks in targets.items():
        if not isinstance(checks, dict):
            continue
        for check_name, spec in checks.items():
            if not isinstance(spec, dict) or 'query' not in spec:
                continue

            try:
                cur.execute(spec['query'])
                row = cur.fetchone()
                actual = row['actual'] if row and 'actual' in row else 0

                # Determine target and type
                if 'target_pct' in spec:
                    # Percentage target — need denominator
                    denom = 1
                    if 'denominator_query' in spec:
                        cur.execute(spec['denominator_query'])
                        drow = cur.fetchone()
                        denom = list(drow.values())[0] if drow else 1
                    pct = round((actual / denom * 100) if denom > 0 else 0, 1)
                    target_val = spec['target_pct']
                    passed = pct >= target_val
                    gap = round(pct - target_val, 1)
                    results.append({
                        "lens": lens_name,
                        "check": check_name,
                        "metric": spec.get('metric', check_name),
                        "actual": actual,
                        "actual_pct": pct,
                        "target_pct": target_val,
                        "gap_pct": gap,
                        "passed": passed,
                        "severity": spec.get('severity', 'medium'),
                    })
                elif 'target' in spec:
                    # Absolute target
                    target_val = spec['target']
                    passed = actual >= target_val
                    gap = actual - target_val
                    results.append({
                        "lens": lens_name,
                        "check": check_name,
                        "metric": spec.get('metric', check_name),
                        "actual": actual,
                        "target": target_val,
                        "gap": gap,
                        "passed": passed,
                        "severity": spec.get('severity', 'medium'),
                    })

            except Exception as e:
                results.append({
                    "lens": lens_name,
                    "check": check_name,
                    "metric": spec.get('metric', check_name),
                    "error": str(e),
                    "passed": False,
                    "severity": spec.get('severity', 'medium'),
                })

    return results


# ---------------------------------------------------------------------------
# Recommendations engine
# ---------------------------------------------------------------------------
def generate_recommendations(target_results) -> list[dict]:
    """Generate operational or strategic recommendations based on gaps."""
    recs = []
    for t in target_results:
        if t.get('passed', True):
            continue
        if t.get('error'):
            continue

        gap_info = ""
        if 'gap_pct' in t:
            gap_info = f"{t['actual_pct']}% vs {t['target_pct']}% target (gap: {t['gap_pct']}%)"
        elif 'gap' in t:
            gap_info = f"{t['actual']} vs {t['target']} target (gap: {t['gap']})"

        # Classify as operational vs strategic
        severity = t.get('severity', 'medium')
        check = t.get('check', '')

        if check in ('census_chain', 'archive_crawl'):
            category = 'operational'
            action = f"Check if {check} ran. Verify orchestrator logs."
        elif check == 'events_populated':
            category = 'operational'
            action = "Events table may not have emitting chain jobs yet. Check if rising-scorer ran."
        elif 'rss' in check:
            category = 'strategic'
            action = "RSS failure rate too high. Recommend: run auto-discovery on failing feeds."
        elif 'ad_ratio' in check:
            category = 'strategic'
            action = "Ad detector not yet shipped. Blocked on Week 2 pipeline work."
        else:
            category = 'strategic'
            action = f"Gap in {t.get('lens', 'unknown')} lens: {t.get('metric', check)}."

        recs.append({
            "check": check,
            "lens": t.get('lens', 'unknown'),
            "category": category,
            "severity": severity,
            "gap": gap_info,
            "recommendation": action,
        })

    return recs


# ---------------------------------------------------------------------------
# Discord + logging
# ---------------------------------------------------------------------------
def format_discord_report(receipts, freshness, target_results, recommendations) -> str:
    lines = [f"**Daily Data QA** — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"]

    # Receipts
    lines.append("**RECEIPTS VS REALITY**")
    for r in receipts:
        if r.get("error"):
            lines.append(f"  {r['job_name']} → {r['table']}: ERROR ({r['error'][:80]})")
        elif r["status"] == "match":
            lines.append(f"  {r['job_name']} → {r['table']}: {r['actual']} (match)")
        else:
            lines.append(f"  {r['job_name']} → {r['table']}: {r['reported']} reported, {r['actual']} found (delta: {r['delta']})")
    if not receipts:
        lines.append("  No completed runs in last 26h")

    # Freshness
    lines.append("\n**FRESHNESS**")
    for f in freshness:
        if f.get("error"):
            lines.append(f"  {f['table']}: ERROR")
        elif f["newest"] is None:
            lines.append(f"  {f['table']}: NO DATA")
        else:
            icon = "OK" if f["passed"] else "STALE"
            lines.append(f"  [{icon}] {f['table']}: {f['age_hours']}h old (max: {f['max_age_hours']}h)")

    # Target results by lens
    if target_results:
        lines.append("\n**QUALITY TARGETS**")

        # Group by lens
        by_lens = {}
        for t in target_results:
            lens = t.get('lens', 'unknown')
            by_lens.setdefault(lens, []).append(t)

        for lens, checks in by_lens.items():
            lines.append(f"\n  __{lens}__")
            for c in checks:
                if c.get('error'):
                    lines.append(f"    [ERR] {c.get('metric', c['check'])}: {c['error'][:60]}")
                elif c['passed']:
                    if 'actual_pct' in c:
                        lines.append(f"    [OK] {c.get('metric', c['check'])}: {c['actual_pct']}%")
                    else:
                        lines.append(f"    [OK] {c.get('metric', c['check'])}: {c['actual']}")
                else:
                    if 'gap_pct' in c:
                        lines.append(f"    [GAP] {c.get('metric', c['check'])}: {c['actual_pct']}% (target: {c['target_pct']}%)")
                    else:
                        lines.append(f"    [GAP] {c.get('metric', c['check'])}: {c['actual']} (target: {c['target']})")

    # Recommendations
    if recommendations:
        lines.append("\n**RECOMMENDATIONS**")
        for r in recommendations:
            icon = "AUTO" if r['category'] == 'operational' else "REVIEW"
            lines.append(f"  [{icon}] [{r['severity'].upper()}] {r['recommendation']}")
            lines.append(f"         Gap: {r['gap']}")

    return "\n".join(lines)


def send_discord(message: str):
    if not DISCORD_WEBHOOK:
        print("WARNING: No Discord webhook set. Printing to stdout.", file=sys.stderr)
        print(message)
        return
    import urllib.request
    payload = json.dumps({"content": message[:2000]}).encode()
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


def log_interaction(write_conn, observation: dict):
    cur = write_conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bot_interactions (bot_name, interaction_type, observation)
            VALUES ('data-qa', 'observation', %s)
        """, (json.dumps(observation),))
        write_conn.commit()
    except Exception as e:
        print(f"WARNING: Failed to log interaction: {e}", file=sys.stderr)
        write_conn.rollback()


def log_recommendations(write_conn, recommendations: list[dict]):
    """Log strategic recommendations to decision_log for learning."""
    cur = write_conn.cursor()
    for rec in recommendations:
        if rec['category'] != 'strategic':
            continue
        try:
            cur.execute("""
                INSERT INTO decision_log (bot_name, category, recommendation)
                VALUES ('data-qa', %s, %s)
            """, (rec['category'], f"[{rec['lens']}] {rec['recommendation']} | Gap: {rec['gap']}"))
        except Exception as e:
            print(f"WARNING: Failed to log recommendation: {e}", file=sys.stderr)
            write_conn.rollback()
            return
    write_conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"=== DATA QA at {datetime.now(timezone.utc).isoformat()} ===")

    ro_conn = get_readonly_conn()

    receipts = check_receipts_vs_reality(ro_conn)
    print(f"Receipt checks: {len(receipts)}")

    freshness = check_freshness(ro_conn)
    print(f"Freshness checks: {len(freshness)}")

    target_results = measure_targets(ro_conn)
    print(f"Target checks: {len(target_results)}")
    passed = sum(1 for t in target_results if t.get('passed'))
    failed = sum(1 for t in target_results if not t.get('passed'))
    print(f"  Passed: {passed}, Gaps: {failed}")

    recommendations = generate_recommendations(target_results)
    print(f"Recommendations: {len(recommendations)}")

    ro_conn.close()

    report = format_discord_report(receipts, freshness, target_results, recommendations)
    send_discord(report)

    try:
        write_conn = get_write_conn()
        log_interaction(write_conn, {
            "receipts": receipts,
            "freshness": freshness,
            "targets": target_results,
            "recommendations": recommendations,
            "targets_passed": passed,
            "targets_failed": failed,
        })
        if recommendations:
            log_recommendations(write_conn, recommendations)
        write_conn.close()
    except Exception as e:
        print(f"WARNING: Could not log interaction: {e}", file=sys.stderr)

    print("Data QA complete.")


if __name__ == "__main__":
    main()
