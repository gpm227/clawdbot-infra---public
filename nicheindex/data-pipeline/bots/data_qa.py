#!/usr/bin/env python3
"""
NicheIndex Data QA Bot
Daily post-census reconciliation: verify what pipeline scripts claim vs what's in Supabase.
Posts to #data-qa Discord channel. Logs to bot_interactions for training data.

Uses SUPABASE_READONLY_DB_URL for all reads (never writes to app tables).
Uses DATABASE_URL for bot_interactions writes only.
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
READONLY_DSN = os.environ.get("SUPABASE_READONLY_DB_URL", "")
WRITE_DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DATA_QA", os.environ.get("DISCORD_WEBHOOK_URL", ""))

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

COVERAGE_CHECKS = [
    {
        "name": "publication_activity",
        "query": """
            SELECT
                (SELECT count(*) FROM publications WHERE is_inactive != true) AS total_active,
                (SELECT count(DISTINCT pa.publication_id) FROM publication_activity pa
                 JOIN publications p ON p.id = pa.publication_id
                 WHERE p.is_inactive != true) AS with_activity
        """,
        "threshold": 0.85,
    },
    {
        "name": "niche_scores",
        "query": """
            SELECT
                (SELECT count(DISTINCT niche) FROM publications WHERE is_inactive != true AND niche IS NOT NULL) AS total_niches,
                (SELECT count(*) FROM niche_scores) AS scored_niches
        """,
        "threshold": 0.95,
    },
    {
        "name": "subscription_queue",
        "query": """
            SELECT
                (SELECT count(*) FROM subscription_queue) AS total_entries,
                (SELECT count(*) FROM subscription_queue WHERE status IN ('pending', 'candidate', 'subscribed')) AS healthy_entries
        """,
        "threshold": 0.80,
    },
]

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
# Check 2: Coverage
# ---------------------------------------------------------------------------
def check_coverage(ro_conn) -> list[dict]:
    results = []
    cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    for check in COVERAGE_CHECKS:
        try:
            cur.execute(check["query"])
            row = cur.fetchone()
            if row:
                vals = list(row.values())
                total = vals[0] or 1
                covered = vals[1] or 0
                pct = covered / total if total > 0 else 0
                passed = pct >= check["threshold"]
                results.append({
                    "name": check["name"],
                    "total": total,
                    "covered": covered,
                    "pct": round(pct * 100, 1),
                    "threshold": check["threshold"] * 100,
                    "passed": passed,
                })
        except Exception as e:
            results.append({"name": check["name"], "error": str(e), "passed": False})

    return results


# ---------------------------------------------------------------------------
# Check 3: Freshness
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
# Discord + logging
# ---------------------------------------------------------------------------
def format_discord_report(receipts, coverage, freshness) -> str:
    lines = [f"**Daily Data QA** — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"]

    lines.append("**RECEIPTS VS REALITY**")
    for r in receipts:
        if r.get("error"):
            lines.append(f"  {r['job_name']} → {r['table']}: ERROR ({r['error'][:80]})")
        elif r["status"] == "match":
            lines.append(f"  {r['job_name']} → {r['table']}: {r['reported']} reported, {r['actual']} found (match)")
        else:
            emoji = "over-reported" if r["delta"] > 0 else "under-reported"
            lines.append(f"  {r['job_name']} → {r['table']}: {r['reported']} reported, {r['actual']} found (delta: {r['delta']}, {emoji})")

    if not receipts:
        lines.append("  No completed runs in last 26h")

    lines.append("\n**COVERAGE**")
    for c in coverage:
        if c.get("error"):
            lines.append(f"  {c['name']}: ERROR ({c['error'][:80]})")
        else:
            icon = "PASS" if c["passed"] else "WARN"
            lines.append(f"  [{icon}] {c['name']}: {c['covered']}/{c['total']} ({c['pct']}%, threshold: {c['threshold']}%)")

    lines.append("\n**FRESHNESS**")
    for f in freshness:
        if f.get("error"):
            lines.append(f"  {f['table']}: ERROR ({f['error'][:80]})")
        elif f["newest"] is None:
            lines.append(f"  {f['table']}: NO DATA")
        else:
            icon = "PASS" if f["passed"] else "STALE"
            lines.append(f"  [{icon}] {f['table']}: {f['age_hours']}h old (max: {f['max_age_hours']}h)")

    return "\n".join(lines)


def send_discord(message: str):
    if not DISCORD_WEBHOOK:
        print("WARNING: No Discord webhook set. Printing to stdout.", file=sys.stderr)
        print(message)
        return
    import urllib.request
    payload = json.dumps({"content": message[:2000]}).encode()
    req = urllib.request.Request(DISCORD_WEBHOOK, data=payload,
                                 headers={"Content-Type": "application/json"})
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"=== DATA QA at {datetime.now(timezone.utc).isoformat()} ===")

    ro_conn = get_readonly_conn()

    receipts = check_receipts_vs_reality(ro_conn)
    print(f"Receipt checks: {len(receipts)}")

    coverage = check_coverage(ro_conn)
    print(f"Coverage checks: {len(coverage)}")

    freshness = check_freshness(ro_conn)
    print(f"Freshness checks: {len(freshness)}")

    ro_conn.close()

    report = format_discord_report(receipts, coverage, freshness)
    send_discord(report)

    try:
        write_conn = get_write_conn()
        log_interaction(write_conn, {
            "receipts": receipts,
            "coverage": coverage,
            "freshness": freshness,
            "has_deltas": any(r.get("delta", 0) != 0 for r in receipts if r.get("status") != "error"),
            "all_fresh": all(f.get("passed", False) for f in freshness),
        })
        write_conn.close()
    except Exception as e:
        print(f"WARNING: Could not log interaction: {e}", file=sys.stderr)

    print("Data QA complete.")


if __name__ == "__main__":
    main()
