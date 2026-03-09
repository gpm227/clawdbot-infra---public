#!/usr/bin/env python3
"""
NicheIndex Daily Health Digest — ICP-aligned scorecards.
Posts decision-readiness coverage, ICP 3 pipeline velocity, and gap analysis.

Measures what % of the index can deliver the aha moment for each ICP:
  ICP 1: "Can a reader find a good newsletter right now?"
  ICP 2: "Can a creator get competitive intelligence worth $59?"
  ICP 3: "Is sponsor intelligence growing from RSS harvest?"

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
    "icp3_pubs": 1000,
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

    # Check ni-harvest staleness
    cur.execute("""
        SELECT completed_at FROM pipeline_runs
        WHERE job_name = 'ni-harvest' AND status = 'completed'
        ORDER BY completed_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        metrics["ni_harvest_last"] = row["completed_at"]
        cur.execute("SELECT %s < now() - interval '26 hours' AS stale", (row["completed_at"],))
        metrics["ni_harvest_stale"] = cur.fetchone()["stale"]
    else:
        metrics["ni_harvest_last"] = None
        metrics["ni_harvest_stale"] = True

    # Check ni-refine staleness
    cur.execute("""
        SELECT completed_at FROM pipeline_runs
        WHERE job_name = 'ni-refine' AND status = 'completed'
        ORDER BY completed_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        metrics["ni_refine_last"] = row["completed_at"]
        cur.execute("SELECT %s < now() - interval '26 hours' AS stale", (row["completed_at"],))
        metrics["ni_refine_stale"] = cur.fetchone()["stale"]
    else:
        metrics["ni_refine_last"] = None
        metrics["ni_refine_stale"] = True

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
    """ICP 3 Sponsor Intel Ready — sponsor intelligence from RSS harvest."""
    # Total sponsor observations (excluding FPs)
    cur.execute("""
        SELECT count(*) AS total_obs
        FROM sponsor_observations
        WHERE is_false_positive != true
    """)
    total_obs = cur.fetchone()["total_obs"]

    # Unique brands
    cur.execute("""
        SELECT count(DISTINCT brand_name) AS unique_brands
        FROM sponsor_observations
        WHERE is_false_positive != true AND brand_name IS NOT NULL
    """)
    unique_brands = cur.fetchone()["unique_brands"]

    # Pubs with sponsor data
    cur.execute("""
        SELECT count(DISTINCT publication_id) AS pubs_with_sponsors
        FROM sponsor_observations
        WHERE is_false_positive != true
    """)
    pubs_with_sponsors = cur.fetchone()["pubs_with_sponsors"]

    # sponsor_summary rows
    cur.execute("SELECT count(*) AS summary_rows FROM sponsor_summary")
    summary_rows = cur.fetchone()["summary_rows"]

    # Categories with sponsor data
    cur.execute("""
        SELECT count(DISTINCT category_name) AS categories
        FROM sponsor_summary
    """)
    categories_covered = cur.fetchone()["categories"]

    # ni-harvest latest cycle
    cur.execute("""
        SELECT records_processed, records_written, duration_seconds,
               metadata->>'sponsor_observations' AS cycle_obs,
               completed_at
        FROM pipeline_runs
        WHERE job_name = 'ni-harvest' AND status = 'completed'
        ORDER BY completed_at DESC LIMIT 1
    """)
    harvest_latest = cur.fetchone()

    return {
        "total_obs": total_obs,
        "unique_brands": unique_brands,
        "pubs_with_sponsors": pubs_with_sponsors,
        "summary_rows": summary_rows,
        "categories_covered": categories_covered,
        "harvest_latest": harvest_latest,
        "light": traffic_light(pubs_with_sponsors, 500, 100),
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

    # ICP 3 gaps
    if icp3["pubs_with_sponsors"] < 1000:
        gaps.append({
            "icp": "ICP 3",
            "description": f'Sponsor coverage: {icp3["pubs_with_sponsors"]}/1000 pubs with sponsor data',
            "impact_pct": 100 - round(icp3["pubs_with_sponsors"] / 1000 * 100, 1),
            "field": "sponsor_coverage",
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
        if pipeline.get("ni_harvest_stale"):
            if pipeline.get("ni_harvest_last"):
                lines.append(f"  ** ni-harvest stale (last: {pipeline['ni_harvest_last']})")
            else:
                lines.append(f"  ** ni-harvest never completed")
        if pipeline.get("ni_refine_stale"):
            if pipeline.get("ni_refine_last"):
                lines.append(f"  ** ni-refine stale (last: {pipeline['ni_refine_last']})")
            else:
                lines.append(f"  ** ni-refine never completed")
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
        bar = progress_bar(min(icp3["pubs_with_sponsors"] / max(target, 1), 1.0))
        lines.append(f"  ICP 3 (Sponsors):     {icp3['pubs_with_sponsors']} pubs {bar}  target: {target}")

    # ICP 3 Sponsor Intel
    if icp3:
        lines.append("")
        lines.append("**ICP 3 SPONSOR INTEL**")
        lines.append(f"  Observations: {icp3['total_obs']:,} ({icp3['unique_brands']} brands, {icp3['pubs_with_sponsors']} pubs)")
        lines.append(f"  Summary table: {icp3['summary_rows']} rows across {icp3['categories_covered']} categories")
        harvest = icp3.get("harvest_latest")
        if harvest and harvest.get("completed_at"):
            ago = datetime.now(timezone.utc) - harvest["completed_at"].replace(tzinfo=timezone.utc) if harvest["completed_at"].tzinfo is None else datetime.now(timezone.utc) - harvest["completed_at"]
            hours_ago = round(ago.total_seconds() / 3600, 1)
            pubs = harvest.get("records_processed") or "?"
            obs = harvest.get("cycle_obs") or harvest.get("records_written") or "?"
            lines.append(f"  Last harvest: {hours_ago}h ago -- {pubs} pubs, {obs} obs")
        else:
            lines.append(f"  Last harvest: no completed runs")

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
