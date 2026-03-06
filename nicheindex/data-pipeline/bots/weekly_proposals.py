#!/usr/bin/env python3
"""
NicheIndex Weekly Proposals — Rule-Based Proposal Engine
Runs Monday 7:15am. Reads proposal rules from jobs.yaml, evaluates each rule's
SQL query, checks condition, writes proposals to bot_directives, posts to Discord.

NO LLM. NO MEMORY. Pure rules.

Uses SUPABASE_READONLY_DB_URL for reads, DATABASE_URL for bot_directives + bot_interactions writes.
"""

import os
import re
import sys
import json
import operator
from datetime import datetime, timezone
from pathlib import Path

import yaml
import psycopg2
import psycopg2.extras

READONLY_DSN = os.environ.get("SUPABASE_READONLY_DB_URL", "")
WRITE_DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DIGEST", os.environ.get("DISCORD_WEBHOOK_URL", ""))

MAX_PROPOSALS = 3

PRIORITY_ORDER = {"critical": 0, "high": 1, "normal": 2}

# Supported comparison operators
OPS = {
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
}

# Regex: field_name  op  number
CONDITION_RE = re.compile(r"^(\w+)\s*(>=|<=|>|<|==)\s*(-?\d+(?:\.\d+)?)$")


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


def load_rules() -> list[dict]:
    """Load proposal rules from jobs.yaml -> analytics_config.proposal_rules."""
    yaml_path = Path(__file__).parent.parent / "jobs.yaml"
    if not yaml_path.exists():
        print(f"ERROR: jobs.yaml not found at {yaml_path}", file=sys.stderr)
        sys.exit(1)
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
    rules = (config.get("analytics_config") or {}).get("proposal_rules") or []
    if not rules:
        print("WARNING: No proposal_rules found in jobs.yaml")
    return rules


def evaluate_condition(condition: str, row: dict) -> bool:
    """
    Evaluate a simple condition like 'cnt >= 2' against a row dict.
    Returns False if field is None or condition cannot be parsed.
    """
    m = CONDITION_RE.match(condition.strip())
    if not m:
        print(f"  WARNING: Cannot parse condition: {condition!r}")
        return False

    field, op_str, threshold_str = m.groups()
    threshold = float(threshold_str)

    value = row.get(field)
    if value is None:
        return False

    try:
        return OPS[op_str](float(value), threshold)
    except (ValueError, TypeError):
        return False


def evaluate_rules(ro_conn) -> list[dict]:
    """Run each rule's query, check condition, return fired proposals."""
    rules = load_rules()
    fired = []

    for rule in rules:
        rule_id = rule.get("id", "unknown")
        query = rule.get("query", "")
        condition = rule.get("condition", "")
        template = rule.get("template", "")
        priority = rule.get("priority", "normal")

        if not query or not condition:
            print(f"  SKIP rule '{rule_id}': missing query or condition")
            continue

        try:
            cur = ro_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query)
            row = cur.fetchone()
            cur.close()
        except Exception as e:
            print(f"  ERROR rule '{rule_id}': query failed: {e}")
            # Don't let one bad query kill the rest
            ro_conn.rollback()
            continue

        if not row:
            print(f"  SKIP rule '{rule_id}': query returned no rows")
            continue

        if evaluate_condition(condition, row):
            try:
                message = template.format(**row)
            except (KeyError, IndexError) as e:
                print(f"  ERROR rule '{rule_id}': template format failed: {e}")
                continue
            print(f"  FIRED rule '{rule_id}' [{priority}]: {message}")
            fired.append({
                "rule_id": rule_id,
                "message": message,
                "priority": priority,
            })
        else:
            print(f"  SKIP rule '{rule_id}': condition not met ({condition}, row={dict(row)})")

    return fired


def select_proposals(fired: list[dict]) -> list[dict]:
    """Sort by priority, take top MAX_PROPOSALS."""
    fired.sort(key=lambda p: PRIORITY_ORDER.get(p["priority"], 99))
    return fired[:MAX_PROPOSALS]


def format_discord(proposals: list[dict]) -> str:
    """Format proposals for Discord."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [f"**Weekly Proposals** -- {today}", ""]

    if not proposals:
        lines.append("No proposals this week. All systems nominal.")
        return "\n".join(lines)

    for i, p in enumerate(proposals, 1):
        tag = f"[{p['priority'].upper()}] " if p["priority"] != "normal" else ""
        lines.append(f"  {i}. {tag}{p['message']}")

    lines.append("")
    lines.append("_No action needed if you don't respond. Safe default: nothing changes._")
    return "\n".join(lines)


def post_discord(message: str):
    """Post message to Discord webhook."""
    if not DISCORD_WEBHOOK:
        print("No DISCORD_WEBHOOK set, printing to stdout:")
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


def write_directives(write_conn, proposals: list[dict]):
    """Write proposals to bot_directives with status='pending'."""
    if not proposals:
        return
    cur = write_conn.cursor()
    for p in proposals:
        cur.execute("""
            INSERT INTO bot_directives (source, action, priority, status, context)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            "weekly-proposals",
            p["message"],
            p["priority"],
            "proposed",
            f"rule: {p['rule_id']}",
        ))
    write_conn.commit()
    cur.close()


def log_interaction(write_conn, proposals: list[dict]):
    """Log run to bot_interactions."""
    try:
        cur = write_conn.cursor()
        observation = {
            "proposals_fired": len(proposals),
            "rules": [{"rule_id": p["rule_id"], "priority": p["priority"]} for p in proposals],
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        cur.execute("""
            INSERT INTO bot_interactions (bot_name, interaction_type, observation)
            VALUES ('weekly-proposals', 'proposals', %s)
        """, (json.dumps(observation, default=str),))
        write_conn.commit()
        cur.close()
    except Exception as e:
        print(f"WARNING: Could not log interaction: {e}", file=sys.stderr)


def main():
    print(f"=== WEEKLY PROPOSALS at {datetime.now(timezone.utc).isoformat()} ===")

    # 1. Evaluate all rules
    ro_conn = get_readonly_conn()
    fired = evaluate_rules(ro_conn)
    ro_conn.close()

    print(f"\n{len(fired)} rule(s) fired total.")

    # 2. Select top proposals
    proposals = select_proposals(fired)
    print(f"Posting {len(proposals)} proposal(s).")

    # 3. Post to Discord
    message = format_discord(proposals)
    post_discord(message)

    # 4. Write to bot_directives + log
    write_conn = get_write_conn()
    write_directives(write_conn, proposals)
    log_interaction(write_conn, proposals)
    write_conn.close()

    print("Weekly proposals complete.")


if __name__ == "__main__":
    main()
