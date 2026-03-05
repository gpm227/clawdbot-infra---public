#!/usr/bin/env python3
"""
NicheIndex Inbox Scorer Bot
v1 rule-based content quality scorer for inbound newsletter emails.

Reads unscored emails from inbound_emails, applies a heuristic rubric,
writes ni_content_quality to publications, and recomputes NI Rating.

Uses DATABASE_URL (read/write) for all operations.
Uses PipelineRun from the nicheindex pipeline for job receipts.

Schedule: 2x daily (9am + 9pm MST) via orchestrator.
"""

import os
import re
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from html.parser import HTMLParser

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# PipelineRun import — lives in nicheindex/pipeline/qc.py
# From this file:  clawdbot-infra/nicheindex/data-pipeline/bots/inbox_scorer.py
# To qc.py:        nicheindex/pipeline/qc.py
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "nicheindex" / "pipeline"))
from qc import PipelineRun

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DSN = os.environ.get("DATABASE_URL", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DIGEST", os.environ.get("DISCORD_WEBHOOK_URL", ""))
MAX_EMAILS_PER_RUN = 200
MIN_EMAILS_FOR_FULL_MODE = 3
BLEND_OLD_WEIGHT = 0.7
BLEND_NEW_WEIGHT = 0.3

# ---------------------------------------------------------------------------
# Signal markers (compiled once)
# ---------------------------------------------------------------------------
ORIGINAL_VOICE_MARKERS = re.compile(
    r"\b(in my (?:view|experience|opinion)|here'?s what (?:i think|struck me)|"
    r"i'?ve (?:been|seen|noticed|spent|worked)|my take|when i (?:was|worked|started)|"
    r"i (?:believe|argue|suspect|contend|predict)|from my perspective|"
    r"let me (?:explain|share|tell you)|personally|in my career)\b",
    re.IGNORECASE,
)

DATA_EVIDENCE_MARKERS = re.compile(
    r"(?:\d+(?:\.\d+)?%|\$\d[\d,.]*|according to|data (?:shows?|suggests?|indicates?)|"
    r"research (?:shows?|finds?|suggests?)|study (?:found|shows?)|survey (?:of|found)|"
    r"year[- ]over[- ]year|quarter[- ]over[- ]quarter|month[- ]over[- ]month)",
    re.IGNORECASE,
)

AI_SLOP_MARKERS = re.compile(
    r"\b(in today'?s rapidly (?:evolving|changing)|it'?s worth noting|"
    r"game[- ]?changer|dive (?:deep|in)|let'?s (?:dive|unpack|explore)|"
    r"at the end of the day|navigate (?:the|this) (?:landscape|complex)|"
    r"in (?:this|the) ever[- ](?:evolving|changing)|unlock (?:the|your)|"
    r"leverage (?:the|this|your)|it'?s important to (?:note|remember)|"
    r"in conclusion|without further ado|buckle up|"
    r"the (?:landscape|world) of .{3,30} is (?:changing|evolving)|"
    r"a comprehensive (?:guide|look|overview)|"
    r"key takeaway|actionable (?:insights?|tips?|strategies?))\b",
    re.IGNORECASE,
)

TRANSACTIONAL_SUBJECT = re.compile(
    r"\b(confirm your|verify your|welcome to|password reset|"
    r"billing|invoice|receipt|subscription (?:confirmed|activated)|"
    r"account (?:created|activated)|sign up|unsubscribe)\b",
    re.IGNORECASE,
)

# NI Rating label thresholds (same as auto_rater.py)
RATING_LABELS = [
    (7.5, "Category Leader"),
    (5.0, "Niche Authority"),
    (2.5, "Emerging"),
    (0.0, "Weak"),
]


# ---------------------------------------------------------------------------
# HTML stripping
# ---------------------------------------------------------------------------
class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._text = []

    def handle_data(self, data):
        self._text.append(data)

    def get_text(self):
        return " ".join(self._text)


def strip_html(html: str) -> str:
    """Strip HTML tags and return plain text."""
    s = _HTMLStripper()
    try:
        s.feed(html)
        return s.get_text()
    except Exception:
        # Fallback: crude tag removal
        return re.sub(r"<[^>]+>", " ", html)


def count_links(html: str) -> int:
    """Count <a href> tags in HTML."""
    return len(re.findall(r"<a\s[^>]*href", html, re.IGNORECASE))


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------
def score_email(subject: str, body_text: str, body_html: str) -> tuple[float, str]:
    """
    Score a single email on content quality (0.0 - 2.0).
    Returns (score, reason).
    """
    # Skip transactional/promotional emails
    if TRANSACTIONAL_SUBJECT.search(subject or ""):
        return 0.0, "Transactional/promotional email (subject match)"

    # Get text content — prefer body_text, fall back to stripped HTML
    text = (body_text or "").strip()
    if not text and body_html:
        text = strip_html(body_html)
    text = text.strip()

    if not text or len(text) < 50:
        return 0.0, "No meaningful content (< 50 chars)"

    word_count = len(text.split())

    # --- AI slop detection (check first — can cap the score) ---
    ai_hits = len(AI_SLOP_MARKERS.findall(text))
    if ai_hits >= 3:
        return 0.25, f"AI slop detected ({ai_hits} markers in {word_count} words)"

    # --- Start from baseline 1.0, adjust up/down ---
    score = 1.0
    reasons = []

    # Word count signals
    if word_count > 1500:
        score += 0.3
        reasons.append(f"long-form ({word_count}w)")
    elif word_count > 800:
        score += 0.15
        reasons.append(f"medium-form ({word_count}w)")
    elif word_count < 200:
        score -= 0.5
        reasons.append(f"very short ({word_count}w)")

    # Original voice signals
    voice_hits = len(ORIGINAL_VOICE_MARKERS.findall(text))
    if voice_hits >= 3:
        score += 0.4
        reasons.append(f"strong original voice ({voice_hits} markers)")
    elif voice_hits >= 1:
        score += 0.2
        reasons.append(f"some original voice ({voice_hits} markers)")

    # Data/evidence signals
    data_hits = len(DATA_EVIDENCE_MARKERS.findall(text))
    if data_hits >= 2:
        score += 0.2
        reasons.append(f"data-backed ({data_hits} evidence markers)")

    # Link dump detection (negative)
    link_count = count_links(body_html or "")
    if link_count > 20 and word_count < 500:
        score -= 0.2
        reasons.append(f"link dump ({link_count} links in {word_count}w)")

    # Mild AI slop (1-2 markers — penalize but don't cap)
    if 1 <= ai_hits <= 2:
        score -= 0.15
        reasons.append(f"mild AI markers ({ai_hits})")

    # Clamp to [0.0, 2.0]
    score = max(0.0, min(2.0, round(score, 2)))

    reason = "; ".join(reasons) if reasons else f"baseline ({word_count}w)"
    return score, reason


# ---------------------------------------------------------------------------
# Rating recomputation
# ---------------------------------------------------------------------------
def assign_label(rating: float) -> str:
    """Assign NI Rating label based on score thresholds."""
    for threshold, label in RATING_LABELS:
        if rating >= threshold:
            return label
    return "Weak"


def recompute_rating(
    ni_publishing_consistency: float | None,
    ni_engagement_proxy: float | None,
    ni_content_quality: float,
    ni_audience_score: float | None,
) -> dict:
    """
    Recompute ni_editorial_score and ni_rating.
    Formula: editorial = avg(consistency, engagement, quality) * 5
             rating = avg(audience, editorial)
    """
    # Editorial score: average of the 3 editorial dimensions * 5
    consistency = ni_publishing_consistency or 0.0
    engagement = ni_engagement_proxy or 0.0
    editorial = (consistency + engagement + ni_content_quality) / 3.0 * 5.0
    editorial = round(editorial, 2)

    # Overall rating: average of audience + editorial
    audience = ni_audience_score or 0.0
    rating = (audience + editorial) / 2.0
    rating = round(rating, 2)

    return {
        "ni_editorial_score": editorial,
        "ni_rating": rating,
        "ni_rating_label": assign_label(rating),
    }


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def get_conn():
    if not DSN:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    return psycopg2.connect(DSN)


def fetch_unscored_emails(conn, limit: int = MAX_EMAILS_PER_RUN) -> list[dict]:
    """Fetch unprocessed inbound emails that have a publication_id."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ie.id, ie.recipient, ie.sender, ie.subject,
               ie.body_text, ie.body_html, ie.publication_id,
               ie.received_at
        FROM inbound_emails ie
        WHERE ie.processed = false
          AND ie.publication_id IS NOT NULL
        ORDER BY ie.received_at ASC
        LIMIT %s
    """, (limit,))
    return cur.fetchall()


def fetch_pub_ni_columns(conn, pub_id: int) -> dict | None:
    """Fetch current NI rating columns for a publication."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ni_content_quality, ni_publishing_consistency, ni_engagement_proxy,
               ni_audience_score, ni_editorial_score, ni_rating, ni_rating_mode,
               ni_rating_label, ni_signals_scored
        FROM publications
        WHERE id = %s
    """, (pub_id,))
    return cur.fetchone()


def count_scored_emails(conn, pub_id: int) -> int:
    """Count how many emails we've already scored for this publication."""
    cur = conn.cursor()
    cur.execute("""
        SELECT count(*) FROM inbound_emails
        WHERE publication_id = %s AND processed = true
    """, (pub_id,))
    return cur.fetchone()[0]


def update_publication(conn, pub_id: int, updates: dict):
    """Update publication NI columns."""
    if not updates:
        return
    set_clauses = []
    values = []
    for key, val in updates.items():
        set_clauses.append(f"{key} = %s")
        values.append(val)
    set_clauses.append("ni_rated_at = now()")
    values.append(pub_id)

    cur = conn.cursor()
    cur.execute(
        f"UPDATE publications SET {', '.join(set_clauses)} WHERE id = %s",
        values,
    )
    conn.commit()


def mark_email_processed(conn, email_id):
    """Mark an inbound email as processed."""
    cur = conn.cursor()
    cur.execute(
        "UPDATE inbound_emails SET processed = true WHERE id = %s",
        (email_id,),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Discord notification (only for anomalies)
# ---------------------------------------------------------------------------
def send_discord(message: str):
    if not DISCORD_WEBHOOK:
        print(message)
        return
    import urllib.request
    payload = json.dumps({"content": message[:2000]}).encode()
    req = urllib.request.Request(
        DISCORD_WEBHOOK, data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "NicheIndex-Bot/1.0"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"WARNING: Discord failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"=== INBOX SCORER at {datetime.now(timezone.utc).isoformat()} ===")

    run = PipelineRun(job_name="inbox-scorer")

    conn = get_conn()
    scored_count = 0
    pubs_updated = set()
    mode_upgrades = 0
    errors = 0

    try:
        emails = fetch_unscored_emails(conn)
        print(f"Found {len(emails)} unscored emails")

        if not emails:
            run.complete()
            conn.close()
            print("No emails to score. Done.")
            return

        for email in emails:
            try:
                pub_id = email["publication_id"]
                subject = email["subject"] or ""
                body_text = email["body_text"] or ""
                body_html = email["body_html"] or ""

                # Score this email
                new_score, reason = score_email(subject, body_text, body_html)
                print(f"  pub={pub_id} score={new_score} reason='{reason}'")

                # Fetch current publication NI data
                pub = fetch_pub_ni_columns(conn, pub_id)
                if not pub:
                    print(f"  WARNING: pub_id={pub_id} not found, skipping")
                    mark_email_processed(conn, email["id"])
                    run.increment(processed=1, skipped=1)
                    continue

                # Blend with existing score (70% old + 30% new)
                existing_quality = pub["ni_content_quality"]
                if existing_quality is not None:
                    blended = float(existing_quality) * BLEND_OLD_WEIGHT + new_score * BLEND_NEW_WEIGHT
                else:
                    blended = new_score
                blended = round(blended, 2)

                # Count total scored emails for this pub (including this one)
                prior_scored = count_scored_emails(conn, pub_id)
                total_scored = prior_scored + 1

                # Build update dict
                updates = {"ni_content_quality": blended}

                # Check for mode upgrade: need 3+ emails and not already full
                current_mode = pub["ni_rating_mode"]
                if total_scored >= MIN_EMAILS_FOR_FULL_MODE and current_mode != "full":
                    updates["ni_rating_mode"] = "full"
                    updates["ni_signals_scored"] = 6
                    mode_upgrades += 1
                    print(f"  UPGRADE pub={pub_id} to full mode ({total_scored} emails scored)")

                # Recompute editorial + rating
                rating_data = recompute_rating(
                    ni_publishing_consistency=float(pub["ni_publishing_consistency"]) if pub["ni_publishing_consistency"] is not None else None,
                    ni_engagement_proxy=float(pub["ni_engagement_proxy"]) if pub["ni_engagement_proxy"] is not None else None,
                    ni_content_quality=blended,
                    ni_audience_score=float(pub["ni_audience_score"]) if pub["ni_audience_score"] is not None else None,
                )
                updates.update(rating_data)

                # Write to DB
                update_publication(conn, pub_id, updates)
                mark_email_processed(conn, email["id"])

                scored_count += 1
                pubs_updated.add(pub_id)
                run.increment(processed=1, written=1)

                # Checkpoint every 25 emails
                if scored_count % 25 == 0:
                    run.checkpoint()

            except Exception as e:
                errors += 1
                run.increment(processed=1, failed=1)
                print(f"  ERROR scoring email {email.get('id')}: {e}", file=sys.stderr)
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Final summary
        run.add_metadata("scored_count", scored_count)
        run.add_metadata("pubs_updated", len(pubs_updated))
        run.add_metadata("mode_upgrades", mode_upgrades)
        run.add_metadata("errors", errors)

        run.complete()

        print(f"\n=== SUMMARY ===")
        print(f"  Emails scored:  {scored_count}")
        print(f"  Pubs updated:   {len(pubs_updated)}")
        print(f"  Mode upgrades:  {mode_upgrades}")
        print(f"  Errors:         {errors}")

        # Discord alert only if >50 mode upgrades (unusual activity)
        if mode_upgrades > 50:
            send_discord(
                f"**Inbox Scorer Alert** — {mode_upgrades} publications upgraded to full mode "
                f"in a single run. {scored_count} emails scored, {len(pubs_updated)} pubs updated."
            )

    except Exception as e:
        run.fail(error_text=str(e)[:500])
        print(f"FATAL: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
