# Infra Watchdog

Deterministic observability layer for NicheIndex pipelines.

## Components

- `watchdog.py` — Python orchestrator (thin wrapper around SQL) + auto-remediation.
- `orchestrator.py` — APScheduler daemon: schedules all pipeline jobs + bot runners.
- `detectors.sql` — All 7 anomaly detectors.
- `resolve.sql` — Auto-resolution queries.

## Principles

- No LLM in hot path.
- `pipeline_runs` is append-only (with checkpoint updates mid-run).
- `bot_alerts` is an alert ledger with dedup writes.
- `bot_interactions` is structured training data (all bot actions logged).
- Writers never block on watchdog.
- `JOB_REGISTRY.md` is the source of truth for monitored jobs.

## Execution Order (per watchdog run)

1. **Resolve** cleared alerts (run all `UPDATE ... resolved=true` queries).
2. **Detect** anomalies across `pipeline_runs` (run all 7 detectors).
3. **Dedup-insert** new alerts into `bot_alerts`.
4. **Notify** — Discord for all alerts (primary), email for RED only (backup).
5. **Auto-remediate** — stuck jobs marked as failed automatically.
6. **Log** — all alerts and remediations written to `bot_interactions`.

Never reverse steps 1 and 2, or you risk re-raising alerts that should close.

## Stuck Detection (checkpoint-aware)

Pipeline scripts call `run.checkpoint()` periodically to write progress to DB.
The watchdog uses `last_checkpoint_at` to distinguish stuck from busy:

| Condition | Verdict |
|---|---|
| `last_checkpoint_at` exists, < 30 min old | Busy (leave alone) |
| `last_checkpoint_at` exists, > 30 min old | Stuck (auto-remediate) |
| `last_checkpoint_at` NULL, running > 2h | Stuck, old-style (auto-remediate) |
| `last_checkpoint_at` NULL, running < 2h | Too early to tell (skip) |

**Auto-remediation:** Marks stuck `pipeline_runs` row as `status='failed'`.
Next scheduled orchestrator run will retry the job.

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | Yes | Postgres connection (write access for bot_alerts + bot_interactions) |
| `RESEND_API_KEY` | Yes | For RED alert emails |
| `WATCHDOG_EMAIL_TO` | Yes | Recipient for RED alerts |
| `DISCORD_WEBHOOK_URL` | Yes | Primary alert channel |

## Orchestrator Schedule (America/Denver)

```
Daily:
  08:00 → rss-pulse
  19:30 → rss-pulse
  20:00 → daily-census → post-census chain:
    niche-trends → publication-intake → growth-delta →
    inactive-detector → new-entrant-detector → auto-rater → data-qa

Weekly:
  Monday 09:00 → pipeline-digest
  Sunday 18:00 → media-kit-collector (when built)
```

## Recommended Watchdog Schedule

Run every 10 minutes via cron or launchd.
