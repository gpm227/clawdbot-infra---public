# Infra Watchdog

Deterministic observability layer for NicheIndex pipelines.

## Components

- `watchdog.py` — Python orchestrator (thin wrapper around SQL).
- `detectors.sql` — All 7 anomaly detectors.
- `resolve.sql` — Auto-resolution queries.

## Principles

- No LLM in hot path.
- `pipeline_runs` is append-only.
- `bot_alerts` is an alert ledger with dedup writes.
- Writers never block on watchdog.
- `JOB_REGISTRY.md` is the source of truth for monitored jobs.

## Execution Order (per run)

1. **Resolve** cleared alerts (run all `UPDATE ... resolved=true` queries).
2. **Detect** anomalies across `pipeline_runs` (run all 7 detectors).
3. **Dedup-insert** new alerts into `bot_alerts`.
4. **Escalate** — for each new RED alert, send email via Resend.

Never reverse steps 1 and 2, or you risk re-raising alerts that should close.

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Yes | Service role key (not anon) |
| `RESEND_API_KEY` | Yes | For RED alert emails |
| `WATCHDOG_EMAIL_TO` | Yes | Recipient for RED alerts |

## Recommended Schedule

Run every 10 minutes via cron or launchd.
