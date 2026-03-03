# clawdbot-infra

Pipeline monitoring and data quality infrastructure for [NicheIndex](https://nicheindex.co).

## Architecture

```
Writers (pipeline scripts)
    │
    ▼
pipeline_runs (append-only receipts)
    │
    ▼
Infra Watchdog (SQL detectors + Python orchestrator)
    │
    ▼
bot_alerts (alert ledger)
    │
    ▼
LLM Bots (narrate alerts to humans)
```

- **Writers** are pipeline cron jobs. They append to `pipeline_runs`. They never block on observability.
- **Infra Watchdog** runs deterministic SQL detectors, dedup-inserts alerts, and emails REDs via Resend.
- **LLM Bots** read `bot_alerts` and summarize for humans. They never perform detection logic.

## Setup

1. Ensure `pipeline_runs` and `bot_alerts` tables exist in Supabase (see `nicheindex/data-pipeline/schema/`).
2. Set environment variables: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `RESEND_API_KEY`, `WATCHDOG_EMAIL_TO`.
3. Run `python nicheindex/data-pipeline/infra/watchdog.py` on a cron (every 10 minutes recommended).
4. Configure Clawdbot bot entries using the SOUL files in `nicheindex/data-pipeline/bots/`.

## Supabase Project

- **Project ID:** `xgqshhwxsfspehtshqfh`
- **Region:** us-east-2
