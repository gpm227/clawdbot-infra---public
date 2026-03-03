# pipeline_runs – Job Receipt Table

## Purpose

Track every pipeline job execution as an append-only receipt so infra and bots
can see what ran, when, and how it behaved.

## Ownership

- **Written by:** pipeline jobs (cron scripts, daemons, orchestrator).
- **Read by:** Watchdog SQL, Watchdog Python wrapper, all three LLM bots, ad-hoc analytics.

## Schema (live in Supabase)

```sql
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id                uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
  job_name          text        NOT NULL,
  started_at        timestamptz DEFAULT now() NOT NULL,
  completed_at      timestamptz,                -- NULL when status='running'
  status            text        DEFAULT 'running' NOT NULL,
                                                -- running | completed | failed
  records_processed integer     DEFAULT 0,
  records_written   integer     DEFAULT 0,
  records_skipped   integer     DEFAULT 0,
  records_failed    integer     DEFAULT 0,
  duration_seconds  numeric,
  qc_checks         jsonb       DEFAULT '{}'::jsonb,
  metadata          jsonb       DEFAULT '{}'::jsonb,
  error_text        text
);
```

## Conventions

- One row per job attempt.
- Writers append new rows; they do not delete history.
- Writers may update a running row to set `status='completed'` or `status='failed'` and fill `completed_at`.
- Logical success is `status = 'completed'`.
- "Stuck" jobs are `status = 'running'` with no `completed_at` after an anomalously long time.
- The Infra Watchdog treats job names listed in `JOB_REGISTRY.md` as canonical. Other names become `unknown_job` alerts.

## Extra columns (beyond basic receipt)

These columns are available for richer bot analysis:

- `records_processed` / `records_written` / `records_skipped` / `records_failed` — volume metrics per run.
- `duration_seconds` — wall clock time.
- `qc_checks` — JSONB blob with job-specific quality checks (structure varies by job).
- `metadata` — JSONB blob with job-specific context (e.g., crawl parameters, batch IDs).
- `error_text` — human-readable error description on failure.
