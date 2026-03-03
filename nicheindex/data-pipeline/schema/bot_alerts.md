# bot_alerts – Alert Ledger

## Purpose

Record all pipeline and data-quality alerts as a durable, queryable ledger.
This table is the single source for human notifications and bot summaries.

## Ownership

- **Written by:** Infra Watchdog (SQL + Python), Supabase Data QA bot.
- **Read by:** Pipeline Watchdog narrator bot, Weekly Pipeline Digest bot, humans.

## Schema (live in Supabase)

```sql
CREATE TABLE IF NOT EXISTS bot_alerts (
  id          uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at  timestamptz DEFAULT now(),
  resolved_at timestamptz,              -- populated on auto-resolve
  alert_type  text        NOT NULL,     -- see types below
  job_name    text,
  severity    text        NOT NULL,     -- yellow | red
  message     text,
  resolved    boolean     DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_bot_alerts_open
  ON bot_alerts(alert_type, job_name)
  WHERE resolved = false;
```

## Standard alert types

**Infra Watchdog (detectors.sql):**

| alert_type | Meaning |
|---|---|
| `missing_job` | Expected job didn't run within its lookback window |
| `duplicate_job` | More runs than `max_per_window` in the lookback |
| `failed_job` | Job completed with `status='failed'` |
| `unknown_job` | Job name in `pipeline_runs` not in `JOB_REGISTRY` |
| `outside_window` | Job ran outside its expected time window |
| `runtime_anomaly` | Job took 3x+ its baseline duration |
| `stuck_job` | Job stuck in `status='running'` past adaptive threshold |

**LLM Bots:**

| alert_type | Source |
|---|---|
| `data_enrichment` | Supabase Data QA bot |

## Semantics

- A row represents the lifecycle of a single alert instance.
- `created_at` = when first detected.
- `resolved = true` + `resolved_at` = when auto-resolved by Watchdog or a bot.
- Open alerts = `resolved = false`. These power dashboards and the Watchdog narrator.
- Multiple alerts of different types can exist for the same `job_name`.

## Dedup insert pattern (required for all writers)

```sql
INSERT INTO bot_alerts (alert_type, job_name, severity, message)
SELECT :alert_type, :job_name, :severity, :message
WHERE NOT EXISTS (
  SELECT 1 FROM bot_alerts
  WHERE alert_type = :alert_type
    AND job_name IS NOT DISTINCT FROM :job_name
    AND resolved = false
);
```
