# CLAUDE – Clawdbot Infra Repo Guide

You are Claude/Claude Code assisting with the clawdbot-infra repository.

## High-level structure

- `nicheindex/data-pipeline/` defines the NicheIndex data-pipeline monitoring team:
  - Schemas for `pipeline_runs` and `bot_alerts` (both already exist in Supabase).
  - Infra Watchdog (SQL + Python) for deterministic anomaly detection.
  - Three LLM bots that read `bot_alerts` and summarize for humans:
    - Pipeline Watchdog narrator (current state).
    - Supabase Data QA (data enrichment health).
    - Weekly Pipeline Digest (Monday summary).

## Supabase

- **Project ID:** `xgqshhwxsfspehtshqfh`
- **Region:** us-east-2
- Both `pipeline_runs` and `bot_alerts` tables are live with data.

## Rules

1. Do not change schema column names or semantics unless explicitly asked.
2. `nicheindex/data-pipeline/JOB_REGISTRY.md` is the source of truth for monitored jobs. Job names must match exactly what pipeline scripts write to `pipeline_runs.job_name`.
3. All anomaly detection logic lives in SQL in `infra/detectors.sql`.
4. `infra/watchdog.py` is a thin orchestrator — keep it small and simple.
5. LLM bots never implement detectors. They only read `bot_alerts` (and sometimes `pipeline_runs`) and talk to humans.
6. Any bot that writes to `bot_alerts` must use the dedup insert pattern. Never overwrite existing rows.

## Tasks you may be asked to perform

- Implement or modify detectors in `infra/detectors.sql`.
- Adjust thresholds or windows in `JOB_REGISTRY.md` and keep SQL in sync.
- Implement Supabase/Postgres connections in `infra/watchdog.py`.
- Refine bot SOUL files under `nicheindex/data-pipeline/bots/`.

## Before editing code

1. Read `nicheindex/INDEX.md`.
2. Read `nicheindex/data-pipeline/INDEX.md`.
3. Read `nicheindex/data-pipeline/JOB_REGISTRY.md`.
4. Follow their constraints exactly.
