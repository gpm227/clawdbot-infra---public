# NicheIndex Data Pipeline Team – End-to-End Spec

## Purpose

Define the complete data-pipeline observability stack for NicheIndex:

- Core schemas: `pipeline_runs`, `bot_alerts`.
- Infra Watchdog (pure SQL + Python; no LLM in the hot path).
- Three LLM bots layered on top:
  - Pipeline Watchdog narrator.
  - Supabase Data QA.
  - Weekly Pipeline Digest.

Writers (pipeline jobs) are append-only and never block on observability.
Watchdog and the bots are external observers.

## Files

| File | Purpose |
|------|---------|
| `JOB_REGISTRY.md` | Canonical list of monitored jobs (source of truth) |
| `schema/pipeline_runs.md` | Job receipt table schema |
| `schema/bot_alerts.md` | Alert ledger schema |
| `infra/README.md` | Infra Watchdog overview |
| `infra/watchdog.py` | Thin Python orchestrator |
| `infra/detectors.sql` | 7 anomaly detectors |
| `infra/resolve.sql` | Auto-resolution logic |
| `bots/pipeline-watchdog.md` | Narrates current alerts |
| `bots/supabase-data-qa.md` | Data enrichment health |
| `bots/weekly-pipeline-digest.md` | Monday summary |

## Design Principles

1. **Schema layer** defines data shape.
2. **Infra layer** runs deterministic validation + auto-resolution.
3. **Bot layer** reads `bot_alerts` (and sometimes `pipeline_runs`) and talks to humans.
4. **No LLMs in the hot path** for job execution or detection.

## LLM Write Rule

Any LLM bot that writes to `bot_alerts` must:

- Use the dedup insert pattern (INSERT ... WHERE NOT EXISTS).
- Never overwrite existing rows.
- Only set `resolved=true` via explicit resolution logic (no blind UPDATEs).
