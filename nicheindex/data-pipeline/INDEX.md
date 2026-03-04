# NicheIndex Data Pipeline Team – End-to-End Spec

## Purpose

Define the complete data-pipeline observability stack for NicheIndex:

- Core schemas: `pipeline_runs`, `bot_alerts`.
- Infra Watchdog (pure SQL + Python; no LLM in the hot path).
- Bot team layered on top for narration, QA, and intelligence.

Writers (pipeline jobs) are append-only and never block on observability.
Watchdog and the bots are external observers.

## Infra (LIVE)

| File | Purpose |
|------|---------|
| `infra/watchdog.py` | 7-detector alert engine |
| `infra/orchestrator.py` | APScheduler daemon (rss-pulse × 2, census + post-census chain) |
| `infra/detectors.sql` | Raw detector queries |
| `infra/resolve.sql` | Alert resolution queries |

## Bot SOUL Files

| File | Status | Purpose |
|------|--------|---------|
| `bots/pipeline-watchdog.md` | LIVE | Real-time alert summary |
| `bots/supabase-data-qa.md` | LIVE | Data enrichment health audit |
| `bots/weekly-pipeline-digest.md` | LIVE | Monday executive summary |
| `bots/product-intelligence.md` | LIVE | Sunday PM unsurfaced data signals |
| `bots/inbox-scorer.md` | PENDING BUILD | Content quality scoring from inbox |
| `bots/media-kit-collector.md` | PENDING BUILD | Post-subscribe media kit scraper |

## Schema Docs

| File | Purpose |
|------|---------|
| `schema/pipeline_runs.md` | Job receipt table schema |
| `schema/bot_alerts.md` | Alert ledger schema |

## Registry

| File | Purpose |
|------|---------|
| `JOB_REGISTRY.md` | Canonical list of monitored jobs |

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
