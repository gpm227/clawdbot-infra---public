# NicheIndex – Clawdbot Data Pipeline Team

This folder defines the autonomous data-pipeline monitoring team for NicheIndex:

- **Infra Watchdog:** deterministic SQL + Python that monitors pipeline jobs.
- **Shared observability storage:** `pipeline_runs` and `bot_alerts` in Supabase.
- **Three LLM bots** running under Clawdbot:
  - Pipeline Watchdog narrator (real-time alert summary).
  - Supabase Data QA (data enrichment health audit).
  - Weekly Pipeline Digest (Monday executive summary).

See `data-pipeline/INDEX.md` for the full architecture and bot specs.
