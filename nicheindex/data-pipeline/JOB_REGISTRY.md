# Canonical Job Registry

This file defines the authoritative list of monitored pipeline jobs.

The Infra Watchdog must mirror this exactly. Job names here must match
what pipeline scripts write to `pipeline_runs.job_name`.

**Timezone:** America/Denver (DST-aware)

| job_name               | lookback_hours | max_per_window | severity | window_start | window_end | notes |
|------------------------|----------------|----------------|----------|--------------|------------|-------|
| rss-pulse              | 25             | 2              | high     |              |            | Runs AM + PM (~12h apart); 25h covers max gap; expect 2 completions |
| daily-census           | 26             | 1              | high     | 20:00        | 21:00      | Nightly snapshot crawl; can start late, takes ~47min; 26h safe buffer |
| growth-delta           | 26             | 1              | medium   |              |            | Runs post-census; 26h covers overnight + buffer |
| inactive-detector      | 26             | 1              | medium   |              |            | Runs post-census |
| new-entrant-detector   | 26             | 1              | medium   |              |            | Runs post-census |
| niche-trends           | 26             | 1              | medium   |              |            | deltas.py; runs post-census |
| sponsor-collector      | 48             | 2              | low      |              |            | Batch subscription runs |
| archive-crawl          | 48             | 1              | low      |              |            | crawl_archives.py; twice-weekly rolling refresh |
| bulk-archive-crawl     | 72             | 1              | low      |              |            | bulk_archive_crawl.py; overnight backfill; takes several hours |
| search-crawl           | 48             | 1              | low      |              |            | search_crawl.py; weekly discovery crawl |

## Jobs NOT yet monitored

These jobs exist in the architecture but do not yet write receipts to `pipeline_runs`.
Add them here once their scripts include receipt writes.

- `sponsor-parser` — Pipeline scaffolded, not built.

## Rules

- Jobs not listed here trigger `unknown_job` (YELLOW).
- A job name appearing in `pipeline_runs` that isn't in this registry is a signal to either add it here or fix the script.
- Never add a job to this registry until its script reliably writes `pipeline_runs` receipts.
- Lookback windows should be generous enough to avoid false alarms from normal scheduling jitter (use 1.5-2x the expected interval).
