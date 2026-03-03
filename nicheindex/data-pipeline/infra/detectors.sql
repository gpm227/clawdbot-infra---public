-- ============================================================
-- NicheIndex Pipeline Detectors
-- 7 anomaly detectors run by watchdog.py
-- All job names and thresholds must match JOB_REGISTRY.md
-- ============================================================

-- ------------------------------------------------------------
-- 1. MISSING EXPECTED JOB
-- No completed run within the lookback window.
-- Severity: mapped from JOB_REGISTRY (high→red, medium/low→yellow)
-- ------------------------------------------------------------
-- Parameters: :job_name, :lookback_hours, :severity
SELECT
  'missing_job'      AS alert_type,
  :job_name          AS job_name,
  :severity          AS severity,
  format('No completed run of %s in the last %sh.', :job_name, :lookback_hours) AS message
WHERE NOT EXISTS (
  SELECT 1 FROM pipeline_runs
  WHERE job_name = :job_name
    AND status = 'completed'
    AND completed_at >= now() - make_interval(hours => :lookback_hours)
);


-- ------------------------------------------------------------
-- 2. DUPLICATE RUNS
-- More completed runs than max_per_window in the lookback.
-- Severity: yellow
-- ------------------------------------------------------------
-- Parameters: :job_name, :lookback_hours, :max_per_window
SELECT
  'duplicate_job'    AS alert_type,
  :job_name          AS job_name,
  'yellow'           AS severity,
  format('%s ran %s times in the last %sh (expected max %s).',
    :job_name, cnt, :lookback_hours, :max_per_window) AS message
FROM (
  SELECT count(*) AS cnt
  FROM pipeline_runs
  WHERE job_name = :job_name
    AND status = 'completed'
    AND started_at >= now() - make_interval(hours => :lookback_hours)
) sub
WHERE cnt > :max_per_window;


-- ------------------------------------------------------------
-- 3. FAILED JOB
-- Any failed run in the lookback window with no subsequent completed run.
-- Severity: mapped from JOB_REGISTRY
-- ------------------------------------------------------------
-- Parameters: :job_name, :lookback_hours, :severity
SELECT
  'failed_job'       AS alert_type,
  :job_name          AS job_name,
  :severity          AS severity,
  format('%s failed at %s: %s',
    :job_name,
    f.completed_at,
    coalesce(f.error_text, 'no error details')
  ) AS message
FROM pipeline_runs f
WHERE f.job_name = :job_name
  AND f.status = 'failed'
  AND f.completed_at >= now() - make_interval(hours => :lookback_hours)
  AND NOT EXISTS (
    SELECT 1 FROM pipeline_runs r
    WHERE r.job_name = :job_name
      AND r.status = 'completed'
      AND r.completed_at > f.completed_at
  )
ORDER BY f.completed_at DESC
LIMIT 1;


-- ------------------------------------------------------------
-- 4. UNKNOWN JOB NAME
-- Job name in pipeline_runs not in the registry.
-- Severity: yellow
-- ------------------------------------------------------------
-- Parameters: :registered_names (array of all registered job names)
SELECT DISTINCT
  'unknown_job'      AS alert_type,
  p.job_name         AS job_name,
  'yellow'           AS severity,
  format('Unregistered job ''%s'' appeared in pipeline_runs.', p.job_name) AS message
FROM pipeline_runs p
WHERE p.started_at >= now() - interval '48 hours'
  AND p.job_name != ALL(:registered_names);


-- ------------------------------------------------------------
-- 5. OUTSIDE EXPECTED TIME WINDOW (DST-safe)
-- Job ran outside its expected daily window.
-- Only applies to jobs with window_start and window_end.
-- Severity: yellow
-- ------------------------------------------------------------
-- Parameters: :job_name, :window_start (time), :window_end (time), :lookback_hours
SELECT
  'outside_window'   AS alert_type,
  :job_name          AS job_name,
  'yellow'           AS severity,
  format('%s started at %s Denver time, outside window %s–%s.',
    :job_name,
    (p.started_at AT TIME ZONE 'America/Denver')::time,
    :window_start,
    :window_end
  ) AS message
FROM pipeline_runs p
WHERE p.job_name = :job_name
  AND p.started_at >= now() - make_interval(hours => :lookback_hours)
  AND (p.started_at AT TIME ZONE 'America/Denver')::time
      NOT BETWEEN :window_start::time AND :window_end::time
ORDER BY p.started_at DESC
LIMIT 1;


-- ------------------------------------------------------------
-- 6. RUNTIME ANOMALY (3x baseline)
-- Job took more than 3x the median of its last 10 completed runs.
-- Severity: yellow
-- ------------------------------------------------------------
-- Parameters: :job_name
SELECT
  'runtime_anomaly'  AS alert_type,
  :job_name          AS job_name,
  'yellow'           AS severity,
  format('%s took %ss — %sx the median baseline of %ss.',
    :job_name,
    latest.duration_seconds,
    round((latest.duration_seconds / baseline.median_dur)::numeric, 1),
    baseline.median_dur
  ) AS message
FROM (
  SELECT duration_seconds
  FROM pipeline_runs
  WHERE job_name = :job_name
    AND status = 'completed'
    AND duration_seconds IS NOT NULL
  ORDER BY completed_at DESC
  LIMIT 1
) latest
CROSS JOIN (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS median_dur
  FROM (
    SELECT duration_seconds
    FROM pipeline_runs
    WHERE job_name = :job_name
      AND status = 'completed'
      AND duration_seconds IS NOT NULL
    ORDER BY completed_at DESC
    OFFSET 1
    LIMIT 10
  ) hist
) baseline
WHERE baseline.median_dur > 0
  AND latest.duration_seconds > baseline.median_dur * 3;


-- ------------------------------------------------------------
-- 7. STUCK RUNNING JOB
-- Job in 'running' status longer than 3x the median duration
-- of its last 10 completed runs (minimum 1 hour floor).
-- Severity: red
-- ------------------------------------------------------------
-- Parameters: :job_name
SELECT
  'stuck_job'        AS alert_type,
  :job_name          AS job_name,
  'red'              AS severity,
  format('%s has been running for %s hours with 0 progress (threshold: %sh).',
    :job_name,
    round(EXTRACT(EPOCH FROM (now() - p.started_at)) / 3600.0, 1),
    round(greatest(baseline.threshold_sec, 3600) / 3600.0, 1)
  ) AS message
FROM pipeline_runs p
CROSS JOIN (
  SELECT coalesce(
    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds) * 3,
    3600
  ) AS threshold_sec
  FROM (
    SELECT duration_seconds
    FROM pipeline_runs
    WHERE job_name = :job_name
      AND status = 'completed'
      AND duration_seconds IS NOT NULL
    ORDER BY completed_at DESC
    LIMIT 10
  ) hist
) baseline
WHERE p.job_name = :job_name
  AND p.status = 'running'
  AND EXTRACT(EPOCH FROM (now() - p.started_at)) > greatest(baseline.threshold_sec, 3600);
