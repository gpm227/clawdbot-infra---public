-- ============================================================
-- NicheIndex Pipeline Auto-Resolution
-- Run BEFORE detectors to close alerts that are no longer valid.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Resolve missing_job when a completed run now exists
-- Parameters: :job_name, :lookback_hours
-- ------------------------------------------------------------
UPDATE bot_alerts
SET resolved = true, resolved_at = now()
WHERE alert_type = 'missing_job'
  AND job_name = :job_name
  AND resolved = false
  AND EXISTS (
    SELECT 1 FROM pipeline_runs
    WHERE job_name = :job_name
      AND status = 'completed'
      AND completed_at >= now() - make_interval(hours => :lookback_hours)
  );


-- ------------------------------------------------------------
-- 2. Resolve stuck_job when job is no longer running
-- Parameters: :job_name
-- ------------------------------------------------------------
UPDATE bot_alerts
SET resolved = true, resolved_at = now()
WHERE alert_type = 'stuck_job'
  AND job_name = :job_name
  AND resolved = false
  AND NOT EXISTS (
    SELECT 1 FROM pipeline_runs
    WHERE job_name = :job_name
      AND status = 'running'
  );


-- ------------------------------------------------------------
-- 3. Resolve failed_job when a completed run supersedes the failure
-- Parameters: :job_name, :lookback_hours
-- ------------------------------------------------------------
UPDATE bot_alerts
SET resolved = true, resolved_at = now()
WHERE alert_type = 'failed_job'
  AND job_name = :job_name
  AND resolved = false
  AND EXISTS (
    SELECT 1 FROM pipeline_runs
    WHERE job_name = :job_name
      AND status = 'completed'
      AND completed_at >= now() - make_interval(hours => :lookback_hours)
      AND completed_at > (
        SELECT max(completed_at) FROM pipeline_runs
        WHERE job_name = :job_name AND status = 'failed'
      )
  );
