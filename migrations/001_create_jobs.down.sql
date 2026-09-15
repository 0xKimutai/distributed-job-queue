DROP TRIGGER IF EXISTS set_updated_at ON jobs;

DROP FUNCTION IF EXISTS trigger_set_updated_at();

DROP INDEX IF EXISTS idx_jobs_pending_priority;

DROP TABLE IF EXISTS jobs;
