-- Migration 010: Add job_id column to run_queue
-- Purpose: Support scheduled jobs by linking queue items to specific jobs

-- Add job_id column (nullable for backward compatibility)
-- NOTE: run_queue lives in ingestion_sys_ops, NOT ingestion_sys_ctrl
ALTER TABLE ingestion_sys_ops.run_queue
ADD COLUMN job_id STRING COMMENT 'Reference to scheduled_jobs.job_id (NULL for legacy manual runs)';

-- Create index for filtering by job_id
CREATE INDEX IF NOT EXISTS idx_run_queue_job_id 
ON ingestion_sys_ops.run_queue(job_id);

-- Create composite index for job-based queue processing
CREATE INDEX IF NOT EXISTS idx_run_queue_job_status 
ON ingestion_sys_ops.run_queue(job_id, status, priority DESC, requested_at ASC);

-- Update table comment
COMMENT ON TABLE ingestion_sys_ops.run_queue IS 
'Queue for dataset processing. job_id/correlation_id link to scheduled_jobs for automated runs, NULL for manual/legacy runs';
