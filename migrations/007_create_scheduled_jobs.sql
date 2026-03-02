-- Migration 007: Create scheduled_jobs table
-- Purpose: Store scheduled jobs configuration and sync with Databricks Jobs API

CREATE TABLE IF NOT EXISTS ingestion_sys_ctrl.scheduled_jobs (
  job_id STRING NOT NULL COMMENT 'Unique job identifier (UUID)',
  job_name STRING NOT NULL COMMENT 'Human-readable job name',
  description STRING COMMENT 'Job description',
  schedule_type STRING NOT NULL COMMENT 'Schedule type: DAILY, WEEKLY, MONTHLY, CRON, ONCE',
  cron_expression STRING COMMENT 'Cron expression for CRON schedule type',
  timezone STRING DEFAULT 'America/Sao_Paulo' COMMENT 'Timezone for schedule execution',
  enabled BOOLEAN DEFAULT true COMMENT 'Whether job is active',
  databricks_job_id BIGINT COMMENT 'Databricks Job ID from Jobs API',
  databricks_job_state STRING COMMENT 'State in Databricks: ACTIVE, PAUSED, DELETED',
  project_id STRING COMMENT 'Project for governance',
  area_id STRING COMMENT 'Area for governance',
  max_concurrent_runs INT DEFAULT 1 COMMENT 'Maximum concurrent runs allowed',
  retry_on_timeout BOOLEAN DEFAULT true COMMENT 'Whether to retry on timeout',
  timeout_seconds INT DEFAULT 86400 COMMENT 'Job timeout in seconds (default 24h)',
  priority INT DEFAULT 100 COMMENT 'Job priority for execution ordering',
  notification_channels STRING COMMENT 'JSON with notification configs',
  notify_on ARRAY<STRING> COMMENT 'Events to notify: FAILURE, TIMEOUT, SUCCESS_AFTER_FAILURE',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Creation timestamp',
  created_by STRING COMMENT 'User who created the job',
  updated_at TIMESTAMP COMMENT 'Last update timestamp',
  updated_by STRING COMMENT 'User who last updated the job',
  last_run_at TIMESTAMP COMMENT 'Last execution timestamp',
  last_run_status STRING COMMENT 'Last execution status',
  last_run_duration_ms BIGINT COMMENT 'Last execution duration in milliseconds',
  next_run_at TIMESTAMP COMMENT 'Next scheduled execution (calculated)',
  last_sync_at TIMESTAMP COMMENT 'Last sync with Databricks timestamp'
)
USING DELTA
COMMENT 'Scheduled jobs configuration and state'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_enabled 
ON ingestion_sys_ctrl.scheduled_jobs(enabled);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_next_run 
ON ingestion_sys_ctrl.scheduled_jobs(next_run_at);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_project_area 
ON ingestion_sys_ctrl.scheduled_jobs(project_id, area_id);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_databricks_id 
ON ingestion_sys_ctrl.scheduled_jobs(databricks_job_id);

-- Add constraint for unique job name
ALTER TABLE ingestion_sys_ctrl.scheduled_jobs
ADD CONSTRAINT job_name_unique UNIQUE (job_name);
