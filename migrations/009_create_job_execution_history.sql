-- Migration 009: Create job_execution_history table
-- Purpose: Store execution history for all job runs

CREATE TABLE IF NOT EXISTS ingestion_sys_ctrl.job_execution_history (
  execution_id STRING NOT NULL COMMENT 'Unique execution identifier',
  job_id STRING NOT NULL COMMENT 'Reference to scheduled_jobs.job_id',
  databricks_run_id BIGINT COMMENT 'Databricks Run ID from Jobs API',
  started_at TIMESTAMP COMMENT 'Execution start timestamp',
  finished_at TIMESTAMP COMMENT 'Execution finish timestamp',
  status STRING COMMENT 'Execution status: PENDING, RUNNING, SUCCESS, FAILED, TIMEOUT, CANCELLED',
  duration_ms BIGINT COMMENT 'Execution duration in milliseconds',
  datasets_processed INT DEFAULT 0 COMMENT 'Number of datasets successfully processed',
  datasets_failed INT DEFAULT 0 COMMENT 'Number of datasets that failed',
  datasets_total INT DEFAULT 0 COMMENT 'Total number of datasets in this execution',
  error_message STRING COMMENT 'Error message if execution failed',
  error_class STRING COMMENT 'Error class/type',
  triggered_by STRING COMMENT 'Trigger source: SCHEDULE, MANUAL, API',
  triggered_by_user STRING COMMENT 'User who triggered (for MANUAL)',
  run_page_url STRING COMMENT 'Databricks run page URL for direct access',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Job execution history and metrics'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_job_execution_job_id 
ON ingestion_sys_ctrl.job_execution_history(job_id);

CREATE INDEX IF NOT EXISTS idx_job_execution_status 
ON ingestion_sys_ctrl.job_execution_history(status);

CREATE INDEX IF NOT EXISTS idx_job_execution_started_at 
ON ingestion_sys_ctrl.job_execution_history(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_execution_databricks_run 
ON ingestion_sys_ctrl.job_execution_history(databricks_run_id);

-- Optimize table for time-based queries (partitioning by month)
-- Note: Databricks liquid clustering can be used in newer versions
OPTIMIZE ingestion_sys_ctrl.job_execution_history
ZORDER BY (job_id, started_at);
