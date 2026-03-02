-- Migration 008: Create job_datasets table
-- Purpose: Store association between jobs and datasets with execution order

CREATE TABLE IF NOT EXISTS ingestion_sys_ctrl.job_datasets (
  job_dataset_id STRING NOT NULL COMMENT 'Unique identifier for job-dataset association',
  job_id STRING NOT NULL COMMENT 'Reference to scheduled_jobs.job_id',
  dataset_id STRING NOT NULL COMMENT 'Reference to datasets.dataset_id',
  execution_order INT DEFAULT 0 COMMENT 'Order of execution within job (lower = earlier)',
  enabled BOOLEAN DEFAULT true COMMENT 'Whether this dataset is active in the job',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Association creation timestamp',
  created_by STRING COMMENT 'User who added this dataset to the job'
)
USING DELTA
COMMENT 'Association between scheduled jobs and datasets'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_job_datasets_job_id 
ON ingestion_sys_ctrl.job_datasets(job_id);

CREATE INDEX IF NOT EXISTS idx_job_datasets_dataset_id 
ON ingestion_sys_ctrl.job_datasets(dataset_id);

CREATE INDEX IF NOT EXISTS idx_job_datasets_execution_order 
ON ingestion_sys_ctrl.job_datasets(job_id, execution_order);

-- Add constraint for unique job-dataset pair
ALTER TABLE ingestion_sys_ctrl.job_datasets
ADD CONSTRAINT job_dataset_unique UNIQUE (job_id, dataset_id);
