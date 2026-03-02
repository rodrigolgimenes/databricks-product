-- Migration 005: Create naming_audit_log table for tracking all naming changes
-- Run this in Databricks SQL or via MCP
--
-- STATUS: ✅ EXECUTED SUCCESSFULLY ON 2026-02-26
-- This migration has been applied to cm_dbx_dev.ingestion_sys_ctrl

CREATE TABLE IF NOT EXISTS cm_dbx_dev.ingestion_sys_ctrl.naming_audit_log (
  audit_id STRING NOT NULL,
  dataset_id STRING,
  operation_type STRING NOT NULL,  -- 'BULK_RENAME', 'INDIVIDUAL_RENAME', 'CONVENTION_CHANGE'
  old_bronze_table STRING,
  new_bronze_table STRING,
  old_silver_table STRING,
  new_silver_table STRING,
  old_naming_version INT,
  new_naming_version INT,
  performed_by STRING NOT NULL,
  performed_at TIMESTAMP NOT NULL,
  change_reason STRING,
  metadata STRING  -- JSON string with additional context
)
USING DELTA
COMMENT 'Audit log for all table naming changes in the governance platform';

-- Note: Table is MANAGED by Unity Catalog, no LOCATION needed
-- Indexes and permissions are managed by Unity Catalog and Databricks
