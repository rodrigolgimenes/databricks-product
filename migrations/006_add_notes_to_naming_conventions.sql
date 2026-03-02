-- Migration 006: Add notes column to naming_conventions table
-- Run this in Databricks SQL or via MCP
--
-- STATUS: ✅ EXECUTED SUCCESSFULLY ON 2026-02-26
-- This migration has been applied to cm_dbx_dev.ingestion_sys_ctrl.naming_conventions

ALTER TABLE cm_dbx_dev.ingestion_sys_ctrl.naming_conventions 
ADD COLUMN notes STRING 
COMMENT 'Description or comments about this naming convention';

-- Verify the column was added
-- DESCRIBE TABLE cm_dbx_dev.ingestion_sys_ctrl.naming_conventions;
