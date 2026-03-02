-- Migration 003: Fix bronze_table and silver_table containing @DBLINK suffix
-- Problem: Table names were created with @DBLINK (e.g., "bronze_mega.CMASTER.GLO_AGENTES@CMASTERPRD")
--          which is invalid for Delta tables. @DBLINK should only be in dataset_name for Oracle queries.
-- Solution: Remove @DBLINK from bronze_table and silver_table columns

-- Update bronze_table: remove @DBLINK suffix
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET bronze_table = REGEXP_REPLACE(bronze_table, '@[A-Za-z0-9_]+$', ''),
    updated_at = CURRENT_TIMESTAMP(),
    updated_by = 'migration_003'
WHERE bronze_table LIKE '%@%';

-- Update silver_table: remove @DBLINK suffix  
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET silver_table = REGEXP_REPLACE(silver_table, '@[A-Za-z0-9_]+$', ''),
    updated_at = CURRENT_TIMESTAMP(),
    updated_by = 'migration_003'
WHERE silver_table LIKE '%@%';

-- Verification query (run after migration to confirm)
SELECT 
    dataset_id,
    dataset_name,
    bronze_table,
    silver_table,
    CASE 
        WHEN bronze_table LIKE '%@%' OR silver_table LIKE '%@%' THEN '❌ STILL HAS @DBLINK'
        ELSE '✅ FIXED'
    END as status
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_name LIKE '%@%';
