-- ====================================================================
-- Script: Cleanup Orphan Jobs
-- Descrição: Marca jobs RUNNING/PENDING há mais de 2 horas como FAILED
-- Uso: Execute no Databricks SQL Editor ou via API
-- ====================================================================

-- 1. DIAGNÓSTICO: Identifica jobs órfãos
SELECT 
  queue_id, 
  dataset_id, 
  status, 
  started_at,
  TIMESTAMPDIFF(MINUTE, started_at, CURRENT_TIMESTAMP()) as minutes_stuck
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE status IN ('RUNNING', 'PENDING')
  AND started_at IS NOT NULL
  AND finished_at IS NULL
  AND started_at < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
ORDER BY started_at ASC;

-- 2. CORREÇÃO: Marca como FAILED com erro de timeout
UPDATE cm_dbx_dev.ingestion_sys_ops.run_queue
SET 
  status = 'FAILED',
  last_error_class = 'TIMEOUT',
  last_error_message = 'Job órfão detectado - excedeu 2 horas sem finalizar (cleanup manual)',
  finished_at = CURRENT_TIMESTAMP()
WHERE status IN ('RUNNING', 'PENDING')
  AND started_at IS NOT NULL
  AND finished_at IS NULL
  AND started_at < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS;

-- 3. VERIFICAÇÃO: Confirma que não há mais órfãos
SELECT COUNT(*) as remaining_orphans
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE status IN ('RUNNING', 'PENDING')
  AND started_at IS NOT NULL
  AND finished_at IS NULL
  AND started_at < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS;
