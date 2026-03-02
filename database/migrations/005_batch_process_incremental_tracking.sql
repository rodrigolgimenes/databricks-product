-- ============================================================================
-- Migration 005: Batch Process Incremental Tracking
-- ============================================================================
-- Adiciona campos para rastrear informações de carga incremental:
-- - Tipo de carga (FULL vs INCREMENTAL)
-- - Quantidade de linhas incrementais processadas
-- - Range do watermark usado na execução
-- ============================================================================

-- Adicionar colunas de tracking incremental na tabela batch_process
ALTER TABLE cm_dbx_dev.ingestion_sys_ops.batch_process ADD COLUMNS (
  load_type STRING COMMENT 'Tipo de carga executada: FULL | INCREMENTAL | SNAPSHOT',
  incremental_rows_read BIGINT COMMENT 'Número de linhas lidas na carga incremental (apenas novos/atualizados)',
  watermark_start STRING COMMENT 'Valor inicial do watermark usado (para cargas incrementais)',
  watermark_end STRING COMMENT 'Valor final do watermark após a execução'
);

-- ============================================================================
-- Validação: Verificar colunas criadas
-- ============================================================================
DESCRIBE TABLE cm_dbx_dev.ingestion_sys_ops.batch_process;

-- ============================================================================
-- Query de teste para verificar funcionamento
-- ============================================================================
SELECT 
  bp.run_id,
  bp.dataset_id,
  bp.status,
  bp.load_type,
  bp.incremental_rows_read,
  bp.bronze_row_count,
  bp.silver_row_count,
  bp.watermark_start,
  bp.watermark_end,
  CAST(TIMESTAMPDIFF(SECOND, bp.started_at, bp.finished_at) AS BIGINT) AS duration_seconds,
  dc.dataset_name,
  dc.incremental_strategy,
  dc.enable_incremental
FROM cm_dbx_dev.ingestion_sys_ops.batch_process bp
LEFT JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control dc 
  ON bp.dataset_id = dc.dataset_id
ORDER BY bp.started_at DESC
LIMIT 10;

-- ============================================================================
-- NOTAS IMPORTANTES
-- ============================================================================
-- 1. Campos são NULLABLE - execuções antigas terão valores NULL
-- 2. load_type deve ser populado pelo notebook Python durante execução:
--    - "FULL" para carga completa (SNAPSHOT)
--    - "INCREMENTAL" para carga incremental (WATERMARK/HASH_MERGE)
-- 3. incremental_rows_read deve contar APENAS as linhas novas/atualizadas
--    (não confundir com bronze_row_count que pode incluir dados históricos)
-- 4. watermark_start/end devem registrar o range temporal da execução incremental
-- 5. Estes campos alimentam as colunas "Tipo Carga" e "Δ Incremental" no UI
-- ============================================================================

-- ============================================================================
-- EXEMPLO DE INSERT com novos campos (para referência do notebook Python)
-- ============================================================================
-- INSERT INTO cm_dbx_dev.ingestion_sys_ops.batch_process
--   (run_id, dataset_id, queue_id, execution_mode, status, 
--    started_at, finished_at, bronze_row_count, silver_row_count,
--    load_type, incremental_rows_read, watermark_start, watermark_end,
--    created_at, created_by)
-- VALUES (
--   '{run_id}', '{dataset_id}', '{queue_id}', '{execution_mode}', '{status}',
--   TIMESTAMP '{started_at}', TIMESTAMP '{finished_at}', {bronze_count}, {silver_count},
--   'INCREMENTAL', 2500, '2024-02-20 00:00:00', '2024-02-26 23:59:59',
--   current_timestamp(), 'orchestrator'
-- );
-- ============================================================================
