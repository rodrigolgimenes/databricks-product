-- ============================================
-- DIAGNÓSTICO: Dados Incrementais no Monitoramento
-- ============================================

-- 1. VERIFICAR SE COLUNAS EXISTEM
-- ============================================
DESCRIBE TABLE cm_dbx_dev.ingestion_sys_ops.batch_process;

-- Procure por estas colunas:
-- - load_type
-- - incremental_rows_read  
-- - watermark_start
-- - watermark_end

-- Se NÃO aparecerem, execute a migration 005:
-- ALTER TABLE cm_dbx_dev.ingestion_sys_ops.batch_process 
-- ADD COLUMN load_type STRING,
-- ADD COLUMN incremental_rows_read BIGINT,
-- ADD COLUMN watermark_start STRING,
-- ADD COLUMN watermark_end STRING;

-- ============================================
-- 2. VERIFICAR DADOS DAS ÚLTIMAS EXECUÇÕES
-- ============================================
SELECT 
  run_id,
  dataset_id,
  status,
  load_type,
  incremental_rows_read,
  watermark_start,
  watermark_end,
  bronze_row_count,
  started_at,
  finished_at
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE started_at >= CURRENT_DATE - INTERVAL 3 DAYS
ORDER BY started_at DESC
LIMIT 20;

-- INTERPRETAÇÃO:
-- - Se load_type = NULL para TODAS as linhas:
--   → Notebook não está populando os campos
--   → Precisa atualizar o notebook no Databricks
--
-- - Se load_type tem valores (FULL, INCREMENTAL, etc):
--   → ✅ Tudo está funcionando!
--   → Frontend deve mostrar os dados
--   → Se não mostra, é problema de cache do navegador

-- ============================================
-- 3. ESTATÍSTICAS DE LOAD_TYPE
-- ============================================
SELECT 
  load_type,
  COUNT(*) as count,
  COUNT(DISTINCT dataset_id) as datasets,
  AVG(CAST(incremental_rows_read AS DOUBLE)) as avg_incremental_rows,
  MIN(started_at) as first_occurrence,
  MAX(started_at) as last_occurrence
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE started_at >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY load_type
ORDER BY count DESC;

-- INTERPRETAÇÃO:
-- - Se aparecer apenas NULL:
--   → Migration existe MAS notebook não popula
--
-- - Se aparecer FULL, INCREMENTAL, etc:
--   → ✅ Sistema funcionando

-- ============================================
-- 4. VERIFICAR DATASETS COM INCREMENTAL HABILITADO
-- ============================================
SELECT 
  dc.dataset_id,
  dc.dataset_name,
  dc.enable_incremental,
  dc.incremental_strategy,
  dc.bronze_mode,
  dc.incremental_metadata,
  dc.override_watermark_value,
  COUNT(bp.run_id) as executions_last_7d,
  SUM(CASE WHEN bp.load_type = 'INCREMENTAL' THEN 1 ELSE 0 END) as incremental_executions
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control dc
LEFT JOIN cm_dbx_dev.ingestion_sys_ops.batch_process bp 
  ON dc.dataset_id = bp.dataset_id 
  AND bp.started_at >= CURRENT_DATE - INTERVAL 7 DAYS
WHERE dc.enable_incremental = TRUE
GROUP BY 
  dc.dataset_id,
  dc.dataset_name,
  dc.enable_incremental,
  dc.incremental_strategy,
  dc.bronze_mode,
  dc.incremental_metadata,
  dc.override_watermark_value
ORDER BY executions_last_7d DESC;

-- INTERPRETAÇÃO:
-- - Se enable_incremental = TRUE mas incremental_executions = 0:
--   → Dataset configurado mas não está executando como incremental
--   → Verificar se notebook está usando a configuração

-- ============================================
-- 5. TESTE RÁPIDO: INSERIR DADOS MANUALMENTE
-- ============================================
-- Para testar se frontend está OK, insira dados manualmente em uma execução recente:

-- PASSO 1: Pegar o run_id mais recente
SELECT 
  run_id,
  dataset_id,
  status,
  load_type,
  started_at
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE status = 'SUCCEEDED'
ORDER BY started_at DESC
LIMIT 1;

-- PASSO 2: Copiar o run_id acima e executar UPDATE:
-- (SUBSTITUA <RUN_ID> pelo valor real)

UPDATE cm_dbx_dev.ingestion_sys_ops.batch_process
SET 
  load_type = 'INCREMENTAL',
  incremental_rows_read = 5432,
  watermark_start = '2024-02-20 10:00:00',
  watermark_end = '2024-02-26 14:00:00'
WHERE run_id = '<RUN_ID>';

-- PASSO 3: Recarregar página de monitoramento (Ctrl+Shift+R)
--
-- Se agora aparecer:
-- ✅ Frontend OK
-- ✅ Backend OK
-- ❌ Notebook não está populando
--
-- Se NÃO aparecer:
-- ❌ Verificar se migration foi executada
-- ❌ Verificar logs do backend
-- ❌ Limpar cache do navegador completamente

-- ============================================
-- 6. VERIFICAR ÚLTIMA EXECUÇÃO DE CADA DATASET
-- ============================================
WITH last_runs AS (
  SELECT 
    dataset_id,
    run_id,
    status,
    load_type,
    incremental_rows_read,
    bronze_row_count,
    silver_row_count,
    started_at,
    ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY started_at DESC) as rn
  FROM cm_dbx_dev.ingestion_sys_ops.batch_process
  WHERE started_at >= CURRENT_DATE - INTERVAL 7 DAYS
)
SELECT 
  dc.dataset_name,
  lr.run_id,
  lr.status,
  lr.load_type,
  lr.incremental_rows_read,
  lr.bronze_row_count,
  lr.started_at,
  dc.enable_incremental,
  dc.incremental_strategy
FROM last_runs lr
JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control dc ON lr.dataset_id = dc.dataset_id
WHERE lr.rn = 1
ORDER BY lr.started_at DESC;

-- ============================================
-- 7. FORÇAR POPULAÇÃO EM MASSA (DESENVOLVIMENTO APENAS)
-- ============================================
-- ⚠️ CUIDADO: Isso é apenas para testes em ambiente de desenvolvimento!
-- ⚠️ NÃO EXECUTE EM PRODUÇÃO!

-- Popula dados de teste em todas as execuções recentes que têm load_type NULL:
/*
UPDATE cm_dbx_dev.ingestion_sys_ops.batch_process
SET 
  load_type = CASE 
    WHEN bronze_row_count > 100000 THEN 'FULL'
    ELSE 'INCREMENTAL'
  END,
  incremental_rows_read = CASE 
    WHEN bronze_row_count <= 100000 THEN CAST(bronze_row_count * 0.8 AS BIGINT)
    ELSE NULL
  END,
  watermark_start = CASE 
    WHEN bronze_row_count <= 100000 THEN CAST(started_at - INTERVAL 3 DAYS AS STRING)
    ELSE NULL
  END,
  watermark_end = CASE 
    WHEN bronze_row_count <= 100000 THEN CAST(started_at AS STRING)
    ELSE NULL
  END
WHERE load_type IS NULL
  AND started_at >= CURRENT_DATE - INTERVAL 7 DAYS;
*/

-- ============================================
-- FIM DO DIAGNÓSTICO
-- ============================================

-- RESUMO DE AÇÕES:
-- 
-- 1. Executar script item 1 (DESCRIBE) para verificar se colunas existem
-- 2. Executar script item 2 (SELECT últimas execuções) para ver dados
-- 3. Executar script item 5 (UPDATE manual) para testar frontend
-- 4. Se frontend funcionar com dados manuais:
--    → Problema é no notebook Python
--    → Atualizar notebook no Databricks
-- 5. Se frontend NÃO funcionar:
--    → Problema é migration ou backend
--    → Executar migration 005
--    → Restart do backend
