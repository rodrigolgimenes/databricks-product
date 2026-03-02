-- =====================================================================
-- LIMPEZA COMPLETA DO SISTEMA DE INGESTÃO GOVERNADA
-- =====================================================================
-- ATENÇÃO: Este script DELETA TODOS OS DADOS de controle e operações
-- Execute apenas em ambientes de desenvolvimento/teste!
-- =====================================================================

-- 1. LIMPAR FILA DE EXECUÇÃO (run_queue)
DELETE FROM cm_dbx_dev.ingestion_sys_ops.run_queue;

-- 2. LIMPAR HISTÓRICO DE PROCESSOS (batch_process)
DELETE FROM cm_dbx_dev.ingestion_sys_ops.batch_process;

-- 3. LIMPAR DETALHES DE TABELAS (batch_process_table_details)
DELETE FROM cm_dbx_dev.ingestion_sys_ops.batch_process_table_details;

-- 4. LIMPAR STEPS DE EXECUÇÃO (batch_process_steps)
DELETE FROM cm_dbx_dev.ingestion_sys_ops.batch_process_steps;

-- 5. DELETAR TODOS OS DATASETS CADASTRADOS
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control;

-- 6. DELETAR VERSÕES DE SCHEMA
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.schema_versions;

-- 7. LIMPAR AUDITORIAS DE DATASET
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_audit;

-- =====================================================================
-- VERIFICAÇÃO: Checar se tudo foi limpo
-- =====================================================================

SELECT 'run_queue' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ops.run_queue
UNION ALL
SELECT 'batch_process' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ops.batch_process
UNION ALL
SELECT 'batch_process_table_details' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ops.batch_process_table_details
UNION ALL
SELECT 'batch_process_steps' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ops.batch_process_steps
UNION ALL
SELECT 'dataset_control' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
UNION ALL
SELECT 'schema_versions' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ctrl.schema_versions
UNION ALL
SELECT 'dataset_audit' as tabela, COUNT(*) as registros FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_audit;

-- =====================================================================
-- RESULTADO ESPERADO: Todas as tabelas devem mostrar 0 registros
-- =====================================================================
