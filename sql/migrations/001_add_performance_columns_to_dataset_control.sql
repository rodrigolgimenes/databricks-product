-- Migration: Adicionar colunas de configuração de performance Oracle
-- Tabela: ingestion_sys.ctrl.dataset_control
-- Data: 2026-02-20
-- Objetivo: Permitir configuração de fetchsize, particionamento JDBC e reparticionamento
--           diretamente pelo frontend ao criar datasets Oracle

-- ============================================================================
-- COLUNAS DE PERFORMANCE ORACLE
-- ============================================================================

-- Fetchsize JDBC (número de linhas por batch)
ALTER TABLE ingestion_sys.ctrl.dataset_control 
ADD COLUMN IF NOT EXISTS oracle_fetchsize INT 
COMMENT 'JDBC fetchsize para leitura Oracle (padrão: 10000). Afeta performance de rede.';

-- Número de partições para escrita Delta
ALTER TABLE ingestion_sys.ctrl.dataset_control 
ADD COLUMN IF NOT EXISTS spark_num_partitions INT 
COMMENT 'Número de partições Spark para escrita Delta (padrão dinâmico). Ex: 200, 800, 1600';

-- Particionamento JDBC paralelo (opcional)
ALTER TABLE ingestion_sys.ctrl.dataset_control 
ADD COLUMN IF NOT EXISTS jdbc_partition_column STRING 
COMMENT 'Coluna numérica para particionamento JDBC paralelo (NULL = desabilitado). Ex: ID, CODIGO';

ALTER TABLE ingestion_sys.ctrl.dataset_control 
ADD COLUMN IF NOT EXISTS jdbc_lower_bound BIGINT 
COMMENT 'Valor mínimo da coluna de particionamento JDBC (usado com partition_column)';

ALTER TABLE ingestion_sys.ctrl.dataset_control 
ADD COLUMN IF NOT EXISTS jdbc_upper_bound BIGINT 
COMMENT 'Valor máximo da coluna de particionamento JDBC (usado com partition_column)';

ALTER TABLE ingestion_sys.ctrl.dataset_control 
ADD COLUMN IF NOT EXISTS jdbc_num_partitions INT 
COMMENT 'Número de partições JDBC paralelas (usado com partition_column). Ex: 4, 8';

-- ============================================================================
-- VALORES PADRÃO RECOMENDADOS
-- ============================================================================

-- Atualizar datasets Oracle existentes com valores padrão
UPDATE ingestion_sys.ctrl.dataset_control
SET 
    oracle_fetchsize = 10000,
    spark_num_partitions = 800,
    jdbc_partition_column = NULL,
    jdbc_lower_bound = NULL,
    jdbc_upper_bound = NULL,
    jdbc_num_partitions = NULL
WHERE source_type = 'ORACLE'
  AND oracle_fetchsize IS NULL;

-- ============================================================================
-- EXEMPLO DE USO: Configurar dataset com particionamento JDBC
-- ============================================================================

-- Exemplo: Dataset com tabela grande (1M+ linhas) e coluna ID numérica
/*
UPDATE ingestion_sys.ctrl.dataset_control
SET 
    oracle_fetchsize = 10000,
    spark_num_partitions = 400,
    jdbc_partition_column = 'ID',
    jdbc_lower_bound = 1,
    jdbc_upper_bound = 2000000,
    jdbc_num_partitions = 8,
    updated_at = current_timestamp(),
    updated_by = 'admin'
WHERE dataset_id = '<seu-dataset-id>';
*/

-- Exemplo: Dataset com tabela média (100K-500K linhas) sem particionamento
/*
UPDATE ingestion_sys.ctrl.dataset_control
SET 
    oracle_fetchsize = 10000,
    spark_num_partitions = 200,
    jdbc_partition_column = NULL,
    jdbc_lower_bound = NULL,
    jdbc_upper_bound = NULL,
    jdbc_num_partitions = NULL,
    updated_at = current_timestamp(),
    updated_by = 'admin'
WHERE dataset_id = '<seu-dataset-id>';
*/

-- ============================================================================
-- VERIFICAÇÃO
-- ============================================================================

-- Verificar datasets Oracle com configurações de performance
SELECT 
    dataset_id,
    dataset_name,
    execution_state,
    oracle_fetchsize,
    spark_num_partitions,
    jdbc_partition_column,
    CASE 
        WHEN jdbc_partition_column IS NOT NULL THEN 'JDBC Partitioning ENABLED'
        ELSE 'Sequential Read'
    END as read_mode
FROM ingestion_sys.ctrl.dataset_control
WHERE source_type = 'ORACLE'
ORDER BY dataset_name;
