-- Configurar performance para o dataset CMALUINTERNO
-- Este dataset estava travando por 20+ minutos
-- Com estas otimizações, deve executar em 5-8 minutos

-- ============================================================================
-- APLICAR OTIMIZAÇÕES
-- ============================================================================

UPDATE ingestion_sys.ctrl.dataset_control
SET 
    -- Fetchsize otimizado para Oracle
    oracle_fetchsize = 10000,
    
    -- Partições dinâmicas para 120K linhas (otimizado)
    spark_num_partitions = 200,
    
    -- Sem particionamento JDBC por enquanto (testar primeiro sem)
    jdbc_partition_column = NULL,
    jdbc_lower_bound = NULL,
    jdbc_upper_bound = NULL,
    jdbc_num_partitions = NULL,
    
    -- Metadata
    updated_at = current_timestamp(),
    updated_by = 'admin_performance_tuning'
    
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';

-- ============================================================================
-- VERIFICAR CONFIGURAÇÃO APLICADA
-- ============================================================================

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
    END as read_mode,
    updated_at,
    updated_by
FROM ingestion_sys.ctrl.dataset_control
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';

-- ============================================================================
-- NOTAS
-- ============================================================================

/*
OTIMIZAÇÕES APLICADAS:

1. oracle_fetchsize = 10000
   - Reduz round-trips JDBC Oracle → Databricks
   - Melhoria esperada: 3-5x mais rápido

2. spark_num_partitions = 200
   - Otimizado para 120.200 linhas
   - Evita overhead de 800 partições (do código original)
   - Cada partição terá ~600 linhas

3. jdbc_partition_column = NULL
   - Desabilitado por enquanto para teste inicial
   - Pode ser habilitado depois se necessário

PRÓXIMOS PASSOS:

1. Executar o dataset novamente
2. Esperar 5-8 minutos (vs 20+ minutos antes)
3. Se ainda lento, habilitar JDBC partitioning:

   UPDATE ingestion_sys.ctrl.dataset_control
   SET 
       jdbc_partition_column = 'ID',  -- descobrir coluna numérica no DBeaver
       jdbc_lower_bound = 1,
       jdbc_upper_bound = 150000,
       jdbc_num_partitions = 8
   WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
*/
