-- ============================================================================
-- Migration 004: Incremental Loading Strategy - Universal Oracle → Delta Engine
-- ============================================================================
-- Adiciona suporte completo para carga incremental inteligente com:
-- - Discovery automático de estratégia (watermark, hash+merge, snapshot, CDC)
-- - Controle de performance (OPTIMIZE condicional)
-- - Reconciliação de deletes (opt-in)
-- - Override manual de watermark (reprocessamento histórico)
-- - Safety checks e monitoramento
-- ============================================================================

-- Adicionar colunas de estratégia incremental
ALTER TABLE cm_dbx_dev.ingestion_sys_ctrl.dataset_control ADD COLUMNS (
  -- ===========================
  -- Estratégia de Carga
  -- ===========================
  incremental_strategy STRING COMMENT 'Estratégia ativa: WATERMARK | HASH_MERGE | SNAPSHOT | APPEND_LOG | REQUIRES_CDC',
  incremental_metadata STRING COMMENT 'JSON: {\"watermark_col\": \"LAST_UPDATE_DATE\", \"pk\": [\"ID\"], \"hash_exclude_cols\": [\"UPDATED_AT\", \"LAST_ACCESS\"]}',
  strategy_locked BOOLEAN COMMENT 'TRUE = estratégia confirmada pelo usuário, discovery não sobrescreve',
  enable_incremental BOOLEAN COMMENT 'FALSE por padrão (opt-in gradual). Quando TRUE, usa estratégia incremental ao invés de SNAPSHOT',
  
  -- ===========================
  -- Modo de Escrita na Bronze
  -- ===========================
  bronze_mode STRING COMMENT 'SNAPSHOT = OVERWRITE completo | CURRENT = MERGE incremental | APPEND_LOG = APPEND puro',
  
  -- ===========================
  -- Discovery Tracking
  -- ===========================
  last_discovery_at TIMESTAMP COMMENT 'Timestamp da última execução de discovery',
  discovery_status STRING COMMENT 'PENDING = aguardando discovery | PENDING_CONFIRMATION = aguarda aprovação usuário | SUCCESS = confirmado | FAILED = erro',
  discovery_suggestion STRING COMMENT 'Estratégia sugerida pelo discovery (WATERMARK/HASH_MERGE/SNAPSHOT/APPEND_LOG/REQUIRES_CDC)',
  
  -- ===========================
  -- Reconciliação de Deletes
  -- ===========================
  enable_reconciliation BOOLEAN COMMENT 'TRUE apenas para dimensões pequenas (< 1M rows). Compara PKs Oracle vs Bronze para marcar deletados',
  last_reconciliation_at TIMESTAMP COMMENT 'Timestamp da última reconciliação de deletes executada',
  
  -- ===========================
  -- Safety Monitoring
  -- ===========================
  watermark_stale_threshold_hours INT COMMENT 'Alerta se watermark não avançar neste período (padrão: 48h). NULL = desabilitado',
  
  -- ===========================
  -- Performance e Otimização
  -- ===========================
  last_optimize_at TIMESTAMP COMMENT 'Timestamp do último OPTIMIZE ZORDER executado',
  optimize_frequency_hours INT COMMENT 'Frequência desejada de OPTIMIZE (padrão: 24h). Usado em conjunto com threshold de merges',
  optimize_threshold_merges INT COMMENT 'Rodar OPTIMIZE após X merges incrementais (padrão: 100). Evita otimização excessiva',
  merge_count_since_optimize INT COMMENT 'Contador de merges desde último OPTIMIZE. Resetado para 0 após cada OPTIMIZE',
  small_files_count INT COMMENT 'Número de arquivos pequenos detectados na última verificação de fragmentação',
  
  -- ===========================
  -- Override Manual (Reprocessamento)
  -- ===========================
  override_watermark_value STRING COMMENT 'Valor manual de watermark para reprocessamento histórico (ex: "2026-01-01 00:00:00"). NULL = usar watermark normal da dataset_watermark'
);

-- ============================================================================
-- Valores padrão para datasets existentes: modo seguro (SNAPSHOT)
-- ============================================================================
-- IMPORTANTE: Datasets existentes mantêm comportamento atual (SNAPSHOT).
-- Incremental é opt-in explícito (enable_incremental = FALSE por padrão).
-- ============================================================================

UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  -- Manter comportamento atual (SNAPSHOT)
  incremental_strategy = 'SNAPSHOT',
  enable_incremental = FALSE,
  bronze_mode = 'SNAPSHOT',
  
  -- Discovery pendente (será executado na próxima oportunidade)
  strategy_locked = FALSE,
  discovery_status = 'PENDING',
  discovery_suggestion = NULL,
  last_discovery_at = NULL,
  
  -- Reconciliação desabilitada por padrão (opt-in)
  enable_reconciliation = FALSE,
  last_reconciliation_at = NULL,
  
  -- Safety checks: watermark estagnado após 48h
  watermark_stale_threshold_hours = 48,
  
  -- Otimização: ZORDER após 100 merges OU a cada 24h
  optimize_frequency_hours = 24,
  optimize_threshold_merges = 100,
  merge_count_since_optimize = 0,
  last_optimize_at = NULL,
  small_files_count = NULL,
  
  -- Sem override de watermark (usar watermark normal)
  override_watermark_value = NULL

WHERE incremental_strategy IS NULL;

-- ============================================================================
-- Validação: Verificar colunas criadas
-- ============================================================================
SELECT 
  'Migration 004 completed successfully' as status,
  COUNT(*) as datasets_migrated,
  SUM(CASE WHEN incremental_strategy = 'SNAPSHOT' THEN 1 ELSE 0 END) as snapshot_mode,
  SUM(CASE WHEN enable_incremental = FALSE THEN 1 ELSE 0 END) as incremental_disabled,
  SUM(CASE WHEN discovery_status = 'PENDING' THEN 1 ELSE 0 END) as pending_discovery
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control;

-- ============================================================================
-- NOTAS IMPORTANTES
-- ============================================================================
-- 1. Todos datasets existentes iniciam com SNAPSHOT (comportamento atual mantido)
-- 2. Discovery será executado automaticamente, mas NÃO ativa incremental
-- 3. Usuário deve revisar discovery_suggestion e confirmar via UI/API
-- 4. Após confirmação: enable_incremental = TRUE + strategy_locked = TRUE
-- 5. Reconciliação de deletes é opt-in apenas para dimensões pequenas
-- 6. OPTIMIZE é condicional: após 100 merges OU 24h (o que vier primeiro)
-- 7. override_watermark_value permite reprocessamento histórico manual
-- ============================================================================
