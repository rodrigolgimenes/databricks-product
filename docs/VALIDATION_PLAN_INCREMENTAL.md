# Plano de Validação End-to-End - Carga Incremental

## Objetivo
Validar a implementação completa do **Motor Universal de Carga Incremental** Oracle → Delta, garantindo que todas as 5 estratégias funcionem corretamente e as 8 colunas técnicas sejam adicionadas.

---

## Pré-requisitos

### 1. Executar Migration 004
```sql
-- Via Databricks SQL Warehouse (MCP)
-- Executar: database/migrations/004_incremental_strategy_columns.sql
```

**Validação**:
```sql
DESCRIBE cm_dbx_dev.ingestion_sys_ctrl.dataset_control;
-- Verificar presença das 19 novas colunas:
-- incremental_strategy, incremental_metadata, strategy_locked, enable_incremental, etc.
```

### 2. Upload do Notebook
- Upload `databricks_notebooks/incremental_loading_functions.py` para workspace Databricks
- Path: `/Workspace/Shared/incremental_loading_functions`

### 3. Dataset de Teste Oracle
Escolher tabela Oracle com as seguintes características:
- **Tamanho**: ~100K - 500K rows (suficiente para validar performance, não demora muito)
- **Primary Key**: Sim (preferencial para testar MERGE)
- **Coluna de Auditoria**: `LAST_UPDATE_DATE` ou `UPDATED_AT` (para WATERMARK)
- **Exemplo**: Tabela de dimensão como `CLIENTE`, `PRODUTO`, `FORNECEDOR`

**Recomendação**: Se possível, escolher uma tabela que já esteja cadastrada em `dataset_control` e não esteja em produção (ambiente de homologação).

---

## Fases de Validação

### FASE 1: Discovery Automático ✅

#### 1.1 Preparar Dataset
```sql
-- Cadastrar novo dataset (ou usar existente)
INSERT INTO cm_dbx_dev.ingestion_sys_ctrl.dataset_control (
  dataset_id, dataset_name, connection_id, bronze_table_name,
  enable_incremental, discovery_status
) VALUES (
  'TEST_INCR_001',
  'OWNER.CLIENTE@DBLINK',
  'oracle_prod_connection_id',
  'cm_dbx_dev.bronze.cliente',
  FALSE,  -- Desabilitado inicialmente
  'PENDING'
);
```

#### 1.2 Executar Processo (irá rodar discovery)
```python
# Via orquestrador governado (que chamará _load_oracle_bronze_incremental)
# O discovery rodará automaticamente quando discovery_status = 'PENDING'
```

#### 1.3 Validar Resultado do Discovery
```sql
SELECT 
  dataset_id,
  discovery_status,        -- Deve ser 'PENDING_CONFIRMATION'
  discovery_suggestion,    -- Ex: 'WATERMARK' ou 'HASH_MERGE'
  incremental_metadata,    -- JSON com watermark_col, pk, hash_exclude_cols
  last_discovery_at
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = 'TEST_INCR_001';
```

**Critérios de Sucesso**:
- ✅ `discovery_status` = `'PENDING_CONFIRMATION'`
- ✅ `discovery_suggestion` contém estratégia válida (`WATERMARK`, `HASH_MERGE`, `SNAPSHOT`, `APPEND_LOG`, `REQUIRES_CDC`)
- ✅ `incremental_metadata` é JSON válido com campos esperados
- ✅ Logs mostram detecção de PK e colunas de auditoria

**Evidências a capturar**:
- Screenshot do registro em `dataset_control`
- Print dos logs do notebook mostrando discovery steps

---

### FASE 2: Primeira Execução Incremental (WATERMARK) ✅

#### 2.1 Ativar Incremental (simulando confirmação do usuário)
```sql
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  enable_incremental = TRUE,
  incremental_strategy = discovery_suggestion,  -- Copiar sugestão
  strategy_locked = TRUE,
  bronze_mode = 'CURRENT',
  discovery_status = 'SUCCESS'
WHERE dataset_id = 'TEST_INCR_001';
```

#### 2.2 Executar Primeira Carga
```python
# Rodar orquestrador (irá chamar _load_oracle_bronze_incremental)
# Primeira execução: sem watermark anterior, irá ler tudo e criar tabela Bronze
```

#### 2.3 Validar Tabela Bronze
```sql
SELECT COUNT(*) FROM cm_dbx_dev.bronze.cliente;
-- Deve retornar ~100K-500K rows

SELECT * FROM cm_dbx_dev.bronze.cliente LIMIT 10;
-- Verificar presença das 8 colunas técnicas

DESCRIBE cm_dbx_dev.bronze.cliente;
-- Verificar schema:
-- Colunas de negócio (originais do Oracle)
-- + _ingestion_ts, _batch_id, _source_table, _op, _watermark_col, _watermark_value, _row_hash, _is_deleted
```

**Critérios de Sucesso**:
- ✅ Tabela Bronze criada com sucesso
- ✅ Count de registros corresponde ao esperado
- ✅ **8 colunas técnicas** presentes:
  - `_ingestion_ts`: TIMESTAMP (não NULL)
  - `_batch_id`: STRING (UUID)
  - `_source_table`: STRING (ex: "OWNER.CLIENTE@DBLINK")
  - `_op`: STRING ("UPSERT")
  - `_watermark_col`: STRING (ex: "LAST_UPDATE_DATE")
  - `_watermark_value`: TIMESTAMP ou NUMERIC (não NULL se watermark_col existe)
  - `_row_hash`: STRING (MD5, 32 chars)
  - `_is_deleted`: BOOLEAN (FALSE)
- ✅ `_row_hash` varia entre registros (não todos iguais)
- ✅ Watermark registrado em `dataset_watermark`

```sql
SELECT * FROM cm_dbx_dev.ingestion_sys_ops.dataset_watermark 
WHERE dataset_id = 'TEST_INCR_001';
-- Verificar watermark_value = MAX(LAST_UPDATE_DATE) da tabela Oracle
```

**Evidências a capturar**:
- Screenshot da query `SELECT * LIMIT 10` mostrando as 8 colunas técnicas
- Screenshot do `dataset_watermark` com valor inicial
- Print dos logs mostrando MERGE executado

---

### FASE 3: Segunda Execução Incremental (Delta Only) ✅

#### 3.1 Simular Mudanças no Oracle
**IMPORTANTE**: Esta é a parte crítica que prova o incremental funciona.

**Opção A - Ambiente de Teste** (ideal):
```sql
-- No Oracle, atualizar alguns registros e inserir novos
UPDATE OWNER.CLIENTE SET NOME = 'TESTE INCREMENTAL', LAST_UPDATE_DATE = SYSDATE WHERE ROWNUM <= 5;
INSERT INTO OWNER.CLIENTE (ID, NOME, LAST_UPDATE_DATE) VALUES (999999, 'NOVO CLIENTE', SYSDATE);
COMMIT;
```

**Opção B - Produção** (mais seguro, não mexe no Oracle):
```sql
-- Forçar watermark para uma data antiga para "fingir" que precisa buscar novos dados
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET override_watermark_value = '2024-01-01 00:00:00'  -- Data antiga
WHERE dataset_id = 'TEST_INCR_001';
```

#### 3.2 Executar Segunda Carga
```python
# Rodar orquestrador novamente
# Deve ler APENAS registros com LAST_UPDATE_DATE >= último watermark
```

#### 3.3 Validar Leitura Incremental
```sql
-- Verificar que Bronze recebeu APENAS novos/alterados registros
SELECT COUNT(*) FROM cm_dbx_dev.bronze.cliente WHERE _batch_id = '<batch_id_segunda_execucao>';
-- Deve retornar ~5-10 registros (se Opção A) ou mais (se Opção B)

-- Verificar watermark avançou
SELECT watermark_value FROM cm_dbx_dev.ingestion_sys_ops.dataset_watermark 
WHERE dataset_id = 'TEST_INCR_001';
-- Deve ser > watermark anterior
```

**Critérios de Sucesso**:
- ✅ Logs mostram leitura incremental: `WHERE LAST_UPDATE_DATE >= '<watermark>'`
- ✅ **Número de registros lidos < total da tabela** (prova que não leu tudo)
- ✅ Registros atualizados têm `_op` = 'UPDATE' (ou 'UPSERT')
- ✅ Watermark avançou para novo valor
- ✅ **merge_count_since_optimize incrementado**

```sql
SELECT merge_count_since_optimize FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control 
WHERE dataset_id = 'TEST_INCR_001';
-- Deve ser 1 ou 2
```

**Evidências a capturar**:
- Screenshot dos logs mostrando `WHERE ... >= watermark` e contagem reduzida
- Screenshot do `dataset_watermark` com novo valor
- Screenshot do `merge_count_since_optimize` incrementado
- **Métrica de I/O**: Comparar tempo/volume de dados lido vs primeira execução (redução dramática)

---

### FASE 4: Validação de Hash e Dedupe ✅

#### 4.1 Validar Hash Normalizado
```sql
-- Verificar que hash é consistente (mesmos dados = mesmo hash)
SELECT 
  ID,
  NOME,
  _row_hash,
  COUNT(*) as ocorrencias
FROM cm_dbx_dev.bronze.cliente
GROUP BY ID, NOME, _row_hash
HAVING COUNT(*) > 1;
-- Deve retornar 0 registros (sem duplicatas por hash)

-- Validar que colunas voláteis não afetam hash
-- Se houver UPDATED_AT, mudar valor e verificar que hash não muda (isso é simulação conceitual)
```

#### 4.2 Validar Dedupe por Watermark >=
```sql
-- Simular duplicata na leitura (se possível no Oracle)
-- INSERT + UPDATE com mesmo PK e diferentes LAST_UPDATE_DATE
-- Bronze deve manter apenas registro com MAIOR watermark
```

**Critérios de Sucesso**:
- ✅ Hash não contém colunas voláteis (UPDATED_AT, LAST_ACCESS, etc)
- ✅ Decimals normalizados (formato string consistente)
- ✅ Timestamps truncados para segundos
- ✅ Dedupe mantém registro correto (maior watermark)

---

### FASE 5: Validação OPTIMIZE Condicional ✅

#### 5.1 Forçar Threshold de OPTIMIZE
```sql
-- Configurar threshold baixo para testar
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  optimize_threshold_merges = 2,  -- Threshold baixo para teste
  merge_count_since_optimize = 2   -- Simular que já atingiu
WHERE dataset_id = 'TEST_INCR_001';
```

#### 5.2 Executar Carga (irá disparar OPTIMIZE)
```python
# Rodar orquestrador
# Deve executar OPTIMIZE ZORDER automaticamente
```

#### 5.3 Validar OPTIMIZE Executado
```sql
SELECT 
  merge_count_since_optimize,  -- Deve ter resetado para 0
  last_optimize_at             -- Deve ser recente
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = 'TEST_INCR_001';

-- Verificar files compactados na Bronze
DESCRIBE DETAIL cm_dbx_dev.bronze.cliente;
-- Verificar `numFiles` reduzido após OPTIMIZE
```

**Critérios de Sucesso**:
- ✅ Logs mostram `OPTIMIZE ZORDER BY (pk_cols)` executado
- ✅ `merge_count_since_optimize` resetado para 0
- ✅ `last_optimize_at` atualizado
- ✅ Número de files na tabela Delta reduzido

**Evidências a capturar**:
- Screenshot do `dataset_control` com contador resetado
- Screenshot dos logs mostrando OPTIMIZE
- `DESCRIBE DETAIL` antes/depois

---

### FASE 6: Validação Reconciliação de Deletes (Opt-in) ✅

#### 6.1 Habilitar Reconciliação
```sql
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET enable_reconciliation = TRUE
WHERE dataset_id = 'TEST_INCR_001';
```

#### 6.2 Simular Delete no Oracle
```sql
-- Deletar 1-2 registros no Oracle (ambiente de teste)
DELETE FROM OWNER.CLIENTE WHERE ID IN (12345, 12346);
COMMIT;
```

#### 6.3 Executar Reconciliação
```python
# Chamar diretamente ou via job agendado
_reconcile_deletes(
    dataset_id='TEST_INCR_001',
    oracle_table='OWNER.CLIENTE@DBLINK',
    bronze_table='cm_dbx_dev.bronze.cliente',
    pk_cols=['ID'],
    jdbc_url='...',
    user='...',
    pwd='...',
    catalog='cm_dbx_dev'
)
```

#### 6.4 Validar Soft Delete
```sql
SELECT 
  ID, 
  NOME, 
  _is_deleted, 
  _ingestion_ts
FROM cm_dbx_dev.bronze.cliente
WHERE _is_deleted = TRUE;
-- Deve retornar os 2 registros deletados

-- Verificar alerta se > 10% deletado
SELECT 
  deleted_count,
  total_count,
  delete_ratio,
  alert
FROM -- resultado da função
-- Se delete_ratio > 0.10, alert = TRUE
```

**Critérios de Sucesso**:
- ✅ Registros deletados no Oracle marcados com `_is_deleted = TRUE` na Bronze
- ✅ LEFT ANTI JOIN usado (nunca NOT IN)
- ✅ Alerta disparado se > 10% deletado
- ✅ `last_reconciliation_at` atualizado

**Evidências a capturar**:
- Screenshot da Bronze com `_is_deleted = TRUE`
- Print dos logs mostrando LEFT ANTI JOIN
- Screenshot de alerta (se aplicável)

---

## FASE 7: Validação Estratégia HASH_MERGE ✅

#### 7.1 Configurar Dataset para HASH_MERGE
```sql
-- Escolher tabela de médio porte (10M-100M) SEM watermark confiável
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  incremental_strategy = 'HASH_MERGE',
  bronze_mode = 'CURRENT',
  enable_incremental = TRUE,
  incremental_metadata = '{"pk": ["ID"], "hash_exclude_cols": ["UPDATED_AT"]}'
WHERE dataset_id = 'TEST_INCR_002';
```

#### 7.2 Executar Primeira Carga
```python
# Rodar orquestrador
# Lê tudo, calcula hash, insere na Bronze
```

#### 7.3 Executar Segunda Carga (sem mudanças no Oracle)
```python
# Rodar orquestrador novamente
# Lê tudo, mas NÃO atualiza registros com hash igual
```

#### 7.4 Validar Skip por Hash
```sql
-- Verificar logs: "MERGE concluído (só atualizou registros com hash diferente)"
-- Verificar que UPDATE só ocorreu para registros com hash mudado
```

**Critérios de Sucesso**:
- ✅ Primeira execução: INSERTs
- ✅ Segunda execução: Nenhum UPDATE (hash igual, skip)
- ✅ Terceira execução (após alterar 1 registro no Oracle): UPDATE apenas esse registro
- ✅ **Performance**: Tempo de MERGE significativamente menor quando nada mudou

---

## FASE 8: Validação Override Watermark (Reprocessamento) ✅

#### 8.1 Configurar Override
```sql
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET override_watermark_value = '2023-01-01 00:00:00'  -- Data antiga
WHERE dataset_id = 'TEST_INCR_001';
```

#### 8.2 Executar Carga
```python
# Rodar orquestrador
# Deve usar override_watermark_value ao invés do watermark normal
```

#### 8.3 Validar Reprocessamento
```sql
-- Verificar logs: "OVERRIDE MANUAL DETECTADO: 2023-01-01"
-- Verificar que leitura usou data antiga: WHERE ... >= '2023-01-01'
-- Verificar volume de registros lidos (muito maior, pois pegou histórico)

-- Limpar override após execução
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET override_watermark_value = NULL
WHERE dataset_id = 'TEST_INCR_001';
```

**Critérios de Sucesso**:
- ✅ Override detectado e usado
- ✅ Volume de dados lido maior (histórico)
- ✅ Watermark final avançou para MAX real (não ficou preso no override)

---

## Checklist Final de Validação

### ✅ Funcionalidades Core
- [ ] Discovery automático detecta estratégia correta (WATERMARK, HASH_MERGE, SNAPSHOT, APPEND_LOG, REQUIRES_CDC)
- [ ] Discovery detecta PK corretamente via `all_constraints`
- [ ] Discovery detecta colunas de auditoria via `all_tab_columns`
- [ ] Discovery valida incrementalidade usando `all_tab_col_statistics` (instantâneo)
- [ ] Discovery detecta colunas voláteis (UPDATED_AT, etc)
- [ ] Primera execução incremental lê tudo e cria Bronze com 8 colunas técnicas
- [ ] Segunda execução incremental lê APENAS delta (WHERE >= watermark)
- [ ] Watermark avança após cada execução
- [ ] Hash normalizado (decimals, timestamps truncados)
- [ ] Hash exclui colunas voláteis
- [ ] MERGE por PK com dedupe (manter maior watermark)
- [ ] MERGE por hash com skip se hash igual
- [ ] OPTIMIZE condicional (threshold de merges)
- [ ] OPTIMIZE ZORDER por PK
- [ ] Reconciliação de deletes com LEFT ANTI JOIN
- [ ] Override watermark para reprocessamento

### ✅ Schema e Dados
- [ ] 8 colunas técnicas presentes na Bronze: `_ingestion_ts, _batch_id, _source_table, _op, _watermark_col, _watermark_value, _row_hash, _is_deleted`
- [ ] `_row_hash` é MD5 válido (32 chars hexadecimal)
- [ ] `_watermark_value` está tipado (TIMESTAMP ou NUMERIC, não STRING)
- [ ] `_op` contém valores válidos (UPSERT, INSERT, UPDATE)
- [ ] Não há duplicatas por PK na Bronze após MERGE

### ✅ Performance e Alertas
- [ ] Redução de I/O na segunda execução (> 80% menos dados lidos)
- [ ] `merge_count_since_optimize` incrementado após cada MERGE
- [ ] OPTIMIZE executado quando threshold atingido
- [ ] Contador resetado após OPTIMIZE
- [ ] Alerta dispara se > 10% da tabela deletada na reconciliação

### ✅ Safety e Fallback
- [ ] `enable_incremental = FALSE` → usa fallback `_load_oracle_bronze` (full refresh)
- [ ] Discovery não ativa automaticamente (status = PENDING_CONFIRMATION)
- [ ] Watermark >= (não >) para evitar perda de dados
- [ ] LEFT ANTI JOIN (nunca NOT IN) para deletes
- [ ] OPTIMIZE ZORDER não executa em todo MERGE (apenas quando threshold)

---

## Evidências de Sucesso (Documentação)

### 1. Screenshots
- [ ] `dataset_control` com discovery_suggestion preenchido
- [ ] Bronze table com `SELECT * LIMIT 10` mostrando 8 colunas técnicas
- [ ] `dataset_watermark` com watermark inicial e avançado
- [ ] Logs mostrando leitura incremental (`WHERE ... >= watermark`)
- [ ] `merge_count_since_optimize` antes/depois de OPTIMIZE
- [ ] Bronze com `_is_deleted = TRUE` após reconciliação

### 2. Métricas de Performance
- [ ] **Primeira execução**: X rows lidos, Y segundos
- [ ] **Segunda execução**: Z rows lidos (Z << X), W segundos (W << Y)
- [ ] **Redução de I/O**: ((X-Z)/X * 100)% (deve ser > 90% em incremental WATERMARK)

### 3. Logs Críticos
```
[DISCOVERY] ✓ Coluna watermark candidata: LAST_UPDATE_DATE (DATE)
[DISCOVERY] ✓ Primary Key encontrada: ['ID']
[DISCOVERY] ✅ Estratégia sugerida: WATERMARK (watermark=LAST_UPDATE_DATE)

[INCREMENTAL] Leitura incremental: WHERE LAST_UPDATE_DATE >= '2024-01-15 10:30:00'
[INCREMENTAL] Registros lidos do Oracle: 127 (vs 100,543 na primeira execução)

[MERGE_PK] Aplicando dedupe por PK + maior watermark
[MERGE_PK] ✓ MERGE concluído: 127 registros processados

[OPTIMIZE] Executando OPTIMIZE ZORDER em bronze.cliente...
[OPTIMIZE] ✓ OPTIMIZE ZORDER BY (ID) concluído
[OPTIMIZE] ✓ Contador resetado para 0

[RECONCILE] Registros a marcar como deletados: 2 de 100,543 (0.00%)
[RECONCILE] ✓ 2 registros marcados como _is_deleted = true
```

---

## Rollback e Cleanup

### Se Validação Falhar
```sql
-- Reverter dataset para full refresh
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET enable_incremental = FALSE, discovery_status = 'PENDING'
WHERE dataset_id = 'TEST_INCR_001';

-- Dropar tabela Bronze de teste
DROP TABLE IF EXISTS cm_dbx_dev.bronze.cliente;

-- Limpar watermark
DELETE FROM cm_dbx_dev.ingestion_sys_ops.dataset_watermark WHERE dataset_id = 'TEST_INCR_001';
```

### Após Validação Completa
```sql
-- Manter tabelas de teste ou limpar
-- Documentar resultados em IMPLEMENTATION_STATUS.md
```

---

## Próximos Passos Após Validação

1. **Backend**: Implementar endpoints `/datasets/:id/confirm-strategy` e `/rediscover`
2. **Frontend**: Modal de confirmação de discovery
3. **Jobs Agendados**: OPTIMIZE automático e reconciliação periódica
4. **Dashboard**: Observabilidade de estratégias e economia de I/O
5. **Rollout Gradual**: Ativar incremental em produção dataset por dataset

---

## Estimativa de Tempo de Validação

- **FASE 1-2**: 30 min (discovery + primeira execução)
- **FASE 3**: 20 min (segunda execução incremental)
- **FASE 4**: 15 min (hash e dedupe)
- **FASE 5**: 15 min (OPTIMIZE)
- **FASE 6**: 20 min (reconciliação)
- **FASE 7**: 15 min (HASH_MERGE)
- **FASE 8**: 10 min (override)
- **Documentação**: 15 min

**TOTAL**: ~2h30min de validação ativa + tempo de execução dos jobs

---

## Contato em Caso de Problemas

- Logs de erro: Verificar output do notebook Databricks
- Query debug: `SELECT * FROM dataset_control WHERE dataset_id = 'TEST_INCR_001'`
- Suporte: Incluir logs completos e screenshot do erro
