# 🚀 Status de Implementação - Carga Incremental Universal

> **Data:** 2026-02-21  
> **Plano:** `plan_id: 026fda13-4ed1-44b6-9572-c51ea0e0745d`  
> **Decisões Críticas:** `docs/INCREMENTAL_LOADING_CRITICAL_DECISIONS.md`

---

## ✅ Fase 1 - Core Engine (PARCIAL - 40%)

### ✅ COMPLETO (4/20 tarefas)

1. ✅ **Migration 004** - `database/migrations/004_incremental_strategy_columns.sql`
   - 19 novas colunas em `dataset_control`
   - Valores padrão seguros (SNAPSHOT, enable_incremental=FALSE)
   - Validação incluída

2. ✅ **Funções de Discovery** - `databricks_notebooks/incremental_loading_functions.py`
   - `_detect_volatile_columns()` - Auto-detecta colunas voláteis (UPDATED_AT, etc)
   - `_discover_incremental_strategy()` - Discovery completo com 4 passos
     - Stats Oracle (all_tab_col_statistics) + amostra 1% fallback
     - Decisão: WATERMARK / HASH_MERGE / SNAPSHOT / APPEND_LOG / REQUIRES_CDC
   - `_get_last_watermark()` - Suporte a override_watermark_value
   - `_add_bronze_metadata_columns()` - 8 colunas técnicas com hash normalizado

### 🔄 PENDENTE (16/20 tarefas)

**CRÍTICAS (devem ser implementadas primeiro):**

3. ⏳ **Funções MERGE** - `databricks_notebooks/incremental_loading_functions.py`
   ```python
   def _merge_bronze_by_pk(df, bronze_table, pk_cols):
       # DeltaTable.merge() por PK
       # WHEN MATCHED: UPDATE all + _op='UPDATE'
       # WHEN NOT MATCHED: INSERT all + _op='INSERT'
       # IMPORTANTE: Dedupe por PK + watermark >= antes do merge
   
   def _merge_bronze_by_hash(df, bronze_table, pk_cols):
       # DeltaTable.merge() por PK
       # WHEN MATCHED AND target._row_hash <> source._row_hash: UPDATE
       # WHEN MATCHED AND hash igual: skip (sem UPDATE)
       # WHEN NOT MATCHED: INSERT
   ```

4. ⏳ **Função OPTIMIZE Condicional**
   ```python
   def _optimize_bronze_table_conditional(bronze_table, pk_cols, dataset_id, catalog):
       # Verificar merge_count_since_optimize > optimize_threshold_merges
       # Se TRUE: OPTIMIZE {bronze_table} ZORDER BY (pk_cols)
       # UPDATE dataset_control: merge_count_since_optimize=0, last_optimize_at=now()
   ```

5. ⏳ **Função Reconciliação Deletes**
   ```python
   def _reconcile_deletes(dataset_id, oracle_table, bronze_table, pk_cols, jdbc_url, user, pwd):
       # PRÉ-REQUISITO: enable_reconciliation = TRUE
       # 1. SELECT pk FROM oracle_table
       # 2. LEFT ANTI JOIN com bronze (NUNCA NOT IN)
       # 3. UPDATE bronze SET _is_deleted = true WHERE pk IN (ausentes)
       # 4. ALERTA se > 10% da tabela marcada como deletada
   ```

6. ⏳ **Orquestrador: _load_oracle_bronze_incremental()**
   ```python
   def _load_oracle_bronze_incremental(dataset_id, dataset_name, connection_id, bronze_table):
       # 1. Carregar estratégia de dataset_control
       # 2. Se discovery_status = PENDING: executar discovery, salvar sugestão
       # 3. Se enable_incremental = FALSE: fallback para _load_oracle_bronze()
       # 4. Se enable_incremental = TRUE:
       #    a. Buscar último watermark (_get_last_watermark)
       #    b. Ler Oracle com filtro (WHERE watermark >= last_value)
       #    c. Adicionar colunas técnicas (_add_bronze_metadata_columns)
       #    d. Aplicar merge/append conforme bronze_mode
       #    e. Incrementar merge_count_since_optimize
       #    f. Verificar se precisa OPTIMIZE
   ```

7. ⏳ **Integração no Orquestrador Principal**
   - Modificar `governed_ingestion_orchestrator.py` linha ~1138
   - Substituir `_load_oracle_bronze()` por lógica condicional:
   ```python
   # Verificar enable_incremental
   enable_inc = dataset_control_row.enable_incremental
   
   if enable_inc:
       b = _load_oracle_bronze_incremental(...)  # Novo
   else:
       b = _load_oracle_bronze(...)  # Manter existente (fallback)
   ```

**FASE 2 - UI e API (0/5 tarefas):**

8. ⏳ Backend: Endpoint `POST /api/portal/datasets/:id/confirm-strategy`
9. ⏳ Backend: Endpoint `POST /api/portal/datasets/:id/rediscover`
10. ⏳ Frontend: Modal de confirmação discovery
11. ⏳ Frontend: Badges visuais estratégia
12. ⏳ Frontend: Campo override_watermark_value

**FASE 3 - Jobs Agendados (0/2 tarefas):**

13. ⏳ Job: OPTIMIZE automático (verifica merge_count)
14. ⏳ Job: Reconciliação deletes (enable_reconciliation=TRUE)

**FASE 4 - Observabilidade (0/2 tarefas):**

15. ⏳ Dashboard: Estratégias por tipo
16. ⏳ Alertas: 6 checks de segurança

**TESTES (0/1 tarefa):**

17. ⏳ Testes unitários: discovery, hash, watermark >=

---

## 📋 Próximos Passos Imediatos

### PASSO 1: Completar Funções Core (3-4 dias)

1. Implementar `_merge_bronze_by_pk()` e `_merge_bronze_by_hash()` no arquivo `incremental_loading_functions.py`
2. Implementar `_optimize_bronze_table_conditional()`
3. Implementar `_reconcile_deletes()` com LEFT ANTI JOIN
4. Implementar `_load_oracle_bronze_incremental()` (função mãe que orquestra tudo)

### PASSO 2: Integrar no Orquestrador (1 dia)

5. Modificar `governed_ingestion_orchestrator.py`:
   - Importar funções de `incremental_loading_functions`
   - Linha ~1138: Adicionar lógica condicional `enable_incremental`
   - Manter fallback seguro para `_load_oracle_bronze()`

### PASSO 3: Executar Migration 004 (30min)

6. Conectar no Databricks SQL Warehouse
7. Executar `database/migrations/004_incremental_strategy_columns.sql`
8. Validar: `SELECT * FROM dataset_control LIMIT 1` (verificar novas colunas)

### PASSO 4: Teste Manual (1-2 dias)

9. Escolher dataset pequeno (~100K rows) com timestamp
10. Executar discovery (deve sugerir WATERMARK)
11. Confirmar estratégia manualmente via SQL:
    ```sql
    UPDATE dataset_control 
    SET 
      incremental_strategy = 'WATERMARK',
      enable_incremental = TRUE,
      strategy_locked = TRUE,
      incremental_metadata = '{"watermark_col": "LAST_UPDATE_DATE", "pk": ["ID"]}'
    WHERE dataset_id = '<test_dataset_id>';
    ```
12. Executar job do orquestrador
13. Validar:
    - Bronze tem 8 colunas técnicas
    - `_row_hash` está preenchido
    - `_watermark_value` está tipado (TIMESTAMP)
    - Watermark foi atualizado em `dataset_watermark`
    - Segundo execução lê apenas incremental (WHERE >= last_value)

---

## 🔥 Riscos e Bloqueadores

### RISCO 1: Funções MERGE Complexas

**Problema:**
- MERGE por PK com dedupe de watermark >= é complexo
- Precisa garantir que registros com mesmo timestamp não sejam perdidos

**Mitigação:**
- Implementar dedupe explícito ANTES do merge:
  ```python
  # Dedupe por PK, manter maior watermark
  w = Window.partitionBy(*pk_cols).orderBy(F.col(watermark_col).desc_nulls_last())
  df_deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
  
  # Depois merge normal
  dt.merge(df_deduped, ...)
  ```

### RISCO 2: Migration 004 Pode Falhar

**Problema:**
- Se algum dataset já tiver coluna com nome conflitante, ALTER TABLE falha

**Mitigação:**
- Testar migration em ambiente DEV primeiro
- Adicionar `IF NOT EXISTS` se suportado, ou try-catch no script

### RISCO 3: Performance do Discovery

**Problema:**
- Discovery executa 3-4 queries no Oracle por dataset
- Se executar para 100 datasets em paralelo, pode sobrecarregar Oracle

**Mitigação:**
- Discovery é executado apenas 1x (ou quando forçado)
- Resultado é cacheado em `discovery_suggestion`
- Adicionar rate limiting se necessário

---

## 📊 Métricas de Progresso

| Fase | Tarefas Completas | Tarefas Totais | % Completo |
|------|-------------------|----------------|------------|
| Fase 1 - Core Engine | 4 | 9 | **44%** |
| Fase 2 - UI e API | 0 | 5 | **0%** |
| Fase 3 - Jobs Agendados | 0 | 2 | **0%** |
| Fase 4 - Observabilidade | 0 | 2 | **0%** |
| Testes | 0 | 2 | **0%** |
| **TOTAL** | **4** | **20** | **20%** |

---

## ✅ Checklist de Validação (antes de produção)

### Arquitetura
- [x] Migration 004 criada com 19 colunas
- [x] Discovery usa stats Oracle (não COUNT DISTINCT)
- [x] Estratégia REQUIRES_CDC para > 100M rows
- [ ] Reconciliação usa LEFT ANTI JOIN (não NOT IN)
- [ ] OPTIMIZE é condicional (merge_count_since_optimize)
- [x] _watermark_value é tipado (TIMESTAMP/NUMERIC)
- [x] override_watermark_value implementado
- [x] _valid_from/_valid_to removido da Bronze CURRENT

### Funcionalidades
- [ ] MERGE por PK com dedupe watermark >=
- [ ] MERGE por hash (skip se igual)
- [ ] Discovery sugere (não ativa automaticamente)
- [ ] enable_incremental = FALSE por padrão
- [ ] Fallback seguro para _load_oracle_bronze()
- [ ] Contador merge_count_since_optimize incrementa

### Testes
- [ ] Teste: WATERMARK strategy (lê apenas incremental)
- [ ] Teste: HASH_MERGE strategy (detecta mudanças)
- [ ] Teste: Override watermark (reprocessamento)
- [ ] Teste: OPTIMIZE condicional (após 100 merges)
- [ ] Teste: Reconciliação deletes (< 10% tabela)

---

## 📝 Notas de Implementação

### Arquivo 1: `incremental_loading_functions.py` (PARCIAL)

**COMPLETO:**
- `_detect_volatile_columns()` - 97 linhas
- `_discover_incremental_strategy()` - 299 linhas
- `_get_last_watermark()` - 48 linhas
- `_add_bronze_metadata_columns()` - 94 linhas

**FALTA:**
- `_merge_bronze_by_pk()` - ~40 linhas
- `_merge_bronze_by_hash()` - ~60 linhas
- `_optimize_bronze_table_conditional()` - ~30 linhas
- `_reconcile_deletes()` - ~80 linhas
- `_load_oracle_bronze_incremental()` - ~150 linhas

**Total estimado:** ~1000 linhas quando completo

### Arquivo 2: `governed_ingestion_orchestrator.py` (NÃO MODIFICADO)

**MUDANÇAS NECESSÁRIAS:**
- Import de `incremental_loading_functions`
- Linha ~1138: Adicionar lógica condicional `enable_incremental`
- Verificar `discovery_status` e executar discovery se PENDING

---

> **PRÓXIMO PASSO:** Implementar as 5 funções faltantes em `incremental_loading_functions.py`
> 
> **ETA:** 3-4 dias para completar Fase 1 (Core Engine)
