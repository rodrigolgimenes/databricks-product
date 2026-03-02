# Guia de Teste - Carga Incremental
## Dataset: CMASTER.GLO_GRUPO_USUARIO@CMASTERPRD

---

## 🎯 Objetivo

Testar o fluxo completo de carga incremental:
1. Discovery automático (detecta PK, watermark, estratégia)
2. Primeira execução (cria Bronze com 8 colunas técnicas)
3. Segunda execução (leitura incremental - apenas delta)
4. Validar 8 colunas técnicas na Bronze

---

## ⚠️ PRÉ-REQUISITO CRÍTICO

**ANTES DE COMEÇAR**: Verificar se o Job do Databricks está usando o orquestrador correto:

1. Acesse Databricks UI
2. Vá em **Workflows** → **Jobs**
3. Procure o job de orquestração (ID: `690887429046802`)
4. Verifique se o **Notebook Path** é: `/Workspace/Shared/governed_ingestion_orchestrator`
5. Se estiver apontando para outro path, atualize para o correto

**Motivo**: O orquestrador antigo não tem a lógica incremental implementada.

---

## 🧪 TESTE 1: Ativação Manual (MAIS RÁPIDO - 5 min)

### Passo 1.1: Ativar Incremental via SQL

```sql
-- Via Databricks SQL ou MCP
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  enable_incremental = TRUE,
  incremental_strategy = 'SNAPSHOT',
  bronze_mode = 'SNAPSHOT',
  strategy_locked = TRUE,
  discovery_status = 'SUCCESS',
  discovery_suggestion = 'SNAPSHOT',
  incremental_metadata = '{"table_size_rows": 10000, "reason": "Manual test"}',
  updated_at = current_timestamp(),
  updated_by = 'test_user'
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

### Passo 1.2: Verificar Configuração

```sql
SELECT 
  dataset_id,
  dataset_name,
  enable_incremental,
  incremental_strategy,
  bronze_mode,
  discovery_status
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

**Resultado esperado**:
- `enable_incremental`: TRUE
- `incremental_strategy`: SNAPSHOT
- `discovery_status`: SUCCESS

### Passo 1.3: Executar Carga

**Via API (PowerShell)**:
```powershell
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/enqueue" -Method POST -ContentType "application/json"
```

**Ou via UI**: Acessar frontend e clicar em "Executar" no dataset

### Passo 1.4: Verificar Logs do Databricks

1. Acesse Databricks UI → **Workflows** → **Job Runs**
2. Procure pelo `run_id` retornado
3. **Logs a procurar**:
   - `[IMPORT] ✓ Incremental loading functions imported successfully`
   - `[RUN:BRONZE] 🔄 INCREMENTAL MODE ENABLED` ou `[RUN:BRONZE] 📦 FULL REFRESH MODE`
   - Se INCREMENTAL MODE: `[INCREMENTAL] enable_incremental = TRUE`

### Passo 1.5: Validar Resultado

```sql
-- Verificar se Bronze tem 8 colunas técnicas
DESCRIBE cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO;

-- Buscar colunas: _ingestion_ts, _batch_id, _source_table, _op, 
--                 _watermark_col, _watermark_value, _row_hash, _is_deleted
```

```sql
-- Ver dados com colunas técnicas
SELECT 
  *,
  _ingestion_ts,
  _batch_id,
  _source_table,
  _op,
  _row_hash,
  _is_deleted
FROM cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO
LIMIT 10;
```

**Resultado esperado**:
- ✅ Tabela Bronze existe
- ✅ 8 colunas técnicas presentes (começam com `_`)
- ✅ `_row_hash` varia entre registros (MD5, 32 chars)
- ✅ `_is_deleted` = FALSE para todos
- ✅ `_op` = 'UPSERT'

---

## 🧪 TESTE 2: Discovery Automático (IDEAL - 10 min)

### Passo 2.1: Resetar para PENDING

```sql
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  enable_incremental = FALSE,
  incremental_strategy = 'SNAPSHOT',
  discovery_status = 'PENDING',
  discovery_suggestion = NULL,
  incremental_metadata = NULL,
  strategy_locked = FALSE,
  last_discovery_at = NULL
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

### Passo 2.2: Executar Carga (vai rodar discovery)

```powershell
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/enqueue" -Method POST
```

### Passo 2.3: Aguardar 2-3 minutos

```powershell
Start-Sleep -Seconds 180
```

### Passo 2.4: Verificar Discovery

```sql
SELECT 
  dataset_id,
  discovery_status,        -- Deve ser 'PENDING_CONFIRMATION'
  discovery_suggestion,    -- Ex: 'WATERMARK', 'HASH_MERGE', 'SNAPSHOT'
  incremental_metadata,    -- JSON com watermark_col, pk, etc
  last_discovery_at
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

**Resultado esperado**:
- ✅ `discovery_status`: 'PENDING_CONFIRMATION'
- ✅ `discovery_suggestion`: 'WATERMARK' | 'HASH_MERGE' | 'SNAPSHOT' | 'APPEND_LOG'
- ✅ `incremental_metadata`: JSON válido com `pk`, `watermark_col`, `hash_exclude_cols`
- ✅ `last_discovery_at`: timestamp recente

### Passo 2.5: Confirmar Estratégia

**Via API**:
```powershell
$body = @{
    strategy = "WATERMARK"  # Ou usar discovery_suggestion
    bronze_mode = "CURRENT"
} | ConvertTo-Json

Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/confirm-strategy" -Method POST -ContentType "application/json" -Body $body
```

**Ou via SQL (manual)**:
```sql
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
  enable_incremental = TRUE,
  incremental_strategy = discovery_suggestion,  -- Copiar sugestão
  bronze_mode = 'CURRENT',
  strategy_locked = TRUE,
  discovery_status = 'SUCCESS'
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

### Passo 2.6: Executar Novamente (agora incremental ativo)

```powershell
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/enqueue" -Method POST
```

---

## 🧪 TESTE 3: Validação de Incremental (Delta Only)

### Passo 3.1: Primeira Execução

```powershell
# Executar dataset com incremental ativado
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/enqueue" -Method POST
```

**Aguardar 2 min e verificar**:
```sql
-- Count na Bronze após primeira execução
SELECT COUNT(*) as total_registros
FROM cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO;

-- Guardar o número para comparar depois
```

### Passo 3.2: Verificar Watermark

```sql
SELECT 
  dataset_id,
  watermark_value,
  watermark_type
FROM cm_dbx_dev.ingestion_sys_ops.dataset_watermark
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

**Resultado esperado**:
- ✅ Registro existe
- ✅ `watermark_value` contém data/hora ou número
- ✅ `watermark_type`: 'TIMESTAMP' ou 'NUMERIC'

### Passo 3.3: Segunda Execução (Incremental)

```powershell
# Executar novamente (deve ler apenas delta)
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/enqueue" -Method POST
```

**Aguardar 2 min e verificar logs**:
- Buscar: `[INCREMENTAL] Registros lidos do Oracle: X`
- **Se X for pequeno (< 10% do total)**: ✅ Incremental funcionando!
- **Se X = total da tabela**: ❌ Está lendo tudo (full refresh)

---

## 📊 CHECKLIST DE VALIDAÇÃO

### ✅ Configuração
- [ ] `enable_incremental` = TRUE
- [ ] `incremental_strategy` definida (WATERMARK/HASH_MERGE/SNAPSHOT)
- [ ] `bronze_mode` = 'CURRENT' (para MERGE) ou 'SNAPSHOT'
- [ ] `discovery_status` = 'SUCCESS'

### ✅ Bronze Table
- [ ] Tabela existe: `cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO`
- [ ] 8 colunas técnicas presentes:
  - [ ] `_ingestion_ts` (TIMESTAMP)
  - [ ] `_batch_id` (STRING UUID)
  - [ ] `_source_table` (STRING)
  - [ ] `_op` (STRING: UPSERT/INSERT/UPDATE)
  - [ ] `_watermark_col` (STRING)
  - [ ] `_watermark_value` (TIMESTAMP ou NUMERIC)
  - [ ] `_row_hash` (STRING MD5 32 chars)
  - [ ] `_is_deleted` (BOOLEAN)

### ✅ Watermark
- [ ] Registro existe em `dataset_watermark`
- [ ] `watermark_value` avança após cada execução
- [ ] Tipo correto (TIMESTAMP ou NUMERIC)

### ✅ Performance (Incremental)
- [ ] Segunda execução lê < 10% dos dados (se WATERMARK)
- [ ] Logs mostram `WHERE [coluna] >= [watermark]`
- [ ] Tempo de execução reduzido vs primeira execução

---

## ❌ TROUBLESHOOTING

### Problema 1: Discovery não roda (fica PENDING)
**Sintoma**: `discovery_status` permanece 'PENDING' após execução

**Causa**: Orquestrador não está importando funções incrementais

**Solução**:
1. Verificar se job Databricks usa `/Workspace/Shared/governed_ingestion_orchestrator`
2. Verificar logs do job: buscar `[IMPORT]`
3. Se não aparecer logs de import: job está usando arquivo errado

### Problema 2: Colunas técnicas não aparecem
**Sintoma**: Bronze não tem colunas `_ingestion_ts`, `_batch_id`, etc

**Causa**: Incremental não está ativo ou fallback para full refresh

**Solução**:
1. Verificar `enable_incremental = TRUE` no dataset_control
2. Verificar logs: deve aparecer `[RUN:BRONZE] 🔄 INCREMENTAL MODE ENABLED`
3. Se aparecer `📦 FULL REFRESH MODE`: incremental está desabilitado

### Problema 3: Lê todos os dados (não é incremental)
**Sintoma**: Segunda execução lê 100% da tabela

**Causa**: Estratégia SNAPSHOT ou primeira execução de WATERMARK

**Solução**:
- **Se SNAPSHOT**: Comportamento esperado (sempre lê tudo)
- **Se WATERMARK**: Normal na primeira vez, segunda execução deve ler apenas delta
- Verificar se `watermark_value` existe em `dataset_watermark`

---

## 🎯 RESULTADOS ESPERADOS

### Sucesso Completo ✅
- Discovery roda e sugere estratégia
- Bronze criada com 8 colunas técnicas
- Watermark registrado e avança
- Segunda execução lê < 10% dos dados (WATERMARK)
- Logs mostram `INCREMENTAL MODE ENABLED`

### Sucesso Parcial ⚠️
- Incremental ativo mas estratégia SNAPSHOT
- Bronze tem colunas técnicas
- Lê tudo sempre (comportamento esperado para SNAPSHOT)

### Falha ❌
- Discovery não roda (fica PENDING)
- Bronze sem colunas técnicas
- Logs mostram `FULL REFRESH MODE` mesmo com `enable_incremental=TRUE`
- **Causa provável**: Job Databricks usando orquestrador desatualizado

---

## 📞 Próximos Passos

### Se Tudo Funcionar
1. Testar com outros datasets
2. Ativar gradualmente (5-10 datasets pequenos)
3. Monitorar redução de I/O

### Se Não Funcionar
1. Validar configuração do job Databricks
2. Testar import manual do notebook incremental
3. Verificar logs completos do job no Databricks UI

---

## 📄 Queries Úteis

```sql
-- Ver todos datasets com incremental ativo
SELECT 
  dataset_id,
  dataset_name,
  incremental_strategy,
  bronze_mode,
  enable_incremental
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE enable_incremental = TRUE;

-- Ver watermarks ativos
SELECT 
  d.dataset_name,
  w.watermark_value,
  w.watermark_type,
  w.updated_at
FROM cm_dbx_dev.ingestion_sys_ops.dataset_watermark w
JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control d ON w.dataset_id = d.dataset_id
ORDER BY w.updated_at DESC;

-- Ver últimas execuções
SELECT 
  d.dataset_name,
  b.run_id,
  b.status,
  b.started_at,
  b.finished_at,
  TIMESTAMPDIFF(SECOND, b.started_at, b.finished_at) as duration_seconds
FROM cm_dbx_dev.ingestion_sys_ops.batch_process b
JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control d ON b.dataset_id = d.dataset_id
WHERE d.dataset_name = 'CMASTER.GLO_GRUPO_USUARIO@CMASTERPRD'
ORDER BY b.started_at DESC
LIMIT 5;
```
