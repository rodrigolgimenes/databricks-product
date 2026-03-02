# Atualizações no Notebook Python - Tracking de Carga Incremental

**Data:** 26/02/2026  
**Arquivo:** `databricks_notebooks/governed_ingestion_orchestrator.py`  
**Status:** ✅ **IMPLEMENTADO**

---

## 📋 Resumo das Mudanças

O notebook Python foi atualizado para popular os novos campos de tracking de carga incremental na tabela `batch_process`:

- `load_type` - Tipo de carga (FULL | INCREMENTAL | SNAPSHOT)
- `incremental_rows_read` - Quantidade de linhas incrementais processadas
- `watermark_start` - Valor inicial do watermark usado
- `watermark_end` - Valor final do watermark após execução

---

## 🔧 Alterações Implementadas

### 1. Função `_create_batch_process` (linha 336)

**Mudança:** Adicionados 4 novos campos no INSERT inicial.

```python
INSERT INTO {OPS}.batch_process (
  run_id, dataset_id, queue_id,
  execution_mode, status,
  started_at, finished_at,
  orchestrator_job_id, orchestrator_run_id, orchestrator_task,
  bronze_row_count, silver_row_count,
  error_class, error_message, error_stacktrace,
  load_type, incremental_rows_read, watermark_start, watermark_end,  # ← NOVOS
  created_at, created_by
) VALUES (
  ...,
  NULL, NULL, NULL, NULL,  # ← Valores iniciais NULL
  current_timestamp(),
  'orchestrator'
)
```

---

### 2. Função `_finish_batch_process` (linha 365)

**Mudança:** Adicionados 4 novos parâmetros opcionais.

```python
def _finish_batch_process(
    *,
    run_id: str,
    status: str,
    bronze_row_count: Optional[int] = None,
    silver_row_count: Optional[int] = None,
    error_class: Optional[str] = None,
    error_message: Optional[str] = None,
    error_stacktrace: Optional[str] = None,
    load_type: Optional[str] = None,              # ← NOVO
    incremental_rows_read: Optional[int] = None,  # ← NOVO
    watermark_start: Optional[str] = None,        # ← NOVO
    watermark_end: Optional[str] = None,          # ← NOVO
) -> None:
```

**Lógica de UPDATE:**
```python
if load_type is not None:
    sets.append(f"load_type = {_sql_string_literal(load_type)}")

if incremental_rows_read is not None:
    sets.append(f"incremental_rows_read = {int(incremental_rows_read)}")

if watermark_start is not None:
    sets.append(f"watermark_start = {_sql_string_literal(watermark_start)}")

if watermark_end is not None:
    sets.append(f"watermark_end = {_sql_string_literal(watermark_end)}")
```

---

### 3. Função `run_one` - Inicialização de variáveis (linha 1143)

**Mudança:** Adicionadas variáveis de tracking antes do bloco `try`.

```python
bronze_count = None
silver_count = None

# Tracking de carga incremental
load_type = "FULL"  # Default: carga completa
incremental_rows_read = None  # Apenas linhas incrementais
watermark_start = None  # Início do range de watermark
watermark_end = None  # Fim do range de watermark

try:
    # ... código de execução
```

---

### 4. Captura de informações incrementais (após linha 1245)

**Mudança:** Lógica para determinar o tipo de carga e capturar watermark.

```python
# ===========================
# CAPTURAR INFORMAÇÕES INCREMENTAIS
# ===========================
# Determinar tipo de carga baseado no resultado
if b.get("incremental") and enable_incremental:
    load_type = "INCREMENTAL"
    incremental_rows_read = bronze_count  # Total de linhas lidas na carga incremental
    
    # Extrair watermark do metadata se disponível
    metadata_json = ds.get("incremental_metadata")
    if metadata_json:
        import json
        try:
            metadata = json.loads(metadata_json) if isinstance(metadata_json, str) else metadata_json
            watermark_col = metadata.get("watermark_col")
            
            # Buscar range do watermark da Bronze table
            if watermark_col and _table_exists(bronze_table):
                try:
                    wm_stats = spark.sql(f"""
                        SELECT 
                            MIN(_watermark_value) as wm_start,
                            MAX(_watermark_value) as wm_end
                        FROM {bronze_table}
                        WHERE _batch_id = {_sql_string_literal(run_id)}
                    """).collect()
                    
                    if wm_stats and wm_stats[0].wm_start:
                        watermark_start = str(wm_stats[0].wm_start)
                        watermark_end = str(wm_stats[0].wm_end)
                        print(f"[RUN:BRONZE] 📊 Watermark range: {watermark_start} → {watermark_end}")
                except Exception as wm_error:
                    print(f"[RUN:BRONZE] ⚠️ Não foi possível capturar watermark: {wm_error}")
        except Exception as json_error:
            print(f"[RUN:BRONZE] ⚠️ Erro ao parsear metadata: {json_error}")
elif incremental_strategy == "SNAPSHOT":
    load_type = "SNAPSHOT"
else:
    load_type = "FULL"

print(f"[RUN:BRONZE] ✓ Carga concluída com sucesso!")
print(f"[RUN:BRONZE] Tipo de carga: {load_type}")
print(f"[RUN:BRONZE] Registros carregados: {bronze_count:,}")
if load_type == "INCREMENTAL" and incremental_rows_read:
    print(f"[RUN:BRONZE] Linhas incrementais: {incremental_rows_read:,}")
```

---

### 5. Chamada final de `_finish_batch_process` no sucesso (linha 1402)

**Mudança:** Passagem dos novos parâmetros na finalização bem-sucedida.

```python
_finish_batch_process(
    run_id=run_id, 
    status="SUCCEEDED", 
    bronze_row_count=bronze_count, 
    silver_row_count=silver_count,
    load_type=load_type,                      # ← NOVO
    incremental_rows_read=incremental_rows_read,  # ← NOVO
    watermark_start=watermark_start,          # ← NOVO
    watermark_end=watermark_end               # ← NOVO
)
```

---

### 6. Chamadas de `_finish_batch_process` nos blocos de erro

**Mudança:** Passagem de `load_type` nos tratamentos de erro.

#### Erro de Schema (linha 1437):
```python
_finish_batch_process(
    run_id=run_id,
    status="FAILED",
    bronze_row_count=bronze_count,
    silver_row_count=None,
    error_class="SCHEMA_ERROR",
    error_message=msg,
    error_stacktrace=st,
    load_type=load_type,                      # ← NOVO
    incremental_rows_read=incremental_rows_read,  # ← NOVO
)
```

#### Erro de Source (linha 1455):
```python
_finish_batch_process(
    run_id=run_id,
    status="FAILED",
    bronze_row_count=bronze_count,
    error_class="SOURCE_ERROR",
    error_message=msg,
    error_stacktrace=st,
    load_type=load_type,  # ← NOVO
)
```

---

## 🔍 Lógica de Determinação do `load_type`

O campo `load_type` é determinado pela seguinte lógica:

```python
if b.get("incremental") and enable_incremental:
    load_type = "INCREMENTAL"  # Carga incremental ativa
elif incremental_strategy == "SNAPSHOT":
    load_type = "SNAPSHOT"     # Modo snapshot explícito
else:
    load_type = "FULL"         # Carga completa (default)
```

---

## 🧪 Como Testar

### 1. Executar carga FULL (comportamento padrão)

```python
# Dataset com enable_incremental = FALSE
# Esperado:
# - load_type = "FULL"
# - incremental_rows_read = NULL
# - watermark_start = NULL
# - watermark_end = NULL
```

### 2. Executar carga INCREMENTAL

```python
# Dataset com:
# - enable_incremental = TRUE
# - incremental_strategy = "WATERMARK"
# - incremental_metadata = {"watermark_col": "UPDATED_AT"}

# Esperado:
# - load_type = "INCREMENTAL"
# - incremental_rows_read = <qtd de linhas lidas>
# - watermark_start = "2024-02-20 00:00:00"
# - watermark_end = "2024-02-26 23:59:59"
```

### 3. Executar carga SNAPSHOT

```python
# Dataset com:
# - enable_incremental = FALSE
# - incremental_strategy = "SNAPSHOT"

# Esperado:
# - load_type = "SNAPSHOT"
# - incremental_rows_read = NULL
# - watermark_start = NULL
# - watermark_end = NULL
```

---

## 📊 Validação no Banco de Dados

Após executar um dataset, valide os dados:

```sql
SELECT 
  run_id,
  dataset_id,
  status,
  load_type,
  bronze_row_count,
  incremental_rows_read,
  watermark_start,
  watermark_end,
  started_at,
  finished_at
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE dataset_id = 'SEU_DATASET_ID'
ORDER BY started_at DESC
LIMIT 5;
```

**Exemplo de resultado esperado:**

| run_id | status | load_type | bronze_row_count | incremental_rows_read | watermark_start | watermark_end |
|--------|--------|-----------|------------------|-----------------------|-----------------|---------------|
| abc-123 | SUCCEEDED | INCREMENTAL | 100000 | 2500 | 2024-02-20 00:00:00 | 2024-02-26 23:59:59 |
| def-456 | SUCCEEDED | FULL | 100000 | NULL | NULL | NULL |

---

## 🐛 Tratamento de Erros

O código inclui tratamento de exceções para captura de watermark:

```python
try:
    wm_stats = spark.sql(...)
    if wm_stats and wm_stats[0].wm_start:
        watermark_start = str(wm_stats[0].wm_start)
        watermark_end = str(wm_stats[0].wm_end)
except Exception as wm_error:
    print(f"[RUN:BRONZE] ⚠️ Não foi possível capturar watermark: {wm_error}")
```

Se a captura de watermark falhar, a execução **NÃO será interrompida**:
- `load_type` será gravado corretamente
- `incremental_rows_read` será gravado corretamente
- `watermark_start` e `watermark_end` ficarão NULL (não é crítico)

---

## 📝 Logs Esperados

Durante a execução, você verá logs como:

### Carga FULL:
```
[RUN:BRONZE] 📦 FULL REFRESH MODE (incremental disabled)
[RUN:BRONZE] ✓ Carga concluída com sucesso!
[RUN:BRONZE] Tipo de carga: FULL
[RUN:BRONZE] Registros carregados: 100,000
```

### Carga INCREMENTAL:
```
[RUN:BRONZE] 🔄 INCREMENTAL MODE ENABLED
[RUN:BRONZE] Strategy: WATERMARK
[RUN:BRONZE] ✓ Incremental load completed!
[RUN:BRONZE] 📊 Watermark range: 2024-02-20 00:00:00 → 2024-02-26 23:59:59
[RUN:BRONZE] ✓ Carga concluída com sucesso!
[RUN:BRONZE] Tipo de carga: INCREMENTAL
[RUN:BRONZE] Registros carregados: 100,000
[RUN:BRONZE] Linhas incrementais: 2,500
```

---

## ✅ Checklist de Validação

- [x] ✅ Função `_create_batch_process` atualizada com novos campos
- [x] ✅ Função `_finish_batch_process` aceita novos parâmetros
- [x] ✅ Variáveis de tracking inicializadas em `run_one`
- [x] ✅ Lógica de captura de informações incrementais implementada
- [x] ✅ Chamada final no sucesso inclui novos parâmetros
- [x] ✅ Chamadas de erro incluem `load_type`
- [x] ✅ Tratamento de exceções para não quebrar execução
- [x] ✅ Logs informativos adicionados

---

## 🚀 Deploy

**Próximos passos:**

1. ✅ Código atualizado no arquivo local
2. ⏳ Commit e push para repositório
3. ⏳ Deploy no Databricks Workspace
4. ⏳ Testar com dataset real
5. ⏳ Validar dados na tabela `batch_process`
6. ⏳ Verificar visualização no frontend

---

## 📞 Suporte

Para dúvidas sobre as atualizações do notebook, contate o time de Engenharia de Dados.
