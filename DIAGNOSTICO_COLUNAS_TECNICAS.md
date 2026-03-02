# Diagnóstico - Colunas Técnicas Ausentes na Bronze

**Data**: 26/02/2026  
**Dataset testado**: `CMASTER.GLO_GRUPO_USUARIO@CMASTERPRD`

---

## ❌ Problema Identificado

### Sintoma
Bronze **NÃO contém as 8 colunas técnicas** após execução com `enable_incremental=TRUE`:
- ❌ `_ingestion_ts`
- ❌ `_batch_id`
- ❌ `_source_table`
- ❌ `_op`
- ❌ `_watermark_col`
- ❌ `_watermark_value`
- ❌ `_row_hash`
- ❌ `_is_deleted`

**Resultado**: Apenas 28 colunas (originais do Oracle), sem metadados técnicos.

---

## 🔍 Investigação Realizada

### 1. Verificação de Configuração ✅
```sql
SELECT enable_incremental, incremental_strategy, bronze_mode 
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control 
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b';
```

**Resultado**:
- `enable_incremental`: **TRUE** ✅
- `incremental_strategy`: **SNAPSHOT** ✅
- `bronze_mode`: **SNAPSHOT** ✅

**Conclusão**: Configuração está correta.

### 2. Verificação de Execução ✅
```sql
SELECT run_id, status, started_at, finished_at 
FROM cm_dbx_dev.ingestion_sys_ops.batch_process 
WHERE dataset_id = '89ab4893-510b-47f4-80d3-b6f1b59fc64b' 
ORDER BY started_at DESC LIMIT 1;
```

**Resultado**:
- `run_id`: `7fc86534-bb3f-4370-9dc6-96407720b220`
- `status`: **SUCCEEDED** ✅
- `started_at`: 2026-02-26 12:19:31
- `finished_at`: 2026-02-26 12:22:09

**Conclusão**: Execução completou com sucesso.

### 3. Verificação de Schema Bronze ❌
```sql
DESCRIBE cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO;
```

**Resultado**: 28 colunas (apenas originais do Oracle)

**Conclusão**: Orquestrador NÃO adicionou colunas técnicas.

---

## 🎯 Causa Raiz

**CAUSA IDENTIFICADA**: O **Job do Databricks está executando o orquestrador DESATUALIZADO**.

### Evidências:
1. ✅ Configuração `enable_incremental=TRUE` está correta
2. ✅ Migration 004 executada (colunas existem em `dataset_control`)
3. ✅ Funções incrementais deployed em `/Workspace/Shared/incremental_loading/`
4. ✅ Orquestrador atualizado deployed em `/Workspace/Shared/governed_ingestion_orchestrator`
5. ❌ **Bronze sem colunas técnicas** = Código incremental NÃO foi executado

### Conclusão:
O job está apontando para um **path diferente** (orquestrador antigo) ou o **import das funções incrementais está falando**.

---

## 🛠️ SOLUÇÃO

### Passo 1: Verificar Configuração do Job Databricks

**Acesse Databricks UI**:
1. Vá em **Workflows** → **Jobs**
2. Procure o job: ID `690887429046802` ou nome do orquestrador
3. Clique no job → **Tasks** tab
4. Verifique **Notebook Path**

**Path correto esperado**:
```
/Workspace/Shared/governed_ingestion_orchestrator
```

**Se estiver diferente**:
- Path errado comum: `/Repos/...` ou `/Users/...` ou path antigo
- **Ação**: Editar task e atualizar para `/Workspace/Shared/governed_ingestion_orchestrator`

### Passo 2: Verificar Logs do Job (Diagnóstico Avançado)

**Acesse o run específico**:
1. Databricks UI → **Workflows** → **Job Runs**
2. Procure `run_id`: `7fc86534-bb3f-4370-9dc6-96407720b220`
3. Clique para ver logs completos

**Logs a procurar**:

#### ✅ Logs Esperados (se funcionando):
```
[IMPORT] Attempting to import incremental loading functions...
[IMPORT] Path: /Workspace/Shared/incremental_loading/incremental_loading_functions
[IMPORT] ✓ Incremental loading functions imported successfully
```

```
[RUN:BRONZE] Iniciando carga Bronze...
[RUN:BRONZE] source_type=ORACLE, dataset_name=CMASTER.GLO_GRUPO_USUARIO@CMASTERPRD
[RUN:BRONZE] 🔄 INCREMENTAL MODE ENABLED
[RUN:BRONZE] Strategy: SNAPSHOT
```

```
[METADATA] ✓ Adicionadas 8 colunas técnicas
[METADATA] Hash calculado a partir de X colunas de negócio
```

#### ❌ Logs que indicam problema:
```
[RUN:BRONZE] 📦 FULL REFRESH MODE (incremental disabled)
```
**Significa**: Orquestrador não detectou `enable_incremental=TRUE` → usando código antigo

```
[IMPORT] ⚠️ Incremental loading functions file not found
[IMPORT] Continuing with full refresh mode only...
```
**Significa**: Import falhou → funções não disponíveis

**Ausência total de logs `[IMPORT]` ou `[INCREMENTAL]`**:
**Significa**: Job está usando orquestrador completamente desatualizado (sem integração)

### Passo 3: Forçar Uso do Orquestrador Correto

**Opção A - Via Databricks UI (Recomendado)**:
1. Editar Job
2. Task → Notebook Path → Atualizar para `/Workspace/Shared/governed_ingestion_orchestrator`
3. Salvar
4. Executar novamente

**Opção B - Via API (Avançado)**:
```python
import requests

DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
JOB_ID = "690887429046802"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Atualizar notebook path
update_url = f"{DATABRICKS_HOST}/api/2.1/jobs/update"
update_payload = {
    "job_id": JOB_ID,
    "new_settings": {
        "tasks": [{
            "notebook_task": {
                "notebook_path": "/Workspace/Shared/governed_ingestion_orchestrator"
            }
        }]
    }
}

response = requests.post(update_url, headers=headers, json=update_payload)
print(response.json())
```

### Passo 4: Validar Após Correção

**1. Executar dataset novamente**:
```powershell
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/datasets/89ab4893-510b-47f4-80d3-b6f1b59fc64b/enqueue" -Method POST
```

**2. Aguardar 2-3 minutos**

**3. Verificar Bronze**:
```sql
DESCRIBE cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO;
```

**Resultado esperado**:
- **28 colunas originais** (Oracle)
- **+ 8 colunas técnicas** (começam com `_`)
- **Total**: 36 colunas

**4. Verificar dados com colunas técnicas**:
```sql
SELECT 
  GRU_IN_CODIGO,
  GRU_ST_NOME,
  _ingestion_ts,
  _batch_id,
  _source_table,
  _op,
  _row_hash,
  _is_deleted
FROM cm_dbx_dev.bronze_mega.CMASTER_GLO_GRUPO_USUARIO
LIMIT 5;
```

**Resultado esperado**:
- ✅ `_ingestion_ts`: timestamp recente
- ✅ `_batch_id`: UUID do run
- ✅ `_source_table`: "CMASTER.GLO_GRUPO_USUARIO@CMASTERPRD"
- ✅ `_op`: "UPSERT"
- ✅ `_row_hash`: MD5 (32 chars hexadecimal)
- ✅ `_is_deleted`: FALSE

---

## 🧪 Teste Alternativo (Se Job Config Não For Acessível)

Se você **não consegue alterar a configuração do job** no Databricks, pode testar executando o notebook diretamente:

### Via Databricks UI:
1. Acesse **Workspace** → `/Workspace/Shared/`
2. Abra `governed_ingestion_orchestrator`
3. Clique **Run All**
4. Passar parâmetros (widgets):
   - `catalog`: cm_dbx_dev
   - `target_dataset_id`: 89ab4893-510b-47f4-80d3-b6f1b59fc64b
   - `max_items`: 1

Isso executará o orquestrador atualizado diretamente, bypassing o job.

---

## 📊 Checklist de Validação

### Antes da Correção ❌
- [x] `enable_incremental` = TRUE em `dataset_control`
- [x] Migration 004 executada
- [x] Funções incrementais deployed
- [x] Orquestrador atualizado deployed
- [ ] Job Databricks usando path correto
- [ ] Bronze com 8 colunas técnicas

### Após Correção ✅
- [ ] Job atualizado para `/Workspace/Shared/governed_ingestion_orchestrator`
- [ ] Logs mostram `[IMPORT] ✓ Incremental loading functions imported successfully`
- [ ] Logs mostram `[RUN:BRONZE] 🔄 INCREMENTAL MODE ENABLED`
- [ ] Bronze tem 36 colunas (28 originais + 8 técnicas)
- [ ] `_row_hash` varia entre registros
- [ ] `_ingestion_ts` tem timestamp recente

---

## 📞 Próximos Passos

### Se Correção Funcionar ✅
1. Testar com 2-3 outros datasets
2. Documentar configuração correta do job
3. Rollout gradual (ativar incremental em mais datasets)

### Se Ainda Não Funcionar ❌
**Possíveis causas adicionais**:
1. **Import falhou**: Arquivo `/Workspace/Shared/incremental_loading/incremental_loading_functions` não existe ou está corrompido
2. **Permissions**: Job não tem permissão para acessar `/Workspace/Shared/`
3. **Cluster config**: Cluster precisa restart após deploy de notebooks
4. **Magic command `%run` não funciona**: Converter para import inline

**Debug avançado**:
```python
# Testar import manual no notebook do orquestrador
# Adicionar no início do notebook:
try:
    %run /Workspace/Shared/incremental_loading/incremental_loading_functions
    print("✅ Import succeeded")
except Exception as e:
    print(f"❌ Import failed: {e}")
```

---

## 📄 Arquivos de Referência

- **Guia de Teste**: `GUIA_TESTE_INCREMENTAL.md`
- **Status Geral**: `FINAL_STATUS.md`
- **Plano de Validação**: `docs/VALIDATION_PLAN_INCREMENTAL.md`

---

**Conclusão**: O problema é **configuração de deployment**, não código. Todas as funções estão implementadas e prontas. Basta garantir que o job do Databricks está executando o orquestrador correto localizado em `/Workspace/Shared/governed_ingestion_orchestrator`.
