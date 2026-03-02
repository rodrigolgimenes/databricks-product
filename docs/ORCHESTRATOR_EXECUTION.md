# Governed Ingestion Orchestrator - Guia de Execução e Troubleshooting

## 🚨 Problema Comum: Jobs Ficam na Fila

### Sintoma
- Jobs aparecem como `PENDING` na interface "Data Load Tool — Execução"
- Status: "Na fila (aguardando Orchestrator)"
- Tabelas Bronze/Silver não são criadas
- `queue_id` aparece mas não há `run_id`

### Causa Raiz
**Nenhum orchestrator está rodando para processar a fila!**

O sistema funciona assim:
1. ✅ Usuário clica em "Executar" → Registro inserido na `run_queue` com status `PENDING`
2. ❌ **Orchestrator deveria processar a fila** → MAS NÃO ESTÁ RODANDO
3. ❌ Job fica eternamente em `PENDING`

---

## ✅ Solução: Executar o Orchestrator

### Opção 1: Execução Manual (Para Testes)

#### Via Notebook do Databricks

1. **Acesse o Databricks Workspace**
   - URL: Seu workspace Databricks

2. **Abra o notebook** `governed_ingestion_orchestrator.py`
   - Caminho: `/databricks_notebooks/governed_ingestion_orchestrator.py`
   - Ou importe do repositório local

3. **Configure os parâmetros** (widgets):
   ```python
   catalog = "cm_dbx_dev"              # Catalog Unity
   max_items = 5                        # Jobs por execução
   max_parallelism = 1                  # MVP: sempre 1
   claim_owner = "orchestrator-manual"  # Identificador
   ```

4. **Execute o notebook completo**
   - Clique em "Run All" no menu superior
   - Ou use `Ctrl + Shift + Enter`

5. **Acompanhe os logs**
   ```
   [CLAIM] Iniciando claim de até 5 jobs PENDING...
   [CLAIM] ✓ 1 jobs foram claimed com sucesso
   [CLAIM]   - queue_id=3c948f8c..., dataset_id=c74c977a..., attempt=0
   
   ================================================================================
   [RUN] Iniciando execução - queue_id=3c948f8c..., dataset_id=c74c977a...
   [RUN] Attempt: 0 / 3
   ================================================================================
   [RUN] run_id gerado: 12345678-...
   [RUN:BRONZE] Iniciando carga Bronze...
   [RUN:BRONZE] ✓ Carga concluída com sucesso!
   [RUN:BRONZE] Registros carregados: 1,234
   [RUN:SILVER] Iniciando promoção para Silver...
   [RUN:SILVER] ✓ MERGE concluído! Registros processados: 1,234
   [RUN] ✓ EXECUÇÃO CONCLUÍDA COM SUCESSO!
   ```

#### Via Scripts Python Locais (MCP)

**Para processos individuais:**
```powershell
# Navegar para o diretório MCP
cd C:\dev\cm-databricks\mcp-databricks-server

# Processar um job da fila
& .\.venv\Scripts\python.exe .\phase3_run_queue_worker_once.py

# Ou filtrar por dataset específico
$env:DATASET_ID = "c74c977a-3256-4067-aa75-1891a5a7ad76"
& .\.venv\Scripts\python.exe .\phase3_run_queue_worker_once.py
```

---

### Opção 2: Execução Automatizada (Produção)

#### Criar Databricks Job

1. **Acesse o Databricks Workspace** → **Workflows** → **Jobs**

2. **Clique em "Create Job"**

3. **Configure o Job:**
   - **Name**: `governed-ingestion-orchestrator`
   - **Task Type**: `Notebook`
   - **Source**: Select your notebook `governed_ingestion_orchestrator.py`
   
4. **Cluster Configuration:**
   - **Cluster Mode**: `New Job Cluster` (recomendado para custos)
   - **Cluster Size**: `Single Node` (MVP)
   - **Runtime**: `14.3 LTS` ou superior
   - **Node Type**: `Standard_DS3_v2` ou equivalente

5. **Parâmetros do Notebook:**
   ```
   catalog: cm_dbx_dev
   max_items: 10
   max_parallelism: 1
   claim_owner: orchestrator-job
   ```

6. **Schedule (Agendamento):**
   - **Trigger Type**: `Scheduled`
   - **Cron Schedule**: 
     - **A cada 5 minutos**: `0 */5 * * * ?`
     - **A cada 10 minutos**: `0 */10 * * * ?`
     - **Horário comercial (9h-18h)**: `0 */5 9-18 * * ?`
   - **Timezone**: `America/Sao_Paulo`

7. **Alerts (Opcional):**
   - Configure notificações por email em caso de falha

8. **Clique em "Create"**

---

## 🔍 Verificação de Status

### 1. Via API (Health Check)

```bash
curl http://localhost:3000/api/portal/orchestrator/status
```

**Resposta Esperada:**
```json
{
  "ok": true,
  "orchestrator_status": {
    "likely_active": true,
    "warning": null
  },
  "queue_stats": {
    "pending": 0,
    "running": 0
  },
  "recent_activity": {
    "last_processed": {
      "queue_id": "3c948f8c-...",
      "status": "SUCCEEDED",
      "finished_at": "2026-02-07T12:45:00.000Z"
    }
  },
  "oldest_pending": []
}
```

**⚠️ Alerta:**
```json
{
  "orchestrator_status": {
    "likely_active": false,
    "warning": "Há jobs pendentes mas nenhum orchestrator parece estar ativo nos últimos 5 minutos"
  },
  "queue_stats": {
    "pending": 3,
    "running": 0
  }
}
```

### 2. Via SQL (Databricks)

```sql
-- Jobs pendentes
SELECT 
  queue_id,
  dataset_id,
  status,
  requested_at,
  attempt,
  next_retry_at
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE status = 'PENDING'
ORDER BY requested_at ASC;

-- Últimas execuções
SELECT 
  queue_id,
  dataset_id,
  status,
  claim_owner,
  started_at,
  finished_at
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE status IN ('SUCCEEDED', 'FAILED')
ORDER BY finished_at DESC
LIMIT 10;

-- Batch process recentes
SELECT
  run_id,
  dataset_id,
  status,
  bronze_row_count,
  silver_row_count,
  started_at,
  finished_at,
  error_message
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
ORDER BY started_at DESC
LIMIT 10;
```

---

## 📊 Logs Avançados

### Logs no Backend (Node.js)

Quando um job é enfileirado, você verá:

```
[ENQUEUE] Início da requisição - dataset_id=c74c977a..., user=portal
[ENQUEUE] Consultando dataset_control para dataset_id=c74c977a...
[ENQUEUE] Dataset encontrado - name=cmvw_desp_total, state=ACTIVE, bronze=cm_dbx_dev.bronze_mega.cmvw_desp_total, silver=cm_dbx_dev.silver_mega.cmvw_desp_total
[ENQUEUE] Inserindo na run_queue - queue_id=3c948f8c-a8bf-42ca-84f8-b9aa76ed4fba, dataset_id=c74c977a..., status=PENDING
[ENQUEUE] ✓ Registro inserido com sucesso na run_queue
[ENQUEUE] queue_id=3c948f8c-a8bf-42ca-84f8-b9aa76ed4fba (primeiros 8 chars para UI)
[ENQUEUE] Aguardando orchestrator processar a fila...
[ENQUEUE] IMPORTANTE: Certifique-se de que o orchestrator está rodando no Databricks!
```

### Logs no Orchestrator (Databricks)

Durante a execução, você verá logs detalhados de cada etapa:

```
[CLAIM] Iniciando claim de até 5 jobs PENDING...
[CLAIM] ✓ 1 jobs foram claimed com sucesso
[CLAIM]   - queue_id=3c948f8c..., dataset_id=c74c977a..., attempt=0

================================================================================
[RUN] Iniciando execução - queue_id=3c948f8c..., dataset_id=c74c977a...
[RUN] Attempt: 0 / 3
================================================================================
[RUN] run_id gerado: 0907fcfc-b676-4570-9f5b-270dce1e358c
[RUN] Marcando queue como RUNNING e criando batch_process...

[RUN:BRONZE] Iniciando carga Bronze...
[RUN:BRONZE] source_type=ORACLE, dataset_name=CMVW_DESP_TOTAL
[RUN:BRONZE] bronze_table=cm_dbx_dev.bronze_mega.cmvw_desp_total
[RUN:BRONZE] ✓ Carga concluída com sucesso!
[RUN:BRONZE] Registros carregados: 15,234

[RUN:SILVER] Iniciando promoção para Silver...
[RUN:SILVER] silver_table=cm_dbx_dev.silver_mega.cmvw_desp_total
[RUN:SILVER] Buscando schema ACTIVE para dataset_id=c74c977a...
[RUN:SILVER] ✓ Schema ACTIVE encontrado
[RUN:SILVER] Aplicando transformações (cast + dedupe + merge)...
[RUN:SILVER] Aplicando cast plan...
[RUN:SILVER] Aplicando deduplicação LWW...
[RUN:SILVER] Executando MERGE na tabela Silver...
[RUN:SILVER] ✓ MERGE concluído! Registros processados: 15,234

[RUN] ✓ EXECUÇÃO CONCLUÍDA COM SUCESSO!
[RUN] Bronze: 15,234 registros, Silver: 15,234 registros
[RUN] Finalizando run_id=0907fcfc-b676-4570-9f5b-270dce1e358c...
================================================================================
```

---

## ⚠️ Troubleshooting

### Problema 1: Orchestrator não encontra jobs

**Sintoma:**
```
[CLAIM] ✓ 0 jobs foram claimed com sucesso
```

**Possíveis causas:**
1. Não há jobs `PENDING` na fila
2. Jobs têm `next_retry_at` futuro (aguardando retry)
3. Jobs estão em estado bloqueado (`PAUSED`, `DEPRECATED`, `BLOCKED_SCHEMA_CHANGE`)

**Solução:**
```sql
-- Verificar todos os jobs
SELECT queue_id, dataset_id, status, next_retry_at
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
ORDER BY requested_at DESC
LIMIT 20;

-- Forçar retry imediato se necessário
UPDATE cm_dbx_dev.ingestion_sys_ops.run_queue
SET next_retry_at = NULL
WHERE queue_id = '3c948f8c-a8bf-42ca-84f8-b9aa76ed4fba';
```

### Problema 2: Erro "NO_ACTIVE_SCHEMA"

**Sintoma:**
```
[RUN:SILVER] ✗ ERRO: Nenhum schema ACTIVE encontrado!
```

**Causa:** Dataset não tem schema aprovado

**Solução:**
1. Acesse o portal
2. Vá para "Aprovações Pendentes"
3. Aprove o schema pendente para o dataset
4. Reexecute o job

### Problema 3: Job fica em CLAIMED mas não progride

**Sintoma:** Status fica em `CLAIMED` por muito tempo

**Causa:** Orchestrator travou/morreu após claim

**Solução:**
```sql
-- Resetar jobs travados (mais de 30min em CLAIMED)
UPDATE cm_dbx_dev.ingestion_sys_ops.run_queue
SET status = 'PENDING', claim_owner = NULL, claimed_at = NULL
WHERE status = 'CLAIMED' 
  AND claimed_at < current_timestamp() - INTERVAL 30 MINUTES;
```

---

## 🔧 Configurações Avançadas

### Ajustar Paralelismo

No notebook, modifique:
```python
MAX_PARALLELISM = 3  # Processar até 3 jobs simultaneamente
```

**⚠️ Atenção:**
- Aumentar paralelismo requer cluster maior
- MVP usa `MAX_PARALLELISM = 1`

### Ajustar Retry Policy

Na criação do job (via API ou portal):
```javascript
{
  "max_retries": 3,        // Número de tentativas
  "priority": 100          // Menor = maior prioridade
}
```

---

## 📞 Suporte

- **Logs do Backend:** Console do Node.js (`node server.js`)
- **Logs do Orchestrator:** Output do notebook Databricks
- **Tabelas de Auditoria:**
  - `cm_dbx_dev.ingestion_sys_ops.run_queue`
  - `cm_dbx_dev.ingestion_sys_ops.batch_process`
  - `cm_dbx_dev.ingestion_sys_ops.batch_process_steps`
  - `cm_dbx_dev.ingestion_sys_ops.batch_process_table_details`
