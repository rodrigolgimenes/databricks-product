# 🚀 Quick Start: Executando o Orchestrator

## 🚨 Problema: Jobs ficam em "PENDING" eternamente

Se você vê jobs com status `PENDING` na interface e eles nunca são executados, é porque **o orchestrator não está rodando**!

## ✅ Solução Rápida

### Opção 1: Execução Manual (Teste)

#### No Databricks:
1. Abra o notebook `databricks_notebooks/governed_ingestion_orchestrator.py` no Databricks
2. Clique em "Run All"
3. Aguarde os logs aparecerem

#### Localmente (MCP):
```powershell
cd C:\dev\cm-databricks\mcp-databricks-server
& .\.venv\Scripts\python.exe .\phase3_run_queue_worker_once.py
```

### Opção 2: Criar Job Automático (Produção)

1. Databricks → **Workflows** → **Jobs** → **Create Job**
2. **Name**: `governed-ingestion-orchestrator`
3. **Notebook**: `governed_ingestion_orchestrator.py`
4. **Schedule**: A cada 5 minutos (`0 */5 * * * ?`)
5. **Cluster**: Single Node, Runtime 14.3 LTS

## 🔍 Verificar Status

### Via API:
```bash
curl http://localhost:3000/api/portal/orchestrator/status
```

### Via SQL (Databricks):
```sql
-- Jobs pendentes
SELECT queue_id, dataset_id, status, requested_at
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE status = 'PENDING'
ORDER BY requested_at ASC;

-- Últimas execuções
SELECT queue_id, status, started_at, finished_at
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE status IN ('SUCCEEDED', 'FAILED')
ORDER BY finished_at DESC
LIMIT 10;
```

## 📚 Documentação Completa

Veja `docs/ORCHESTRATOR_EXECUTION.md` para documentação detalhada, troubleshooting e configurações avançadas.

## 📊 Logs Adicionados

Agora o sistema tem logs avançados em:

### Backend (Node.js):
```
[ENQUEUE] ✓ Registro inserido com sucesso na run_queue
[ENQUEUE] queue_id=3c948f8c-a8bf-42ca-84f8-b9aa76ed4fba
[ENQUEUE] IMPORTANTE: Certifique-se de que o orchestrator está rodando!
```

### Orchestrator (Databricks):
```
[CLAIM] ✓ 1 jobs foram claimed com sucesso
[RUN] Iniciando execução - queue_id=3c948f8c...
[RUN:BRONZE] ✓ Carga concluída com sucesso!
[RUN:SILVER] ✓ MERGE concluído!
[RUN] ✓ EXECUÇÃO CONCLUÍDA COM SUCESSO!
```

## 🆕 Novo Endpoint de Health Check

```bash
curl http://localhost:3000/api/portal/orchestrator/status
```

Retorna:
- Status do orchestrator (ativo/inativo)
- Contagem de jobs pendentes/em execução
- Últimas execuções
- Alertas se há problemas
