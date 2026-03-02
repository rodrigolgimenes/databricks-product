# Guia de Execução Targetizada de Datasets

## 📋 Visão Geral

Este guia documenta as melhorias implementadas para permitir execução targetizada (específica) de datasets no orchestrator, além de correções no fluxo de criação e limpeza automática de jobs órfãos.

## ✨ Melhorias Implementadas

### 1. **Execução Targetizada no Orchestrator** ✅
O orchestrator agora suporta dois modos de operação:

#### 🎯 Modo Targetizado (Novo)
- Executa **apenas** o dataset especificado
- Usado quando o usuário clica "Executar" em um dataset específico no portal
- Parâmetro: `target_dataset_id`

#### 📦 Modo Batch (Original)
- Executa **múltiplos datasets** da fila (até `max_items`)
- Usado para processamento agendado/em lote
- Sem parâmetro `target_dataset_id` (ou vazio)

### 2. **Datasets Nascem ACTIVE** ✅
- **Antes:** Datasets criados nasciam em estado `DRAFT` (não executáveis)
- **Agora:** Datasets nascem em estado `ACTIVE` (prontos para execução imediata)
- **Benefício:** UX melhorada - usuário pode executar imediatamente após criar

### 3. **Limpeza Automática de Jobs Órfãos** ✅
- Orchestrator agora detecta e marca como `FAILED` jobs presos há mais de 2 horas
- Previne acúmulo de jobs "travados" na fila
- Executa automaticamente no início de cada run do orchestrator

---

## 🚀 Como Usar: Execução Targetizada

### Opção A: Via Portal (Recomendado)

1. Acesse o portal em `http://localhost:3000/v2.html`
2. Selecione o dataset desejado
3. Clique no botão **"Executar"**
4. O sistema:
   - Enfileira o dataset na `run_queue`
   - (Futuro) Dispara automaticamente o job Databricks com parâmetro targetizado

### Opção B: Manualmente no Databricks

1. Acesse Databricks Workspace
2. Navegue até o Job do Orchestrator
3. Clique em **"Run now with different parameters"**
4. Adicione os parâmetros:
   ```json
   {
     "target_dataset_id": "2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3",
     "max_items": "1"
   }
   ```
5. Clique em **"Run now"**

### Opção C: Via Databricks CLI

```bash
databricks jobs run-now \
  --job-id <ORCHESTRATOR_JOB_ID> \
  --notebook-params '{
    "target_dataset_id": "2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3",
    "max_items": "1"
  }'
```

---

## 📊 Parâmetros do Orchestrator

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `catalog` | string | `cm_dbx_dev` | Catálogo do Unity Catalog |
| `max_items` | int | `5` | Máximo de jobs a clamar da fila |
| `max_parallelism` | int | `1` | Grau de paralelismo (MVP: 1) |
| `target_dataset_id` | string | `""` (vazio) | **NOVO:** ID do dataset para execução targetizada |
| `claim_owner` | string | auto | Identificador único do orchestrator |

---

## 🔍 Logs e Diagnóstico

### Logs do Orchestrator

#### Modo Targetizado
```
🎯 EXECUÇÃO TARGETIZADA ativada para dataset_id=2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3
[CLAIM] 🎯 MODO TARGETIZADO: dataset_id=2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3
[CLAIM] Claiming jobs ONLY for the specified dataset
[CLAIM] ✓ 1 jobs foram claimed com sucesso
```

#### Modo Batch
```
📦 EXECUÇÃO BATCH (processando múltiplos datasets)
[CLAIM] 📦 MODO BATCH: Iniciando claim de até 5 jobs PENDING...
[CLAIM] ✓ 3 jobs foram claimed com sucesso
```

#### Cleanup de Órfãos
```
[CLEANUP] Verificando jobs órfãos (timeout > 120 minutos)...
[CLEANUP] ✓ Limpeza de órfãos concluída
```

### Erros Comuns

#### ❌ Dataset não encontrado na fila
```
[CLAIM] ❌ Dataset 2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3 não encontrado na fila PENDING ou não elegível para execução
```
**Causa:** Dataset não está enfileirado ou já foi processado  
**Solução:** Verifique o estado no portal e re-enfileire se necessário

#### ❌ Dataset em estado não elegível
```
[RUN] NOT_ELIGIBLE state=PAUSED
```
**Causa:** Dataset está em estado `PAUSED`, `DEPRECATED` ou `BLOCKED_SCHEMA_CHANGE`  
**Solução:** Altere o estado do dataset no portal antes de executar

---

## 🛠️ Scripts Utilitários

### 1. Limpeza Manual de Jobs Órfãos
```sql
-- Execute: scripts/cleanup_orphan_jobs.sql
-- Marca jobs RUNNING/PENDING há mais de 2 horas como FAILED
```

### 2. Verificar Estado da Fila
```sql
SELECT status, COUNT(*) as count 
FROM cm_dbx_dev.ingestion_sys_ops.run_queue 
GROUP BY status;
```

### 3. Verificar Jobs de um Dataset Específico
```sql
SELECT 
  queue_id, 
  status, 
  requested_at, 
  started_at, 
  finished_at,
  last_error_message
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE dataset_id = '2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3'
ORDER BY requested_at DESC
LIMIT 10;
```

---

## 📈 Métricas e Monitoramento

### KPIs Disponíveis no Dashboard (v2.html)

1. **Queue Status:**
   - 🟢 SUCCEEDED: Jobs concluídos com sucesso
   - 🟡 PENDING: Aguardando processamento
   - 🔵 RUNNING: Em execução
   - 🔴 FAILED: Falhas (incluindo órfãos detectados)

2. **Execution History:**
   - Histórico de execuções por dataset
   - Duração de cada run
   - Status detalhado de cada etapa

3. **Orphan Jobs:**
   - Jobs marcados como `FAILED` com `last_error_class = 'TIMEOUT'`
   - Indica problemas de infra ou crashes do orchestrator

---

## 🔐 Permissões Necessárias

Para execução targetizada via API (futuro), o backend precisará de:

```env
# .env
DATABRICKS_HOST=adb-1234567890123456.7.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
DATABRICKS_ORCHESTRATOR_JOB_ID=123456789
```

Permissões do token:
- `jobs:run` - Disparar jobs
- `jobs:read` - Ler status de execução

---

## 🧪 Testes de Validação

### Teste 1: Execução Targetizada Manual

1. **Setup:**
   - Enfileire dataset via portal: `POST /api/portal/datasets/{id}/enqueue`
   - Verificar que status = `PENDING`

2. **Execução:**
   - Rodar orchestrator com `target_dataset_id={id}`

3. **Validação:**
   ```sql
   -- Verificar que APENAS 1 job foi processado
   SELECT COUNT(*) as processed_count
   FROM cm_dbx_dev.ingestion_sys_ops.batch_process
   WHERE dataset_id = '2b5d5379-30c8-469e-b4e9-a9e5aa1e3aa3'
     AND started_at > CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES;
   ```

4. **Resultado esperado:** `processed_count = 1`

### Teste 2: Modo Batch (Múltiplos Datasets)

1. **Setup:**
   - Enfileire 3 datasets diferentes

2. **Execução:**
   - Rodar orchestrator SEM `target_dataset_id`

3. **Validação:**
   ```sql
   SELECT COUNT(DISTINCT dataset_id) as datasets_processed
   FROM cm_dbx_dev.ingestion_sys_ops.run_queue
   WHERE status = 'SUCCEEDED'
     AND claimed_at > CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES;
   ```

4. **Resultado esperado:** `datasets_processed = 3`

### Teste 3: Limpeza de Órfãos

1. **Setup:** Simular job órfão (manual via SQL)
   ```sql
   UPDATE cm_dbx_dev.ingestion_sys_ops.run_queue
   SET status = 'RUNNING', 
       started_at = CURRENT_TIMESTAMP() - INTERVAL 3 HOURS
   WHERE queue_id = '<test_queue_id>';
   ```

2. **Execução:**
   - Rodar orchestrator (qualquer modo)

3. **Validação:**
   ```sql
   SELECT status, last_error_class
   FROM cm_dbx_dev.ingestion_sys_ops.run_queue
   WHERE queue_id = '<test_queue_id>';
   ```

4. **Resultado esperado:** `status = 'FAILED', last_error_class = 'TIMEOUT'`

---

## 🐛 Troubleshooting

### Problema: Job targetizado não executa

**Sintomas:**
- Parâmetro `target_dataset_id` fornecido
- Orchestrator termina sem processar nada

**Diagnóstico:**
1. Verificar se dataset está PENDING:
   ```sql
   SELECT status FROM cm_dbx_dev.ingestion_sys_ops.run_queue
   WHERE dataset_id = '<id>' AND status = 'PENDING';
   ```

2. Verificar estado do dataset:
   ```sql
   SELECT execution_state FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
   WHERE dataset_id = '<id>';
   ```

**Soluções:**
- Se não está PENDING → Enfileire novamente
- Se estado != ACTIVE → Altere para ACTIVE no portal
- Verifique logs do orchestrator para erro específico

### Problema: Muitos jobs órfãos

**Sintomas:**
- Dashboard mostra alto volume de jobs com `last_error_class = 'TIMEOUT'`

**Causas possíveis:**
- Orchestrator crashando frequentemente
- Timeout de execução muito baixo (< 2 horas)
- Problemas de conectividade com Oracle

**Soluções:**
1. Aumentar timeout se necessário (linha 1166 do orchestrator)
2. Investigar logs de crashes do Databricks cluster
3. Verificar conectividade Oracle: `diagnostics/test_oracle_connection.py`

---

## 📚 Referências

- [Plano Completo](../plans/0e3b01a7-e180-499b-85c2-65ef45720580.md)
- [Orchestrator Source](../databricks_notebooks/governed_ingestion_orchestrator.py)
- [Portal Routes](../src/portalRoutes.js)
- [Dashboard UI](../public/v2.html)

---

## 🚧 Próximos Passos (Roadmap)

### P2 - Alta (Próximos Dias)
1. **Trigger Automático via API** 🔜
   - Backend dispara job automaticamente ao enfileirar
   - UI mostra link direto para execução no Databricks
   - Fallback gracioso se API falhar

2. **Feedback em Tempo Real** 🔜
   - WebSocket para status updates
   - Notificação quando job completa

### P3 - Média (Próximas Semanas)
3. **Validação de Integridade**
   - Script para detectar datasets órfãos
   - Alertas para inconsistências

4. **Métricas Avançadas**
   - Queue age (tempo médio de espera)
   - Targeted vs Batch execution rate
   - Orphan detection rate

---

**Última atualização:** 2026-02-20  
**Versão:** 1.0.0  
**Autor:** Equipe Data Engineering
