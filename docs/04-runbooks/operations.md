# docs/04-runbooks/operations.md
## Runbook Operacional — Plataforma de Ingestão Governada

---

## 1) Objetivo
Este runbook descreve **como operar, monitorar, reprocessar e depurar** a Plataforma de Ingestão Governada em ambientes produtivos, cobrindo:
- operação diária do Orchestrator;
- monitoramento de execuções e SLAs;
- tratamento de falhas;
- reprocessamentos e backfills;
- procedimentos seguros (sem quebrar governança).

---

## 2) Visão Geral Operacional
Componentes operados:
- **Databricks Orchestrator (job único)**
- **Run Queue** (`ingestion_sys.ops.run_queue`)
- **Execuções** (`batch_process`)
- **Governança de Schema** (`schema_versions`, `schema_approvals`)
- **Lifecycle de Dataset** (`dataset_control`)

Princípios:
- nunca executar dataset “fora do sistema”;
- nunca escrever Silver manualmente;
- toda ação deve deixar rastro em tabelas de controle.

---

## 3) Operação Diária (Checklist)

### 3.1 Verificar saúde geral
```sql
SELECT status, COUNT(*) 
FROM ingestion_sys.ops.run_queue
GROUP BY status;
```

```sql
SELECT status, COUNT(*) 
FROM ingestion_sys.ops.batch_process
WHERE started_at >= current_date()
GROUP BY status;
```

---

### 3.2 Verificar filas represadas
```sql
SELECT *
FROM ingestion_sys.ops.run_queue
WHERE status = 'PENDING'
  AND requested_at < current_timestamp() - INTERVAL 1 HOUR;
```

Ações possíveis:
- verificar cluster capacity;
- reduzir `MAX_PARALLELISM`;
- escalar cluster;
- investigar erro recorrente.

---

## 4) Monitoramento de Execuções

### 4.1 Últimas execuções por dataset
```sql
SELECT dataset_id, status, started_at, finished_at
FROM ingestion_sys.ops.batch_process
ORDER BY started_at DESC
LIMIT 50;
```

---

### 4.2 Métricas por camada
```sql
SELECT layer, SUM(row_count) rows
FROM ingestion_sys.ops.batch_process_table_details
WHERE started_at >= current_date()
GROUP BY layer;
```

---

## 5) Tratamento de Falhas

### 5.1 Falha de Schema (SCHEMA_ERROR)
Sintoma:
- dataset em `BLOCKED_SCHEMA_CHANGE`
- erro `SCHEMA_CHANGE_DETECTED`

Ação:
1. Inspecionar `schema_versions`
2. Avaliar diff
3. Aprovar ou rejeitar

```sql
SELECT *
FROM ingestion_sys.ctrl.schema_versions
WHERE status = 'PENDING';
```

---

### 5.2 Falha Temporária (SOURCE_ERROR / RUNTIME_ERROR)
Sintoma:
- retries em andamento
- `attempt < max_retries`

Ação:
- aguardar retry automático
- ou intervir manualmente (ver seção 6)

---

## 6) Reprocessamento e Backfill

### 6.1 Reprocessar último run (manual)
```sql
INSERT INTO ingestion_sys.ops.run_queue
(queue_id, dataset_id, trigger_type, requested_by, requested_at, status)
VALUES
(uuid(), '<dataset_id>', 'MANUAL', 'ops', current_timestamp(), 'PENDING');
```

---

### 6.2 Backfill histórico
```sql
INSERT INTO ingestion_sys.ops.run_queue
(queue_id, dataset_id, trigger_type, requested_by, requested_at, status, priority)
VALUES
(uuid(), '<dataset_id>', 'BACKFILL', 'ops', current_timestamp(), 'PENDING', 10);
```

Observações:
- backfill respeita contrato ativo;
- LWW garante determinismo;
- watermarks podem ser ajustados **somente via Admin**.

---

## 7) Operações Administrativas Seguras

### 7.1 Pausar dataset
```sql
UPDATE ingestion_sys.ctrl.dataset_control
SET execution_state = 'PAUSED'
WHERE dataset_id = '<dataset_id>';
```

---

### 7.2 Retomar dataset
```sql
UPDATE ingestion_sys.ctrl.dataset_control
SET execution_state = 'ACTIVE'
WHERE dataset_id = '<dataset_id>';
```

---

### 7.3 Descontinuar dataset
```sql
UPDATE ingestion_sys.ctrl.dataset_control
SET execution_state = 'DEPRECATED'
WHERE dataset_id = '<dataset_id>';
```

---

## 8) O que NÃO fazer (alerta)
- ❌ executar notebooks de ingestão manualmente
- ❌ escrever diretamente em Silver
- ❌ alterar schema sem aprovação
- ❌ apagar registros de controle/ops
- ❌ criar jobs paralelos fora do orchestrator

---

## 9) Incidentes e RCA
Em incidentes críticos:
1. Identificar `run_id`
2. Mapear dataset afetado
3. Identificar classe de erro
4. Registrar RCA:
   - causa raiz
   - impacto
   - ação corretiva
   - prevenção

Fontes:
- `batch_process`
- `batch_process_table_details`
- logs do Databricks

---

## 10) Escala e Capacidade
Sinais de pressão:
- run_queue crescendo continuamente
- aumento de retries
- longos tempos de execução

Ações:
- escalar cluster
- revisar contratos pesados
- dividir cargas por janela
- revisar paralelismo

---

## 11) Referências
- `docs/00-index.md`
- Specs Fase 01 → Fase 05
- ADR-0001 → ADR-0005
- PRD — Plataforma de Ingestão Governada

---

## 12) Encerramento
Este runbook garante que a plataforma possa ser **operada com segurança**, mesmo sob falhas,
sem quebrar governança ou gerar inconsistências de dados.
