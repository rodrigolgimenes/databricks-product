# docs/02-specs/phase-01-orchestrator.md
## Fase 01 — Orchestrator (Job Único Databricks)

---

## 1) Objetivo da fase
Implementar **um único workflow/job no Databricks** capaz de executar **N datasets** de forma governada, resiliente e paralela, isolando falhas por dataset e registrando estado operacional completo no schema `ingestion_sys.ops`.

---

## 2) Escopo
### Inclui
- 1 Databricks Workflow (`governed_ingestion_orchestrator`)
- Driver (notebook ou wheel) com paralelismo controlado
- Consumo da `run_queue`
- Criação e atualização de `batch_process` e `batch_process_table_details`
- Execução Bronze obrigatória
- Promoção Silver condicionada a estado e contrato
- Política de retries por dataset
- Logging estruturado por dataset/run

### Não inclui
- UI (Admin Console ou User Portal)
- Aprovação de schema (entra na Fase 03)
- RBAC UC detalhado (entra na Fase 04)

---

## 3) Artefatos gerados
- **Workflow Databricks**
  - Nome: `governed_ingestion_orchestrator`
  - Tipo: Job único
- **Código**
  - `OrchestratorDriver`
  - `DatasetRunner`
  - `BronzeWriter`
  - `SilverPromoter`
- **Tabelas utilizadas**
  - `ingestion_sys.ctrl.dataset_control`
  - `ingestion_sys.ops.run_queue`
  - `ingestion_sys.ops.batch_process`
  - `ingestion_sys.ops.batch_process_table_details`
  - `ingestion_sys.ops.dataset_watermark`

---

## 4) Implementação (fluxo e pseudocódigo)

### 4.1 Fluxo macro
1. Orchestrator inicia
2. Seleciona datasets elegíveis:
   - `execution_state = 'ACTIVE'`
   - OU entradas pendentes em `run_queue`
3. Aplica limite de paralelismo
4. Para cada dataset:
   - Cria `batch_process`
   - Executa Bronze
   - Decide promoção Silver
   - Atualiza status final
5. Finaliza sem falhar globalmente se um dataset falhar

---

### 4.2 Seleção de datasets
```sql
SELECT *
FROM ingestion_sys.ctrl.dataset_control
WHERE execution_state = 'ACTIVE'
```

OU

```sql
SELECT *
FROM ingestion_sys.ops.run_queue
WHERE status = 'PENDING'
  AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp())
ORDER BY priority, requested_at
```

---

### 4.3 Driver (pseudocódigo)
```python
datasets = select_eligible_datasets()
pool = ThreadPoolExecutor(max_workers=MAX_PARALLELISM)

for dataset in datasets:
    pool.submit(run_dataset, dataset)

pool.shutdown(wait=True)
```

---

### 4.4 Execução por dataset
```python
def run_dataset(dataset):
    run_id = create_batch_process(dataset)

    try:
        write_bronze(dataset, run_id)

        if can_promote_silver(dataset):
            promote_silver(dataset, run_id)
        else:
            mark_skipped_silver(run_id)

        mark_success(run_id)

    except SchemaError as e:
        mark_failed(run_id, "SCHEMA_ERROR", e)
        block_dataset(dataset)

    except Exception as e:
        mark_failed(run_id, "RUNTIME_ERROR", e)
        schedule_retry(dataset)
```

---

### 4.5 Regras críticas
- Bronze **sempre executa**
- Silver só executa se:
  - `execution_state = ACTIVE`
  - schema não bloqueado
- Falha em Silver **não apaga Bronze**
- Retry é **por dataset**, nunca global

---

## 5) Critérios de aceite (checklist técnico)
- [ ] Job único executa N datasets
- [ ] Paralelismo respeita `MAX_PARALLELISM`
- [ ] Falha de 1 dataset não interrompe outros
- [ ] `batch_process` criado para cada dataset
- [ ] `batch_process_table_details` preenchido
- [ ] Retry ocorre respeitando `max_retries`
- [ ] Logs possuem `dataset_id`, `run_id`, `layer`
- [ ] Bronze sempre escreve dados

---

## 6) Riscos e guardrails
### Riscos
- Paralelismo excessivo sobrecarregar cluster
- Dataset com erro de schema causar loop infinito

### Guardrails
- Limite explícito de workers
- Backoff via `next_retry_at`
- Bloqueio automático em erro de schema (Fase 03)

---

## 7) Próxima fase — pré-requisitos
Para iniciar a **Fase 02**:
- Orchestrator funcional
- `run_queue` operacional
- Bronze e Silver com hooks de contrato

Próximo documento:  
👉 `docs/02-specs/phase-02-silver-contract-and-run-queue.md`
