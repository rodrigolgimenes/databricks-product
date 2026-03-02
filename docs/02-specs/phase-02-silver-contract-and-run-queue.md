# docs/02-specs/phase-02-silver-contract-and-run-queue.md
## Fase 02 — Silver Contract + Run Queue (Execução Governada)

---

## 1) Objetivo da fase
Implementar a **execução governada da Silver** baseada em **ExpectSchemaJSON** e **run_queue**, garantindo:
- escrita determinística (MERGE + LWW),
- retries por dataset,
- isolamento de falhas,
- métricas operacionais completas,
- integração direta com o Orchestrator (Fase 01).

Esta fase **materializa** os ADRs 0002, 0003 e 0004 em código executável.

---

## 2) Escopo
### Inclui
- Leitura e validação do **ExpectSchemaJSON ativo**
- Geração de **cast plan determinístico**
- Validação de schema (colunas, tipos, nulabilidade)
- Dedupe **Last Write Wins**
- Escrita Silver via **MERGE**
- Integração completa com `run_queue`
- Registro de métricas e erros acionáveis

### Não inclui
- Aprovação de schema (Fase 03)
- RBAC UC (Fase 04)
- UI/Admin Portal (Fases 04 e 05)

---

## 3) Artefatos gerados
- **Código**
  - `ExpectSchemaLoader`
  - `SchemaValidator`
  - `CastPlanBuilder`
  - `SilverWriter`
  - `RunQueueService`
- **Tabelas utilizadas**
  - `ingestion_sys.ctrl.schema_versions`
  - `ingestion_sys.ops.run_queue`
  - `ingestion_sys.ops.batch_process`
  - `ingestion_sys.ops.batch_process_table_details`
  - `ingestion_sys.ops.dataset_watermark`
- **Contrato**
  - `/contracts/expectschemajson.schema.json`

---

## 4) Implementação (fluxos e pseudocódigo)

### 4.1 Seleção do item da fila
```python
item = run_queue.claim_next(
    max_items=1,
    owner=orchestrator_run_id
)
```

Pré-condições:
- `status = PENDING`
- `next_retry_at <= now()`

---

### 4.2 Carregamento do contrato
```python
schema = load_active_schema(dataset_id)

if not schema:
    raise SchemaError("NO_ACTIVE_SCHEMA")
```

---

### 4.3 Validação estrutural
```python
validation = validate_schema(bronze_df, schema)

if not validation.ok:
    raise SchemaError(validation.error_code)
```

Erros possíveis:
- `MISSING_COLUMN`
- `UNEXPECTED_COLUMN`
- `TYPE_MISMATCH`
- `NULLABILITY_VIOLATION`

---

### 4.4 Cast plan determinístico
```python
casted_df = apply_cast_plan(bronze_df, schema.columns)
```

Regras:
- cast explícito
- falha imediata em erro
- nenhuma inferência automática

---

### 4.5 Deduplicação (LWW)
```python
deduped_df = dedupe_lww(
    df=casted_df,
    keys=schema.primary_key,
    order_col=schema.order_column
)
```

Obrigatório antes de qualquer MERGE.

---

### 4.6 Escrita Silver (MERGE)
```python
merge_to_silver(
    df=deduped_df,
    table=silver_table,
    keys=schema.primary_key
)
```

- `MATCHED` → UPDATE
- `NOT MATCHED` → INSERT

---

### 4.7 Métricas e watermark
```python
update_metrics(
    run_id,
    layer="SILVER",
    row_count=df.count(),
    inserted=inserted,
    updated=updated
)

update_watermark(
    dataset_id,
    schema.watermark
)
```

---

### 4.8 Erros e retry
```python
except SchemaError as e:
    mark_run_failed(run_id, "SCHEMA_ERROR", e)
    block_dataset(dataset_id)

except Exception as e:
    schedule_retry(queue_id)
```

---

## 5) Critérios de aceite (checklist técnico)
- [ ] Silver **não executa** sem contrato ativo
- [ ] Nenhum cast implícito ocorre
- [ ] Dedupe executa antes do MERGE
- [ ] MERGE determinístico (LWW)
- [ ] Métricas gravadas por execução
- [ ] Retry respeita `max_retries`
- [ ] Erros possuem `error_class` e mensagem acionável
- [ ] Watermark atualizado somente em sucesso

---

## 6) Riscos e guardrails
### Riscos
- Contratos mal definidos
- Alto custo em MERGE para grandes volumes

### Guardrails
- Validação estrita
- Índices/partições adequadas
- Monitorar métricas de MERGE

---

## 7) Pré-requisitos para próxima fase
Para iniciar a **Fase 03**:
- Silver governada funcionando
- Erros de schema identificáveis
- Contratos versionáveis

Próximo documento:
👉 `docs/02-specs/phase-03-schema-versioning-and-blocking.md`
