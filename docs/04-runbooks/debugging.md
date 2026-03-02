# docs/04-runbooks/debugging.md
## Runbook de Debug — Plataforma de Ingestão Governada

---

## 1) Objetivo
Este runbook descreve **como diagnosticar e debugar problemas** na Plataforma de Ingestão Governada de forma estruturada, rápida e sem quebrar governança.

Foco:
- identificar causa raiz;
- diferenciar erro de dados vs erro de plataforma;
- evitar ações manuais perigosas;
- reduzir MTTR.

---

## 2) Princípio de Debug
> **Sempre debugar pelo estado persistido, nunca pelo notebook isolado.**

Fontes oficiais:
- `ingestion_sys.ops.batch_process`
- `ingestion_sys.ops.batch_process_table_details`
- `ingestion_sys.ops.run_queue`
- `ingestion_sys.ctrl.dataset_control`
- `ingestion_sys.ctrl.schema_versions`

---

## 3) Checklist Rápido (5 perguntas)
1. Qual o `dataset_id`?
2. Qual o `run_id`?
3. Qual a `error_class`?
4. Em qual camada falhou (Bronze / Silver)?
5. O dataset está bloqueado?

---

## 4) Debug por Tipo de Erro

### 4.1 SCHEMA_ERROR
Sintomas:
- dataset em `BLOCKED_SCHEMA_CHANGE`
- Silver não executa

Query:
```sql
SELECT *
FROM ingestion_sys.ctrl.schema_versions
WHERE status = 'PENDING';
```

Ação:
- revisar diff
- aprovar ou rejeitar schema
- **não forçar execução**

---

### 4.2 SOURCE_ERROR
Sintomas:
- erro intermitente
- retries ativos

Query:
```sql
SELECT attempt, last_error_message
FROM ingestion_sys.ops.run_queue
WHERE dataset_id = '<dataset_id>';
```

Ação:
- validar conectividade
- aguardar retry
- re-enfileirar manualmente se necessário

---

### 4.3 RUNTIME_ERROR
Sintomas:
- erro no código
- stacktrace disponível

Query:
```sql
SELECT error_stacktrace
FROM ingestion_sys.ops.batch_process
WHERE run_id = '<run_id>';
```

Ação:
- corrigir código
- reprocessar via run_queue
- **não executar notebook isolado**

---

## 5) Dataset Não Executa
Verificar:
```sql
SELECT execution_state
FROM ingestion_sys.ctrl.dataset_control
WHERE dataset_id = '<dataset_id>';
```

Estados bloqueantes:
- `PAUSED`
- `DEPRECATED`
- `BLOCKED_SCHEMA_CHANGE`

---

## 6) Execução Lenta
Indicadores:
- tempo alto entre `started_at` e `finished_at`
- crescimento do run_queue

Ações:
- reduzir `MAX_PARALLELISM`
- revisar MERGE
- avaliar particionamento Silver

---

## 7) Debug de Silver
Verificar:
- contrato ativo
- dedupe correto
- contagem de linhas

```sql
SELECT *
FROM ingestion_sys.ctrl.schema_versions
WHERE dataset_id = '<dataset_id>' AND status = 'ACTIVE';
```

---

## 8) O que NÃO fazer
- ❌ executar Silver manualmente
- ❌ editar schema no código
- ❌ apagar registros de ops
- ❌ desbloquear dataset manualmente

---

## 9) RCA (Root Cause Analysis)
Template mínimo:
- Dataset:
- Run ID:
- Erro:
- Causa raiz:
- Correção:
- Prevenção:

---

## 10) Referências
- operations.md
- ADR-0001 → ADR-0005
- Specs Fase 01 → Fase 05
