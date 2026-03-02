# docs/03-adrs/ADR-0002-run-queue-retries-claim.md

## ADR-0002 — Run Queue, Retries e Claim Simples

**Status:** Accepted  
**Data:** 2025-12-12  
**Decisores:** Plataforma de Dados / Arquitetura  
**Contexto:** Plataforma de Ingestão Governada

---

## 1) Contexto
A plataforma precisa orquestrar execuções de datasets de forma:
- governada e auditável;
- resiliente a falhas temporárias;
- escalável sem explosão de jobs;
- compatível com o **job único** definido no ADR-0001.

É necessário um mecanismo de fila que suporte:
- múltiplos gatilhos (schedule, manual, backfill);
- prioridade;
- retries com backoff;
- isolamento por dataset;
- rastreabilidade ponta a ponta.

---

## 2) Decisão
Adotar uma **Run Queue persistida em Delta** (`ingestion_sys.ops.run_queue`) com:

- **Estados explícitos**: `PENDING → CLAIMED → RUNNING → SUCCEEDED | FAILED | CANCELLED`
- **Claim simples** (MVP):
  - o driver seleciona registros `PENDING`
  - marca como `CLAIMED` com `claim_owner` e `claimed_at`
- **Retry por dataset**, controlado por:
  - `attempt`
  - `max_retries`
  - `next_retry_at`
- **Prioridade numérica** (menor = executa antes)

---

## 3) Justificativa
### Por que run_queue persistida
- Fonte única da verdade de execução
- Permite replay, backfill e auditoria
- Desacopla *scheduler* do *executor*
- Funciona bem com paralelismo interno do driver

### Por que claim simples (e não lock distribuído)
- Delta já fornece atomicidade suficiente para MVP
- Menos complexidade operacional
- Evolutivo para locks otimistas ou service dedicado no futuro

---

## 4) Fluxo operacional

### 4.1 Inserção na fila
- Scheduler (cron/ADF/trigger) insere:
  - `trigger_type = SCHEDULE | MANUAL | BACKFILL`
  - `status = PENDING`
  - `priority` conforme tipo

### 4.2 Claim (driver)
```sql
SELECT *
FROM ingestion_sys.ops.run_queue
WHERE status = 'PENDING'
  AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp())
ORDER BY priority, requested_at
LIMIT N
```

Em seguida:
```sql
UPDATE ingestion_sys.ops.run_queue
SET status = 'CLAIMED',
    claim_owner = '<orchestrator_run_id>',
    claimed_at = current_timestamp()
WHERE queue_id IN (...)
```

---

### 4.3 Execução
- Ao iniciar dataset:
  - status → `RUNNING`
- Ao finalizar:
  - sucesso → `SUCCEEDED`
  - falha → `FAILED` ou re-agendado

---

### 4.4 Retry
- Em falha:
  - incrementa `attempt`
  - se `attempt < max_retries`:
    - calcula `next_retry_at` (backoff exponencial simples)
    - status volta para `PENDING`
  - se excedido:
    - status final = `FAILED`
    - erro persistido

---

## 5) Consequências
### Positivas
- Execução previsível
- Retry controlado e auditável
- Isolamento por dataset
- Base sólida para fairness e quotas futuras

### Negativas / Trade-offs
- Possibilidade teórica de double-claim (mitigada por design)
- Backoff simples no MVP
- Não é um message broker em tempo real

---

## 6) Alternativas consideradas (e rejeitadas)

### A) Databricks Jobs API por dataset
**Rejeitado**:
- Acoplamento forte ao scheduler
- Baixa auditabilidade
- Custo operacional

### B) Message Broker (Kafka, Service Bus)
**Rejeitado no MVP**:
- Overkill para batch governado
- Complexidade de operação
- Latência não necessária

---

## 7) Implicações técnicas
- `run_queue` é **obrigatória** para execução governada
- Scheduler **nunca executa código**, apenas enfileira
- Driver:
  - nunca executa item sem claim
  - nunca reprocessa sem atualizar estado
- `batch_process` referencia `queue_id`

---

## 8) Regras imutáveis (guardrails)
- ❌ Execução sem registro em `run_queue`
- ❌ Retry fora do controle da fila
- ❌ Estado implícito ou inferido
- ✅ Estado explícito e persistido
- ✅ Retry sempre visível e auditável

---

## 9) Referências
- ADR-0001 — Single Workflow Orchestrator
- `sql/ddl/ingestion_sys.ops.sql`
- `docs/02-specs/phase-01-orchestrator.md`
- PRD — Plataforma de Ingestão Governada

---

## 10) Próximos ADRs relacionados
- ADR-0003 — ExpectSchemaJSON como contrato Silver
- ADR-0004 — Last Write Wins + Merge determinístico
