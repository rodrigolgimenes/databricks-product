# docs/00-index.md — Mapa do repositório + como usar no Warp

Este documento é o **“norte”** (curto) da Plataforma de Ingestão Governada e serve para:
- orientar navegação no repositório;
- padronizar como anexar contexto no Warp (trabalho por fase/tarefa);
- definir **a ordem correta de execução do desenvolvimento**, incluindo **tabelas de setup** do produto.

> Fonte: PRD “Plataforma de Ingestão Governada” e Plano Técnico “Job Único Databricks” (arquivos de referência do projeto).

---

## 1) Como navegar (princípio de modularidade)

**1 arquivo master** (este) + **N arquivos por fase** + **ADRs separados**.

Regra prática para o Warp:
- Para implementar uma fase: anexe **somente** `docs/02-specs/phase-XX-*.md` daquela fase.
- Para aplicar uma decisão: anexe **somente** o `docs/03-adrs/ADR-000X-*.md` correspondente.
- Para gerar/alterar DDL: anexe **somente** `sql/ddl/*.sql` (e o spec da fase que descreve o DDL).

---

## 2) Estrutura de pastas recomendada

```
/README.md
/docs/
  /00-index.md
  /01-architecture/
    overview.md
    data-model.md
    rbac-unity-catalog.md
  /02-specs/
    phase-01-orchestrator.md
    phase-02-silver-contract-and-run-queue.md
    phase-03-schema-versioning-and-blocking.md
    phase-04-admin-console-lifecycle-rbac.md
    phase-05-user-portal-errors.md
  /03-adrs/
    ADR-0001-single-workflow-orchestrator.md
    ADR-0002-run-queue-retries-claim.md
    ADR-0003-silver-contract-expectschemajson.md
    ADR-0004-last-write-wins-merge.md
    ADR-0005-schema-change-blocking.md
  /04-runbooks/
    operations.md
    debugging.md
    backfills-and-reprocess.md
/contracts/
  expectschemajson.schema.json
/sql/
  /ddl/
    ingestion_sys.ctrl.sql
    ingestion_sys.ops.sql
  /migrations/
/src/
  ...código...
```

---

## 3) Glossário rápido (termos do produto)

- **Dataset**: unidade publicada pelo usuário (origem + config + naming resolvido + estado).
- **Bronze**: camada de fidelidade máxima (não “corrige” dados).
- **Silver**: camada técnica com tipagem forte, normalização e contrato determinístico.
- **ExpectSchemaJSON**: contrato executável da Silver (fonte da verdade).
- **Orchestrator**: **1 único** workflow/job no Databricks que executa N datasets com paralelismo controlado.
- **run_queue**: fila governada de execução (scheduler enfileira, orchestrator consome).
- **Schema versioning**: qualquer mudança estrutural em Silver gera versão nova; mudança bloqueia dataset até aprovação.
- **RBAC (Unity Catalog)**: permissões herdadas por Projeto/Área (sem permissão “por dataset”).

---

## 4) Ordem correta de desenvolvimento (sequência oficial)

> Objetivo: evitar refactor caro. Primeiro “fundação” (modelos/tabelas/estados), depois runtime, depois governança/UX.

### Fase 0 — Fundação: DDL e “setup do produto” (obrigatório antes de qualquer código)
**Entregáveis**
1. Criar schemas do sistema:
   - `ingestion_sys.ctrl` (controle/metadados)
   - `ingestion_sys.ops` (operação/runtime)
2. Criar tabelas mínimas de setup (core do produto):
   - **Admin Console / setup**
     - `ctrl.projects`
     - `ctrl.areas`
     - `ctrl.connections_oracle`
     - `ctrl.connections_sharepoint`
     - `ctrl.naming_conventions` (versionada)
     - `ctrl.rbac_defaults` (por projeto/área)
   - **Datasets e lifecycle**
     - `ctrl.dataset_control` (inclui `ExecutionState`, naming resolvido, config de origem, ponteiros de schema)
     - `ctrl.dataset_state_changes` (audit log)
   - **Runtime / execuções**
     - `ops.run_queue`
     - `ops.batch_process`
     - `ops.batch_process_table_details`
     - `ops.dataset_watermark`
   - **Schema (versionamento e aprovação)**
     - `ctrl.schema_versions`
     - `ctrl.schema_approvals`
3. Seeds iniciais (opcional, mas recomendado para DEV):
   - 1 projeto + 1 área
   - 1 naming_convention ativa
   - 1 conexão Oracle e/ou SharePoint “aprovada”
   - RBAC default para leitura/escrita/admin

**Critérios de aceite**
- Todas as tabelas existem no UC como Delta.
- Scripts idempotentes (rodar 2x não quebra).
- Constraints mínimas (chaves/unique onde fizer sentido) e colunas técnicas (`CreatedAt/UpdatedAt/CreatedBy/UpdatedBy`).

---

### Fase 1 — Runtime: Orchestrator (Job único + paralelismo controlado)
**Entregáveis**
- 1 Databricks Workflow: `governed_ingestion_orchestrator`
- 1 driver (notebook ou wheel) que:
  - seleciona datasets elegíveis;
  - consome `run_queue` (SCHEDULE) e executa dataset específico (MANUAL);
  - cria registros em `batch_process` e `batch_process_table_details`;
  - executa Bronze sempre;
  - promove Silver conforme contrato (ou falha com erro acionável);
  - aplica política de retry baseada no `run_queue`;
  - isola falha por dataset (1 dataset não derruba o job todo);
  - respeita `ExecutionState` (ACTIVE/PAUSED/DEPRECATED/BLOCKED_SCHEMA_CHANGE).

**Critérios de aceite**
- N datasets em paralelo com limite `max_parallelism`.
- Dataset #11 só inicia quando um termina.
- Falha de 1 dataset não afeta os demais.
- Logs operacionais completos por dataset.

---

### Fase 2 — Contrato Silver + run_queue (execução governada)
**Entregáveis**
- JSON Schema do `ExpectSchemaJSON` (`/contracts/…`)
- Implementação de:
  - cast plan
  - dedupe (Last Write Wins)
  - MERGE/APPEND
- Implementação de `run_queue`:
  - estados e backoff
  - claim simples (MVP) e marcações de sucesso/falha

---

### Fase 3 — Versionamento de schema + bloqueio automático
**Entregáveis**
- Fingerprint determinístico do contrato
- Diff estrutural (MVP)
- Criação automática de schema_version **PENDING** e schema_approval **PENDING**
- Dataset entra em `BLOCKED_SCHEMA_CHANGE` e Silver não escreve

---

### Fase 4 — Admin Console (sem UI no MVP) + RBAC UC + lifecycle
**Entregáveis**
- Stored procedures / scripts idempotentes para:
  - criar projetos/áreas
  - registrar conexões aprovadas
  - versionar naming conventions
  - aplicar GRANTs (UC) por projeto/área (bronze vs silver)
- Regras de transição de estado + auditoria

---

### Fase 5 — User Portal + erros acionáveis + preview Silver
**Entregáveis**
- API/serviços do portal (ou camada equivalente) para:
  - wizard publish (DRAFT / PUBLICAR=ACTIVE+RUN #1)
  - página do dataset (runs, erro 3 camadas, estado, owner)
  - schema proposal read-only
  - aprovação/rejeição de schema
  - preview Silver Top 10 do último batch com sucesso (sem fallback Bronze)

---

## 5) Como “chamar” cada fase no Warp (copy/paste)

### Implementar uma fase
> “Implemente a Fase 1 seguindo o spec. Foque primeiro na seção de ‘Implementação’ e depois rode os critérios de aceite.”

**Contexto para anexar**
- `docs/02-specs/phase-01-orchestrator.md`
- `docs/03-adrs/ADR-0001-single-workflow-orchestrator.md`
- `sql/ddl/ingestion_sys.ctrl.sql`
- `sql/ddl/ingestion_sys.ops.sql`

### Aplicar uma decisão (ADR)
> “Aplique ADR-0004 no código de promote_silver (MERGE last write wins + dedupe).”

**Contexto**
- `docs/03-adrs/ADR-0004-last-write-wins-merge.md`

### Gerar/alterar DDL
> “Atualize o DDL para incluir as colunas de ponteiro de schema e estados do dataset_control. Não quebre idempotência.”

**Contexto**
- `docs/02-specs/phase-03-schema-versioning-and-blocking.md`
- `sql/ddl/ingestion_sys.ctrl.sql`

---

## 6) Guardrails (regras que evitam refactor)

- **Job único** no Databricks (não criar 1 job por dataset).
- **Paralelismo dentro do driver** (não paralelizar tasks do workflow).
- **Bronze sempre escreve**; Silver é governada por contrato.
- **Silver não pode “quebrar silenciosamente”**: cast failures são erro (SCHEMA_ERROR).
- **Mudança de schema => bloquear** (BLOCKED_SCHEMA_CHANGE), criar versão pendente, exigir aprovação.
- **RBAC por Projeto/Área** (não por dataset). UI não decide permissões.

---

## 7) Próximos documentos a serem gerados (ordem)

1. `docs/01-architecture/overview.md`
2. `docs/01-architecture/data-model.md` (ERD + tabelas + chaves)
3. `sql/ddl/ingestion_sys.ctrl.sql`
4. `sql/ddl/ingestion_sys.ops.sql`
5. `docs/02-specs/phase-01-orchestrator.md`
6. `docs/03-adrs/ADR-0001 … ADR-0005`
7. `docs/02-specs/phase-02 … phase-05`
8. `/contracts/expectschemajson.schema.json`
9. `/docs/04-runbooks/*`

> **Nota:** você pediu “um arquivo por vez”. Este é o **primeiro** (00-index). O próximo mais útil para destravar implementação é **o DDL do sistema** (`sql/ddl/ingestion_sys.ctrl.sql`), porque sem isso o runtime não tem onde gravar estado.

