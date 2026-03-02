# docs/01-architecture/data-model.md
## Modelo de Dados — Plataforma de Ingestão Governada

---

## 1) Objetivo
Descrever o **modelo de dados lógico e físico** da Plataforma de Ingestão Governada, detalhando:
- entidades principais;
- relacionamentos;
- responsabilidades por schema;
- como o modelo suporta governança, execução e auditoria.

Este documento complementa a visão arquitetural e serve como referência para desenvolvimento, troubleshooting e evolução.

---

## 2) Organização por Schema

### 2.1 `ingestion_sys.ctrl` (Controle / Governo)
Responsável por **setup, estado e decisões** do produto.

Entidades:
- projetos, áreas
- conexões aprovadas
- naming conventions
- lifecycle de datasets
- versionamento e aprovação de schema
- RBAC defaults

---

### 2.2 `ingestion_sys.ops` (Operacional / Runtime)
Responsável por **execução, métricas e rastreabilidade**.

Entidades:
- fila de execução
- execuções por dataset
- métricas por camada/tabela
- watermarks incrementais

---

## 3) Entidades — Controle (`ctrl`)

### 3.1 projects
**Responsabilidade:** delimitar domínio organizacional.

Campos-chave:
- `project_id` (PK)
- `is_active`

Relacionamentos:
- 1:N com `areas`
- 1:N com `dataset_control`

---

### 3.2 areas
**Responsabilidade:** agrupar datasets por área funcional.

Campos-chave:
- `area_id` (PK)
- `project_id` (FK lógico)

Relacionamentos:
- N:1 com `projects`
- 1:N com `dataset_control`

---

### 3.3 connections_oracle / connections_sharepoint
**Responsabilidade:** armazenar **conexões aprovadas**.

Campos-chave:
- `connection_id` (PK)
- `approval_status`

Relacionamentos:
- referenciadas por `dataset_control`

---

### 3.4 naming_conventions
**Responsabilidade:** padronizar nomes físicos de tabelas.

Campos-chave:
- `naming_version` (PK)
- `is_active`

Regras:
- apenas 1 versão ativa
- resolução ocorre no publish

---

### 3.5 rbac_defaults
**Responsabilidade:** fonte da verdade de permissões UC.

Campos-chave:
- `rbac_id` (PK)
- `project_id`
- `area_id`
- `layer`
- `privilege`

---

### 3.6 dataset_control
**Responsabilidade:** **núcleo do produto** — identidade e estado do dataset.

Campos-chave:
- `dataset_id` (PK)
- `execution_state`
- `bronze_table`
- `silver_table`
- `current_schema_ver`

Relacionamentos:
- N:1 com `projects`
- N:1 com `areas`
- 1:N com `schema_versions`
- 1:N com `batch_process`

---

### 3.7 dataset_state_changes
**Responsabilidade:** auditoria de lifecycle.

Campos-chave:
- `change_id` (PK)
- `dataset_id`
- `old_state` / `new_state`

---

### 3.8 schema_versions
**Responsabilidade:** versionamento do contrato Silver.

Campos-chave:
- (`dataset_id`, `schema_version`) (PK)
- `status`
- `schema_fingerprint`

Relacionamentos:
- 1:N com `schema_approvals`

---

### 3.9 schema_approvals
**Responsabilidade:** decisão explícita de mudanças estruturais.

Campos-chave:
- `approval_id` (PK)
- `dataset_id`
- `schema_version`
- `decision`

---

## 4) Entidades — Operacional (`ops`)

### 4.1 run_queue
**Responsabilidade:** orquestrar execuções governadas.

Campos-chave:
- `queue_id` (PK)
- `dataset_id`
- `status`
- `attempt`
- `next_retry_at`

Relacionamentos:
- 1:1 (opcional) com `batch_process`

---

### 4.2 batch_process
**Responsabilidade:** representar **1 execução** de 1 dataset.

Campos-chave:
- `run_id` (PK)
- `dataset_id`
- `status`
- `started_at` / `finished_at`

Relacionamentos:
- 1:N com `batch_process_table_details`
- N:1 com `dataset_control`

---

### 4.3 batch_process_table_details
**Responsabilidade:** métricas e status por camada/tabela.

Campos-chave:
- `detail_id` (PK)
- `run_id`
- `layer`
- `status`

---

### 4.4 dataset_watermark
**Responsabilidade:** controle incremental por dataset.

Campos-chave:
- `dataset_id` (PK)
- `watermark_column`
- `watermark_value`

---

## 5) Relacionamentos (visão lógica)

```
projects ──┐
           ├─ areas ──┐
           │          ├─ dataset_control ──┐
           │          │                    ├─ schema_versions ── schema_approvals
           │          │                    ├─ dataset_state_changes
           │          │                    └─ batch_process ── batch_process_table_details
           │          │
           │          └─ run_queue
```

---

## 6) Regras de Integridade (lógicas)
- Dataset sempre pertence a 1 projeto e 1 área
- Dataset bloqueado **não escreve Silver**
- Apenas 1 schema ACTIVE por dataset
- Watermark só atualiza em sucesso
- Execução sempre gera `batch_process`

---

## 7) Observações de Design
- FKs são **lógicas**, não físicas (Delta)
- Auditoria é **append-only**
- Modelo prioriza:
  - rastreabilidade
  - clareza operacional
  - evolução sem refactor

---

## 8) Referências
- `sql/ddl/ingestion_sys.ctrl.sql`
- `sql/ddl/ingestion_sys.ops.sql`
- ADR-0001 → ADR-0005
- Specs Fase 01 → Fase 05

---

## 9) Próximo Documento
👉 `docs/04-runbooks/operations.md`
