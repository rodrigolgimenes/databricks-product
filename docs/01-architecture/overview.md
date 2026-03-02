# docs/01-architecture/overview.md
## Visão Geral da Arquitetura — Plataforma de Ingestão Governada

---

## 1) Objetivo
Descrever a **arquitetura de alto nível** da Plataforma de Ingestão Governada, explicando:
- componentes principais;
- responsabilidades de cada camada;
- fluxos de dados e controle;
- decisões arquiteturais já consolidadas (via ADRs).

Este documento é **contextual** e serve como porta de entrada técnica para novos engenheiros, arquitetos e stakeholders.

---

## 2) Princípios Arquiteturais
- **Job único** no Databricks para orquestração (ADR-0001)
- **Execução governada e auditável** (run_queue)
- **Contrato explícito de schema** (ExpectSchemaJSON)
- **Determinismo por padrão** (LWW + MERGE)
- **Falha isolada por dataset**
- **Governança antes da UI**
- **RBAC centralizado (Unity Catalog)**

---

## 3) Componentes Principais

### 3.1 Orchestrator (Databricks)
- Um único Workflow/Job
- Driver controla paralelismo interno
- Executa datasets de forma isolada
- Atualiza estado operacional

Interage com:
- `ingestion_sys.ctrl`
- `ingestion_sys.ops`
- Bronze / Silver tables

---

### 3.2 Run Queue
- Fila persistida em Delta
- Controla:
  - prioridade
  - retries
  - backfills
- Desacopla *quem agenda* de *quem executa*

---

### 3.3 Camadas de Dados
- **Bronze**
  - fidelidade máxima
  - sem contrato
  - ingestão contínua
- **Silver**
  - contrato explícito
  - versionamento
  - bloqueio automático em drift

---

### 3.4 Schema Governance
- ExpectSchemaJSON
- Versionamento formal
- Aprovação explícita
- Histórico completo

---

### 3.5 Admin Console (sem UI no MVP)
- Setup do produto
- Projetos, Áreas e Conexões
- Naming conventions versionadas
- Aplicação de RBAC no UC

---

### 3.6 User Portal
- Publicação guiada
- Visibilidade de estados
- Erros acionáveis
- Preview confiável da Silver

---

## 4) Fluxo de Dados (alto nível)

```
Fonte → Bronze → (Validação/Contrato) → Silver → Consumo
               ↑
            Run Queue
```

- Bronze nunca bloqueia
- Silver só executa com contrato válido
- Drift gera bloqueio automático

---

## 5) Fluxo de Controle (execução)

1. Scheduler insere item na `run_queue`
2. Orchestrator faz claim
3. Dataset executa isoladamente
4. Bronze escreve
5. Silver valida contrato
6. Resultado:
   - sucesso
   - retry
   - bloqueio por schema

---

## 6) Governança e Observabilidade
- Todas as execuções geram `batch_process`
- Detalhe por camada em `batch_process_table_details`
- Watermarks controlam incremental
- Estados explícitos no `dataset_control`

---

## 7) Segurança e Acesso
- Unity Catalog como fonte única de RBAC
- Permissões por Projeto / Área
- Silver mais restrita que Bronze
- Nenhuma permissão por dataset

---

## 8) Evolução e Escala
Arquitetura preparada para:
- centenas de datasets
- múltiplas áreas/projetos
- novos tipos de fonte
- UI rica sem refactor de backend

---

## 9) Referências
- `docs/00-index.md`
- ADR-0001 → ADR-0005
- Specs Fase 01 → Fase 05
- PRD — Plataforma de Ingestão Governada

---

## 10) Próximo Documento
👉 `docs/01-architecture/data-model.md`
