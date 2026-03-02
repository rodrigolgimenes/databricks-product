# docs/03-adrs/ADR-0001-single-workflow-orchestrator.md

## ADR-0001 — Single Workflow Orchestrator (Job Único no Databricks)

**Status:** Accepted  
**Data:** 2025-12-12  
**Decisores:** Plataforma de Dados / Arquitetura  
**Contexto:** Plataforma de Ingestão Governada

---

## 1) Contexto
A plataforma precisa executar **centenas de datasets** com:
- governança de execução;
- isolamento de falhas por dataset;
- controle de paralelismo;
- observabilidade unificada;
- baixo custo operacional e simplicidade de operação.

Alternativas como **1 job por dataset** ou **1 job por área/projeto** aumentam:
- custo de agendamento;
- complexidade de deploy;
- dificuldade de troubleshooting;
- risco de inconsistência de padrões.

---

## 2) Decisão
Adotar **UM ÚNICO workflow/job no Databricks** (`governed_ingestion_orchestrator`) responsável por:
- selecionar datasets elegíveis;
- consumir a `run_queue`;
- executar datasets em **paralelo controlado**;
- isolar falhas por dataset;
- registrar estado completo de execução.

O paralelismo é **interno ao driver**, não ao workflow.

---

## 3) Justificativa
### Por que job único
- **Escala linear**: N datasets sem criar N jobs.
- **Governança centralizada**: políticas de retry, backoff e bloqueio em um ponto.
- **Observabilidade consistente**: métricas e logs padronizados.
- **Custo menor**: menos jobs, menos overhead de scheduler.
- **Menos refactor futuro**: padrão estável para MVP e escala.

---

## 4) Consequências
### Positivas
- Execução previsível e padronizada
- Troubleshooting simplificado
- Evolução incremental (ex.: prioridades, quotas, fairness)

### Negativas / Trade-offs
- Driver mais complexo
- Exige disciplina de paralelismo e timeout
- Falha no driver impacta todos os datasets daquele ciclo

---

## 5) Alternativas consideradas (e rejeitadas)
### A) 1 Job por Dataset
**Rejeitado**:
- Explosão de jobs
- Deploy e versionamento complexos
- Custo e ruído operacional

### B) 1 Job por Projeto / Área
**Rejeitado**:
- Fragmenta governança
- Dificulta visibilidade global
- Padrões divergentes ao longo do tempo

---

## 6) Implicações técnicas
- O workflow Databricks:
  - **não** paraleliza tasks do job
  - executa **1 driver** com pool de workers
- O driver:
  - controla `MAX_PARALLELISM`
  - cria registros em `batch_process`
  - nunca lança exceção global por falha de dataset
- Retry é **por dataset**, via `run_queue`

---

## 7) Regras imutáveis (guardrails)
- ❌ Não criar jobs por dataset
- ❌ Não paralelizar tasks no workflow
- ✅ Paralelismo apenas no driver
- ✅ Falha isolada por dataset
- ✅ Job deve sempre terminar com sucesso global

---

## 8) Referências
- `docs/02-specs/phase-01-orchestrator.md`
- `sql/ddl/ingestion_sys.ops.sql`
- Plano Técnico — Job Único Databricks
- PRD — Plataforma de Ingestão Governada

---

## 9) Próximos ADRs relacionados
- ADR-0002 — Run Queue, Retries e Claim
- ADR-0003 — ExpectSchemaJSON como contrato Silver
