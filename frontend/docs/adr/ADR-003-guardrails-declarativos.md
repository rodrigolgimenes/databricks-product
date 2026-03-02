# ADR-003 — Guardrails Declarativos por Job/Domínio
## Status
Aprovado
## Contexto
Guardrails hardcoded não escalam e dificultam governança por criticidade.
## Decisão
1. Configuração declarativa em `job_guardrail_config`.
2. Campos mínimos: `expected_frequency`, `volume_min`, `volume_max`, `watermark_required`, `dq_required`, `silent_failure_check`.
3. Motor de pós-validação gera `SUCCEEDED_WITH_ISSUES` quando aplicável.
## Consequências
1. Regras podem variar por domínio sem deploy de código.
2. Controle de rollout gradual por criticidade.
3. Evidência auditável do motivo de reclassificação.

