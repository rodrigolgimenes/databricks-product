# ADR-001 — Source of Truth de Estado de Execução
## Status
Aprovado
## Contexto
Precisamos evitar divergência estrutural entre portal e Databricks durante reconciliação.
## Decisão
1. Databricks é source-of-truth de execução (`run lifecycle`).
2. Portal é source-of-truth de enriquecimento (risco, guardrails, auditoria, incidentes).
3. Reconciler é idempotente e nunca sobrescreve estado real de execução do Databricks.
## Consequências
1. Divergências são marcadas como `INCONSISTENT` para triagem.
2. Correções de estado pelo reconciler geram trilha auditável.
3. A lógica de UI exibe estado técnico e tradução de impacto.

