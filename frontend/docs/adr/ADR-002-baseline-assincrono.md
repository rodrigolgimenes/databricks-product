# ADR-002 — Baseline Persistido e Assíncrono
## Status
Aprovado
## Contexto
Recalcular baseline em leitura causa instabilidade e falso positivo de near-miss.
## Decisão
1. Persistir baseline em `job_baseline_metrics`.
2. Atualização assíncrona por job agendado (janela móvel de 30–50 runs válidos).
3. UI e APIs de leitura consomem baseline persistido.
## Consequências
1. Comparações p50/p95/p99 ficam determinísticas.
2. Redução de ruído operacional em alertas.
3. Dependência explícita de job de recomputação no pipeline operacional.

