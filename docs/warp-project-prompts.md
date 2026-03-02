# Warp Drive — Project Standard Prompts
## Plataforma de Ingestão Governada

Este arquivo contém os **prompts padrão** a serem salvos no **Warp Drive** para garantir
consistência de código, decisões arquiteturais e uso correto do repositório.

---

## 1) Prompt Mestre do Projeto
**Nome no Warp Drive:** `project-system-prompt`

```
Você está trabalhando na Plataforma de Ingestão Governada.

REGRAS IMUTÁVEIS:
- Existe APENAS UM Databricks Job (orchestrator).
- Execução é sempre governada via run_queue.
- Silver exige ExpectSchemaJSON ativo.
- MERGE determinístico (Last Write Wins).
- Mudança estrutural bloqueia dataset automaticamente.
- RBAC é por Projeto/Área via Unity Catalog.
- Nunca executar notebooks manualmente.

PADRÕES TÉCNICOS:
- Python + PySpark
- Delta Lake
- Código idempotente
- Logs estruturados (dataset_id, run_id, layer)
- Erros explícitos e acionáveis

DOCUMENTAÇÃO:
- Specs em docs/02-specs
- Decisões em docs/03-adrs
- Nunca contradizer ADRs existentes.

Antes de responder:
1) Identifique a fase (01–05)
2) Verifique ADRs aplicáveis
3) Gere código mínimo, claro e governado
```

---

## 2) Prompt para Implementar Fase
**Nome:** `implement-phase`

```
Implemente a fase especificada seguindo EXATAMENTE o arquivo docs/02-specs/phase-XX-*.md anexado.

Regras:
- Não inventar requisitos
- Não alterar decisões arquiteturais
- Produzir código executável (não pseudo quando possível)
- Garantir critérios de aceite

Ao final, valide cada item do checklist técnico.
```

---

## 3) Prompt para Alterar Código Existente
**Nome:** `modify-existing-code`

```
Você está modificando código existente.

Passos obrigatórios:
1) Identifique impacto em run_queue, schema ou lifecycle
2) Verifique se algum ADR é violado
3) Preserve idempotência
4) Atualize métricas e logs se necessário

Nunca:
- Criar novo job Databricks
- Bypassar Silver contract
- Executar lógica fora do orchestrator
```

---

## 4) Prompt para Debug
**Nome:** `debug-issue`

```
Depure o problema usando APENAS:
- batch_process
- batch_process_table_details
- run_queue
- dataset_control
- schema_versions

Forneça:
- diagnóstico
- causa raiz
- ação corretiva governada
- prevenção futura

Nunca sugerir execução manual de notebook.
```

---

## 5) Prompt para Reprocessamento / Backfill
**Nome:** `reprocess-backfill`

```
Descreva como executar reprocessamento ou backfill de forma GOVERNADA.

Regras:
- Sempre via run_queue
- Nunca alterar watermark manualmente
- Respeitar bloqueios de schema
- Garantir determinismo (MERGE + LWW)

Incluir:
- SQL de enqueue
- validações prévias
- riscos
```

---

## 6) Prompt para Revisão de PR
**Nome:** `review-checklist`

```
Revise este PR verificando:

- Aderência aos ADRs 0001–0005
- Uso correto da run_queue
- Silver protegida por contrato
- Logs e métricas completos
- Código idempotente
- Nenhuma lógica crítica no front-end

Retorne:
- pontos OK
- riscos
- sugestões objetivas
```

---

## 7) Como usar no Warp
- Salvar cada seção como um Prompt no Warp Drive
- Usar como contexto fixo para agentes
- Compartilhar entre time e automações

---

## 8) Encerramento
Esses prompts garantem que **humanos e agentes** trabalhem com o mesmo
modelo mental, evitando refactors, desvios arquiteturais e decisões conflitantes.
