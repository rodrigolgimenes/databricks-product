# docs/02-specs/phase-05-user-portal-errors.md
## Fase 05 — User Portal, Erros Acionáveis e Preview da Silver

---

## 1) Objetivo da fase
Implementar a **camada de experiência do usuário** da Plataforma de Ingestão Governada, permitindo que usuários de dados:
- publiquem datasets de forma guiada;
- acompanhem execuções e estados;
- entendam erros de forma **acionável** (não técnica);
- visualizem preview confiável da Silver;
- participem do fluxo de aprovação de schema.

Esta fase **não altera decisões técnicas**, apenas as **expõe corretamente**.

---

## 2) Escopo
### Inclui
- APIs/serviços do User Portal
- Wizard de publicação de dataset
- Página de detalhe do dataset
- Exibição de erros em 3 camadas
- Preview da Silver (somente dados válidos)
- Aprovação / rejeição de schema
- Integração total com estados do lifecycle

### Não inclui
- Execução de pipelines (orchestrator é responsável)
- Gestão de RBAC (Admin Console)
- Alterações diretas em Bronze/Silver

---

## 3) Personas
- **Data Producer**: publica datasets, corrige erros, acompanha execuções
- **Data Owner**: aprova schema, decide mudanças estruturais
- **Admin**: governa projetos, áreas e permissões (fora do portal)

---

## 4) Funcionalidades principais

### 4.1 Wizard de publicação
Fluxo:
1. Seleção de Projeto / Área
2. Definição de origem (Oracle / SharePoint)
3. Configuração técnica mínima
4. Preview Bronze
5. Geração do dataset em `DRAFT`
6. Ação **Publicar**
   - estado → `ACTIVE`
   - enqueue inicial (`run_queue`)
   - primeira execução automática

Regras:
- Naming é resolvido no publish
- Nenhuma execução ocorre em `DRAFT`

---

### 4.2 Página do Dataset
Exibir:
- Identidade (nome, projeto, área, owner)
- Estado atual (`ACTIVE`, `PAUSED`, etc.)
- Últimas execuções
- Último erro (se houver)
- Versão ativa de schema
- Histórico de mudanças de estado

---

## 5) Modelo de erros acionáveis (3 camadas)

### 5.1 Camada 1 — Mensagem humana
Exemplo:
> “A coluna **order_date** chegou como texto, mas o contrato exige **timestamp**.”

---

### 5.2 Camada 2 — Causa técnica
- `TYPE_MISMATCH`
- coluna: `order_date`
- esperado: `timestamp`
- recebido: `string`

---

### 5.3 Camada 3 — Detalhe técnico (debug)
- stacktrace
- código interno
- referência ao `run_id`

Somente a Camada 1 é exibida por padrão.

---

## 6) Preview da Silver
Regras:
- Apenas se último run **SUCCEEDED**
- Nunca fallback para Bronze
- Limite: Top 10 linhas
- Indicar versão de schema usada

Exemplo:
```sql
SELECT *
FROM silver_table
ORDER BY _ingestion_ts DESC
LIMIT 10
```

---

## 7) Aprovação de schema
### 7.1 Exibição
- Diferença entre versão ativa e pendente
- Tipo de mudança (adição, remoção, tipo)
- Impacto estimado

---

### 7.2 Ações
- **Aprovar**
  - versão → `ACTIVE`
  - dataset → `ACTIVE`
- **Rejeitar**
  - versão → `REJECTED`
  - dataset permanece bloqueado

---

## 8) Integração com estados
O Portal **nunca infere estado**.
Tudo vem de:
- `dataset_control.execution_state`
- `schema_versions.status`
- `batch_process.status`

Estados bloqueantes:
- `BLOCKED_SCHEMA_CHANGE`
- `PAUSED`
- `DEPRECATED`

---

## 9) Critérios de aceite (checklist técnico)
- [ ] Wizard cria dataset em `DRAFT`
- [ ] Publish gera enqueue inicial
- [ ] Estados refletem backend fielmente
- [ ] Erros exibidos em 3 camadas
- [ ] Preview só aparece em sucesso
- [ ] Aprovação altera estado corretamente
- [ ] Nenhuma lógica crítica duplicada no front

---

## 10) Riscos e guardrails
### Riscos
- Usuário ignorar erro técnico
- Excesso de detalhe confundir usuário

### Guardrails
- Linguagem simples por padrão
- Detalhe técnico sob demanda
- Estados sempre explícitos

---

## 11) Encerramento
Esta fase completa a **plataforma end-to-end**, conectando:
- governança técnica
- execução confiável
- experiência clara para usuários

Com isso, o produto está pronto para:
- MVP produtivo
- escala controlada
- evolução incremental
