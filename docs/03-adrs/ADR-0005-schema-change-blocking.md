# docs/03-adrs/ADR-0005-schema-change-blocking.md

## ADR-0005 — Schema Change Blocking (Bloqueio Automático por Mudança Estrutural)

**Status:** Accepted  
**Data:** 2025-12-12  
**Decisores:** Plataforma de Dados / Arquitetura  
**Contexto:** Plataforma de Ingestão Governada

---

## 1) Contexto
Mudanças estruturais em dados (adição, remoção ou alteração de tipo de coluna) são uma das
principais causas de incidentes em pipelines analíticos, pois:
- quebram consumidores downstream silenciosamente;
- passam despercebidas quando há inferência automática;
- geram resultados inconsistentes ao longo do tempo.

A plataforma precisa garantir que **nenhuma mudança estrutural na Silver** aconteça
sem **visibilidade, rastreabilidade e decisão explícita**.

---

## 2) Decisão
Implementar **Schema Change Blocking automático**, onde:

- qualquer divergência estrutural entre:
  - o schema efetivo do Bronze pós-ingestão
  - e o **ExpectSchemaJSON ativo**
- gera:
  - criação automática de uma **nova versão de schema** (`PENDING`);
  - bloqueio imediato do dataset (`execution_state = BLOCKED_SCHEMA_CHANGE`);
  - interrupção da escrita na Silver;
  - registro completo para revisão/aprovação.

A ingestão Bronze **continua executando normalmente**.

---

## 3) O que é considerado mudança de schema
São consideradas **mudanças estruturais**:
- adição de coluna;
- remoção de coluna;
- alteração de tipo;
- mudança de nulabilidade;
- alteração de chave primária.

Não são consideradas mudanças estruturais:
- variação de volume;
- reordenação física de colunas;
- mudança apenas em valores.

---

## 4) Fluxo operacional

### 4.1 Detecção
1. Bronze executa
2. Schema efetivo é extraído
3. Calcula-se um **fingerprint determinístico**
4. Compara com o fingerprint da versão ativa

---

### 4.2 Divergência detectada
- Criar registro em `ingestion_sys.ctrl.schema_versions`:
  - `schema_version = last_version + 1`
  - `status = PENDING`
- Criar registro em `ingestion_sys.ctrl.schema_approvals`
- Atualizar `dataset_control.execution_state = BLOCKED_SCHEMA_CHANGE`
- Registrar erro `SCHEMA_CHANGE_DETECTED`

---

### 4.3 Comportamento do Orchestrator
- Bronze: ✅ continua
- Silver: ❌ não executa
- Retry: ❌ não aplicável
- Dataset permanece bloqueado até decisão explícita

---

## 5) Aprovação / Rejeição
### Aprovação
- Admin/Owner aprova versão
- Nova versão torna-se `ACTIVE`
- Dataset volta para `ACTIVE`
- Próxima execução promove Silver normalmente

### Rejeição
- Versão marcada como `REJECTED`
- Dataset permanece bloqueado
- Correção deve ocorrer na origem ou contrato

---

## 6) Justificativa
- Elimina schema drift silencioso
- Força decisão consciente
- Protege consumidores
- Mantém histórico completo de decisões
- Compatível com execução contínua de Bronze

---

## 7) Consequências
### Positivas
- Governança forte e explícita
- Menos incidentes downstream
- Auditoria completa
- Base sólida para UI/Admin Portal

### Negativas / Trade-offs
- Pode atrasar disponibilidade da Silver
- Requer processo de aprovação
- Maior rigor operacional

---

## 8) Alternativas consideradas (e rejeitadas)

### A) Aceitar schema novo automaticamente
**Rejeitado**:
- Alto risco
- Quebras silenciosas
- Perda de confiança

### B) Apenas logar divergência
**Rejeitado**:
- Reativo demais
- Não impede impacto downstream

---

## 9) Implicações técnicas
- Fingerprint deve ser:
  - determinístico
  - independente da ordem das colunas
- Versionamento é **obrigatório**
- Dataset bloqueado **não promove Silver**
- UI/Admin Console devem refletir estado de bloqueio

---

## 10) Regras imutáveis (guardrails)
- ❌ Silver escrever com schema divergente
- ❌ Auto-approve de mudanças estruturais
- ❌ Ignorar drift
- ✅ Bloqueio automático sempre
- ✅ Aprovação explícita obrigatória

---

## 11) Referências
- ADR-0003 — ExpectSchemaJSON
- ADR-0004 — Last Write Wins + Merge
- `docs/02-specs/phase-03-schema-versioning-and-blocking.md`
- PRD — Plataforma de Ingestão Governada

---

## 12) Encerramento
Este ADR fecha o **conjunto de decisões arquiteturais imutáveis**
relacionadas a **execução, escrita e governança de schema**.
