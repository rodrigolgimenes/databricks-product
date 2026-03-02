# docs/03-adrs/ADR-0003-silver-contract-expectschemajson.md

## ADR-0003 — ExpectSchemaJSON como Contrato da Silver

**Status:** Accepted  
**Data:** 2025-12-12  
**Decisores:** Plataforma de Dados / Arquitetura  
**Contexto:** Plataforma de Ingestão Governada

---

## 1) Contexto
A camada Silver é responsável por fornecer dados **tecnicamente confiáveis e determinísticos** para consumo analítico.
Problemas recorrentes em pipelines tradicionais:
- inferência implícita de schema;
- casts silenciosos;
- mudanças estruturais não rastreadas;
- quebra de consumidores downstream sem visibilidade.

É necessário um **contrato explícito, versionável e executável** que:
- defina tipos, nulabilidade e regras mínimas;
- permita validação automática;
- seja rastreável e auditável;
- bloqueie mudanças estruturais não aprovadas.

---

## 2) Decisão
Adotar **ExpectSchemaJSON** como o **contrato oficial da Silver**, onde:
- cada dataset possui **um JSON de contrato**;
- o contrato é:
  - versionado,
  - validado,
  - executado no runtime;
- a Silver **nunca infere schema automaticamente**;
- qualquer divergência gera erro explícito.

O ExpectSchemaJSON é a **fonte única da verdade** para:
- estrutura da Silver;
- tipos de dados;
- regras de nulabilidade;
- chaves técnicas (quando aplicável).

---

## 3) Justificativa
### Por que contrato explícito
- Evita “schema drift silencioso”
- Garante previsibilidade para consumidores
- Permite versionamento controlado
- Facilita debugging e governança

### Por que JSON (e não código hardcoded)
- Independente de linguagem
- Auditável
- Fácil diffs
- Consumível por UI e APIs
- Evolutivo para validações mais ricas

---

## 4) Estrutura do ExpectSchemaJSON (alto nível)
```json
{
  "dataset": "sales_orders",
  "version": 3,
  "columns": [
    {
      "name": "order_id",
      "type": "string",
      "nullable": false
    },
    {
      "name": "order_date",
      "type": "timestamp",
      "nullable": false
    }
  ],
  "primary_key": ["order_id"],
  "watermark": {
    "column": "order_date",
    "type": "timestamp"
  }
}
```

> O schema completo é definido em `/contracts/expectschemajson.schema.json`.

---

## 5) Fluxo de execução na Silver

1. Bronze gera DataFrame bruto
2. Driver carrega ExpectSchemaJSON ativo
3. Valida:
   - colunas esperadas
   - tipos
   - nulabilidade
4. Gera **cast plan determinístico**
5. Executa:
   - cast explícito
   - dedupe (se aplicável)
   - merge/append
6. Persistência Silver
7. Métricas registradas em `batch_process_table_details`

---

## 6) Consequências
### Positivas
- Silver 100% previsível
- Erros de schema detectados cedo
- Base sólida para governança e versionamento
- Integração natural com Admin Console e Portal

### Negativas / Trade-offs
- Mais rigor no onboarding de datasets
- Requer contrato antes de produzir Silver
- Curva inicial para usuários menos técnicos

---

## 7) Alternativas consideradas (e rejeitadas)

### A) Inferência automática do Spark
**Rejeitado**:
- Não versionável
- Erros silenciosos
- Drift imprevisível

### B) Schemas hardcoded em código
**Rejeitado**:
- Difícil de auditar
- Acoplamento forte
- Pouca transparência para negócio

---

## 8) Implicações técnicas
- `expect_schema_json` armazenado em:
  - `ingestion_sys.ctrl.schema_versions`
- Silver só executa se:
  - existir versão **ACTIVE**
- Falha de contrato gera:
  - erro `SCHEMA_ERROR`
  - bloqueio automático (ADR-0005)
- Cast failures **não são ignorados**

---

## 9) Regras imutáveis (guardrails)
- ❌ Silver sem contrato
- ❌ Inferência automática de tipos
- ❌ Cast silencioso
- ✅ Contrato explícito sempre
- ✅ Erro acionável e rastreável

---

## 10) Referências
- ADR-0001 — Single Workflow Orchestrator
- ADR-0002 — Run Queue, Retries e Claim
- `docs/02-specs/phase-02-silver-contract-and-run-queue.md`
- PRD — Plataforma de Ingestão Governada

---

## 11) Próximos ADRs relacionados
- ADR-0004 — Last Write Wins + Merge determinístico
- ADR-0005 — Schema Change Blocking
