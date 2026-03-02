# docs/03-adrs/ADR-0004-last-write-wins-merge.md

## ADR-0004 — Last Write Wins + MERGE Determinístico na Silver

**Status:** Accepted  
**Data:** 2025-12-12  
**Decisores:** Plataforma de Dados / Arquitetura  
**Contexto:** Plataforma de Ingestão Governada

---

## 1) Contexto
A camada Silver precisa garantir **consistência determinística** quando:
- existem múltiplos registros para a mesma chave natural;
- há reprocessamentos, backfills ou retries;
- a origem não garante unicidade perfeita;
- eventos chegam fora de ordem.

Sem uma política explícita, surgem:
- duplicidades;
- resultados não reprodutíveis;
- divergências entre execuções;
- impactos downstream difíceis de rastrear.

---

## 2) Decisão
Adotar **Last Write Wins (LWW)** como política padrão de resolução de conflitos na Silver, implementada via **MERGE determinístico** no Delta Lake.

Definição:
- Para cada **chave de negócio** (ou técnica),
- o **registro mais recente** vence,
- com base em um **campo determinístico** (ex.: `updated_at`, `event_time`, `ingestion_ts`),
- definido no **ExpectSchemaJSON**.

---

## 3) Justificativa
### Por que Last Write Wins
- Simples de explicar e operar
- Determinístico e reproduzível
- Compatível com retries e backfills
- Escala bem para batch

### Por que MERGE (e não append + dedupe posterior)
- Atomicidade
- Menos janelas de inconsistência
- Métricas claras por execução
- Melhor governança operacional

---

## 4) Regras de deduplicação (obrigatórias)
Antes do MERGE, o dataset **DEVE**:
1. Agrupar por chave(s) definidas no contrato
2. Ordenar por campo de controle (desc)
3. Manter apenas 1 registro por chave

Exemplo lógico:
```sql
ROW_NUMBER() OVER (
  PARTITION BY business_key
  ORDER BY control_timestamp DESC
) = 1
```

---

## 5) Fluxo de escrita na Silver

1. DataFrame Bronze validado pelo contrato
2. Aplicação de cast explícito
3. Dedupe via LWW
4. Escrita via MERGE:
   - MATCHED → UPDATE
   - NOT MATCHED → INSERT
5. Registro de métricas
6. Atualização de watermark

---

## 6) Pseudocódigo (simplificado)
```python
deduped_df = (
  df
  .withColumn(
    "_rn",
    row_number().over(
      Window.partitionBy(pk_cols).orderBy(col(order_col).desc())
    )
  )
  .filter(col("_rn") == 1)
)

(
  delta_table.alias("t")
  .merge(
    deduped_df.alias("s"),
    " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])
  )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)
```

---

## 7) Consequências
### Positivas
- Resultado previsível
- Idempotência prática
- Reprocessamento seguro
- Base sólida para Gold

### Negativas / Trade-offs
- Requer definição clara de chave
- Requer coluna de ordenação confiável
- Não preserva histórico por padrão

---

## 8) Alternativas consideradas (e rejeitadas)

### A) Append-only + view deduplicada
**Rejeitado**:
- Complexidade operacional
- Custos maiores
- Inconsistência temporal

### B) First Write Wins
**Rejeitado**:
- Não funciona bem com backfills
- Não reflete estado mais recente

---

## 9) Implicações técnicas
- ExpectSchemaJSON **DEVE** declarar:
  - chaves (`primary_key`)
  - coluna de ordenação (`watermark` ou `last_updated`)
- Silver sempre usa MERGE quando houver chave
- Métricas de insert/update devem ser coletadas
- LWW é padrão; exceções exigem novo ADR

---

## 10) Regras imutáveis (guardrails)
- ❌ MERGE sem dedupe prévio
- ❌ Critério implícito de ordenação
- ❌ Append silencioso em Silver
- ✅ Dedupe determinístico sempre
- ✅ MERGE explícito e auditável

---

## 11) Referências
- ADR-0003 — ExpectSchemaJSON como contrato Silver
- `docs/02-specs/phase-02-silver-contract-and-run-queue.md`
- Delta Lake MERGE semantics
- PRD — Plataforma de Ingestão Governada

---

## 12) Próximos ADRs relacionados
- ADR-0005 — Schema Change Blocking
