# 🔥 Decisões Arquiteturais Críticas - Carga Incremental Universal

> **Status:** Aprovado após revisão técnica  
> **Data:** 2026-02-21  
> **Plano Completo:** Ver `plan_id: 026fda13-4ed1-44b6-9572-c51ea0e0745d`

---

## 🎯 Mudanças Fundamentais Aplicadas ao Plano

### 1️⃣ Discovery: Sugestão + Confirmação (NÃO Automático 100%)

**❌ ANTES (Perigoso):**
- Discovery ativava incremental automaticamente
- `incremental_strategy` definido direto no banco
- Risco: PKs erradas, timestamps que não refletem UPDATEs

**✅ AGORA (Seguro):**
- Discovery **sugere**, usuário **confirma**
- Nova coluna: `discovery_suggestion` (sugestão pendente)
- Nova coluna: `discovery_status = 'PENDING_CONFIRMATION'`
- Após confirmação: `strategy_locked = TRUE` (protege contra re-discovery acidental)

**Justificativa:**
> Oracle tem PKs incorretas, Unique Keys de negócio duvidosas, colunas de data que não mudam em UPDATE. Discovery silencioso é bomba-relógio.

---

### 2️⃣ Bronze: Snapshot vs Current (Não "One Size Fits All")

**❌ ANTES (Incompleto):**
- Apenas MERGE como padrão universal
- Fragmentação Delta ignorada
- Tabelas pequenas teriam overhead desnecessário

**✅ AGORA (Flexível):**
```
bronze_mode:
  - SNAPSHOT:    OVERWRITE completo (tabelas pequenas, dimensões)
  - CURRENT:     MERGE incremental (tabelas grandes com watermark)
  - APPEND_LOG:  APPEND puro (logs, eventos)
```

**Decisão por tamanho:**
- `< 1M rows` → SNAPSHOT (simples, sem fragmentação)
- `> 10M rows + watermark` → CURRENT (eficiente)
- `Sem PK + timestamp` → APPEND_LOG

**Justificativa:**
> MERGE gera arquivos pequenos, aumenta fragmentação, precisa OPTIMIZE periódico. Nem toda tabela precisa disso.

---

### 3️⃣ Watermark: `>=` com Dedupe (NÃO `>`)

**❌ ANTES (Perde Dados):**
```sql
WHERE watermark_col > :last_value
```
**Problema:** Registros com mesmo timestamp máximo são perdidos silenciosamente.

**✅ AGORA (Seguro):**
```sql
WHERE watermark_col >= :last_value
```
Depois: deduplicar por PK + manter maior watermark.

**Justificativa:**
> Perda silenciosa de dados é inaceitável. `>=` + dedupe garante idempotência.

---

### 4️⃣ Hash: Normalização + Exclusão de Voláteis

**❌ ANTES (Ineficiente):**
- Hash de **todas** as colunas
- Decimals com precisão variável
- Timestamps com microsegundos
- Colunas `UPDATED_AT`, `LAST_ACCESS` incluídas

**Resultado:** UPDATEs desnecessários por variações irrelevantes.

**✅ AGORA (Inteligente):**
```python
# Normalização
decimals:    CAST(col AS DECIMAL(p,s)) → string fixo
timestamps:  DATE_TRUNC('second', col)
strings:     TRIM(UPPER(col))  # se case-insensitive

# Exclusão automática de voláteis
hash_exclude_cols = [
    "UPDATED_AT", "LAST_MODIFIED", "LAST_ACCESS", 
    "MODIFIED_AT", "LAST_LOGIN", "ACCESS_COUNT"
]

# Override manual permitido no JSON metadata
```

**Justificativa:**
> Colunas voláteis mudam sempre sem refletir mudança real de negócio. Hash normalizado evita falsos positivos.

---

### 5️⃣ Reconciliação: Opt-in (NÃO Default)

**❌ ANTES (Custoso):**
- Reconciliação periódica para todos datasets
- Full scan Oracle + Bronze = 💥

**✅ AGORA (Seletivo):**
```
enable_reconciliation = FALSE  # Padrão
```
**Habilitar apenas para:**
- Dimensões pequenas (< 1M rows)
- Tabelas críticas onde delete tracking é obrigatório

**Para fatos grandes:**
- Reconciliação **desabilitada**
- Exigir CDC/SCN ou aceitar sem delete tracking

**Justificativa:**
> Comparar todas PKs de Oracle vs Bronze para tabela de 100M rows é inviável.

---

### 6️⃣ Rollout: Opt-in Gradual (NÃO Big Bang)

**❌ ANTES (Arriscado):**
- Migration 004 ativa incremental para todos
- Bronze dropada e recriada
- Quebra ingestões existentes

**✅ AGORA (Seguro):**
```sql
-- Datasets existentes
incremental_strategy = 'SNAPSHOT'  -- Mantém comportamento atual
enable_incremental = FALSE         -- Opt-in explícito

-- Novos datasets
discovery_status = 'PENDING'       -- Discovery roda, mas não ativa
```

**Fluxo:**
1. Dataset criado → Discovery sugere estratégia
2. Usuário revisa e confirma (via UI/API)
3. `enable_incremental = TRUE` + `strategy_locked = TRUE`
4. Próxima execução usa nova estratégia

**Justificativa:**
> Produção não pode quebrar. Opt-in permite validação incremental dataset por dataset.

---

## 📊 Schema Completo Atualizado

```sql
ALTER TABLE dataset_control ADD COLUMNS (
  -- Estratégia
  incremental_strategy STRING,       -- WATERMARK | HASH_MERGE | SNAPSHOT | APPEND_LOG
  incremental_metadata STRING,       -- JSON com PK, watermark_col, hash_exclude_cols
  strategy_locked BOOLEAN,           -- Protege contra re-discovery acidental
  enable_incremental BOOLEAN,        -- FALSE por padrão (opt-in)
  
  -- Modo Bronze
  bronze_mode STRING,                -- SNAPSHOT | CURRENT | APPEND_LOG
  
  -- Discovery tracking
  last_discovery_at TIMESTAMP,
  discovery_status STRING,           -- PENDING | PENDING_CONFIRMATION | SUCCESS | FAILED
  discovery_suggestion STRING,       -- Estratégia sugerida antes da confirmação
  
  -- Reconciliação (opt-in)
  enable_reconciliation BOOLEAN,     -- FALSE por padrão
  last_reconciliation_at TIMESTAMP,
  
  -- Safety monitoring
  watermark_stale_threshold_hours INT,  -- Alerta: 48h sem watermark avançar
  
  -- Performance
  last_optimize_at TIMESTAMP,
  optimize_frequency_hours INT       -- OPTIMIZE ZORDER automático (24h)
);
```

---

## 🔐 Colunas Técnicas Bronze INCREMENTAL

**Bronze INCREMENTAL (10 colunas técnicas):**
```
_ingestion_ts        TIMESTAMP
_batch_id            STRING
_source_table        STRING
_op                  STRING        -- INSERT/UPDATE/UPSERT
_watermark_col       STRING
_watermark_value     STRING
_row_hash            STRING        -- MD5 normalizado
_is_deleted          BOOLEAN
_valid_from          TIMESTAMP     -- SCD Type 2 futuro
_valid_to            TIMESTAMP     -- NULL = atual
```

**Bronze SNAPSHOT (simples):**
```
[COLUNAS_ORIGINAIS_ORACLE]
_ingestion_ts        TIMESTAMP     (opcional)
_batch_id            STRING        (opcional)
```

---

## 🚨 Alertas Críticos de Segurança

### Monitoramento Obrigatório:
1. **Watermark Estagnado:** Não avançou em 48h
2. **Leitura Zero:** Incremental retornou 0 linhas por 5 execuções consecutivas
3. **Watermark Quebrado:** Incremental retornou 100% da tabela (deveria ser incremental)
4. **Reconciliação Suspeita:** Marcou > 10% da tabela como deletada
5. **Fragmentação:** Bronze tem > 1000 arquivos pequenos
6. **OPTIMIZE Atrasado:** Última otimização há mais de `optimize_frequency_hours`

---

## 🎯 Roadmap Ajustado

### Fase 1 - Core Engine (2-3 semanas)
✅ Migration com `enable_incremental = FALSE` por padrão  
✅ Discovery que **sugere**, não aplica automaticamente  
✅ Hash normalizado com exclusão de voláteis  
✅ Watermark `>=` com dedupe  
✅ `_optimize_bronze_table()` com ZORDER  
✅ Fallback seguro para SNAPSHOT se discovery falhar

### Fase 2 - UI e API (1 semana)
✅ Modal de confirmação de discovery  
✅ Override manual de `watermark_col` e `hash_exclude_cols`  
✅ Toggle `enable_incremental` com warning  
✅ Badges visuais: 🟢 INCREMENTAL | 🔵 SNAPSHOT | 🟡 PENDING_CONFIRMATION

### Fase 3 - Reconciliação (1 semana)
✅ Opt-in com `enable_reconciliation`  
✅ Validação: apenas para < 1M rows  
✅ Alerta se > 10% deletado

### Fase 4 - Observabilidade (1 semana)
✅ Dashboard de estratégias por tipo  
✅ 6 alertas críticos implementados  
✅ Job agendado: OPTIMIZE automático  
✅ Monitoramento de fragmentação Delta

---

## ✅ Resultado Final Esperado

### Motor Universal Seguro:
- ✅ Discovery inteligente com confirmação obrigatória
- ✅ 4 estratégias (WATERMARK, HASH_MERGE, SNAPSHOT, APPEND_LOG)
- ✅ Fallback automático para SNAPSHOT se falhar

### Eficiência com Segurança:
- ✅ 70-90% redução tempo execução (tabelas grandes com watermark)
- ✅ 0% risco de quebrar ingestões existentes (opt-in gradual)
- ✅ OPTIMIZE automático previne fragmentação

### Auditoria Completa:
- ✅ Rastreabilidade: quando/como cada registro foi ingerido
- ✅ Idempotência: `>=` + dedupe garante sem duplicatas
- ✅ Soft delete: histórico de remoções (quando habilitado)

### Monitoramento Proativo:
- ✅ 6 alertas críticos detectam problemas antes de virar crise
- ✅ Dashboard de eficiência mostra economia real
- ✅ Safety checks previnem perda silenciosa de dados

---

## 🔥 Pontos de Atenção para Implementação

1. **NUNCA** ativar incremental sem confirmação do usuário
2. **SEMPRE** usar `WHERE watermark >= :last_value` (não `>`)
3. **SEMPRE** normalizar hash (decimals, timestamps)
4. **SEMPRE** excluir colunas voláteis do hash
5. **SEMPRE** permitir opt-in para reconciliação (não default)
6. **SEMPRE** manter `_load_oracle_bronze()` original como fallback
7. **SEMPRE** executar OPTIMIZE periódico em Bronze CURRENT

---

## 💥 5 Correções Finais de Arquitetura de Guerra

### ❌ 1. _valid_from/_valid_to Removido da Bronze CURRENT

**Problema Identificado:**
- Bronze CURRENT via MERGE sobrescreve registros
- `_valid_from`/`_valid_to` perdem sentido (estado atual apenas)
- Híbrido entre snapshot e SCD2 gera confusão semântica

**✅ Solução:**
- Bronze CURRENT = **estado atual apenas** (8 colunas técnicas, sem _valid_from/_valid_to)
- **SCD Type 2 APENAS na Silver**, onde há contrato e lógica de historificação adequada
- Bronze SNAPSHOT: ainda mais simples (apenas colunas originais + opção de _ingestion_ts/_batch_id)

---

### ❌ 2. Performance da Validação de Incrementalidade

**Problema Identificado:**
```sql
-- NUNCA fazer isso em tabela grande:
SELECT COUNT(DISTINCT col) FROM tabela_100M_rows;  -- Extremamente caro
```

**✅ Solução:**
```sql
-- Opção A: Stats do Oracle (instantâneo)
SELECT num_rows, num_distinct, low_value, high_value, num_nulls
FROM all_tab_col_statistics
WHERE owner = :owner AND table_name = :table AND column_name = :col;

-- Opção B: Amostra 1% (se stats não disponíveis)
SELECT 
  COUNT(*) * 100 as total_rows_estimate,
  APPROX_COUNT_DISTINCT(col) * 100 as distinct_values_estimate
FROM owner.table@dblink SAMPLE (1);
```

**Justificativa:**
> `COUNT(DISTINCT col)` em tabela de 50M rows pode levar minutos. Stats do Oracle são instantâneos.

---

### ❌ 3. Hash+Merge para Tabelas Muito Grandes

**Problema Identificado:**
- HASH_MERGE = full scan Oracle + full scan Bronze + merge pesado
- Para tabelas 100M+ rows, mesmo com hash normalizado, é **inviável**

**✅ Solução:**
```python
elif pk_found and 10M <= table_size <= 100M:  # Hash+Merge: tabelas médias
    strategy = "HASH_MERGE"
elif pk_found and table_size > 100M:  # Tabelas muito grandes: CDC obrigatório
    strategy = "REQUIRES_CDC"
    metadata = {
        "reason": "Table too large for full scan hash comparison",
        "recommendation": "Implement Oracle CDC/LogMiner or accept SNAPSHOT mode"
    }
```

**Justificativa:**
> Full scan de 200M rows Oracle + 200M rows Bronze para comparar hash = 30-60 minutos. CDC é obrigatório para esse porte.

---

### ❌ 4. Reconciliação: Usar LEFT ANTI JOIN (NUNCA NOT IN)

**Problema Identificado:**
```sql
-- ERRADO: Performance ruim, nulls causam bugs
SELECT pk FROM bronze WHERE pk NOT IN (SELECT pk FROM oracle);
```

**✅ Solução:**
```sql
-- CORRETO: LEFT ANTI JOIN
SELECT bronze.pk1, bronze.pk2
FROM bronze_table bronze
LEFT ANTI JOIN oracle_pks oracle
  ON bronze.pk1 = oracle.pk1 AND bronze.pk2 = oracle.pk2
WHERE bronze._is_deleted = FALSE;
```

**Justificativa:**
> `NOT IN` com NULL values retorna resultado incorreto. ANTI JOIN é semanticamente correto e mais rápido.

---

### ❌ 5. Otimização Condicional (Não Excessiva)

**Problema Identificado:**
- OPTIMIZE a cada 24h pode ser:
  - **Excessivo** se houver poucos merges
  - **Insuficiente** se houver muitos merges
- ZORDER só faz sentido se PK é usada para filtros

**✅ Solução:**
```python
def _optimize_bronze_table_conditional(bronze_table, pk_cols):
    # Rodar OPTIMIZE apenas se:
    if merge_count_since_optimize > optimize_threshold_merges:  # Ex: 100 merges
        spark.sql(f"OPTIMIZE {bronze_table} ZORDER BY ({','.join(pk_cols)})")
        # Resetar contador
        update_dataset_control(merge_count_since_optimize=0, last_optimize_at=now())
```

**Nova coluna:**
```sql
optimize_threshold_merges INT DEFAULT 100,  -- OPTIMIZE após 100 merges
merge_count_since_optimize INT DEFAULT 0    -- Contador
```

**Justificativa:**
> OPTIMIZE é caro. Rodar após 100 merges é mais eficiente que tempo fixo.

---

### ❌ BONUS: Watermark Tipado + Override para Reprocessamento

**Problema Identificado:**
- `watermark_value` como STRING genérica:
  - Comparação lexical incorreta
  - Problemas de timezone
  - Erro se formato mudar
- Sem mecanismo para reprocessamento histórico (ex: voltar 7 dias)

**✅ Solução:**
```python
def _get_last_watermark(dataset_id):
    # PRIORITY: Verificar override primeiro (reprocessamento manual)
    override = get_override_watermark_value(dataset_id)
    if override is not None:
        return override  # Tipado: TIMESTAMP ou NUMERIC
    
    # Senão, usar watermark normal
    return get_normal_watermark(dataset_id)  # Tipado: TIMESTAMP ou NUMERIC
```

**Nova coluna:**
```sql
override_watermark_value STRING COMMENT 'Valor manual de watermark para reprocessamento (NULL = usar normal)'
```

**Uso:**
```sql
-- Reprocessar últimos 7 dias
UPDATE dataset_control 
SET override_watermark_value = '2026-02-14 00:00:00'
WHERE dataset_id = 'xxx';

-- Limpar override após reprocessamento
UPDATE dataset_control 
SET override_watermark_value = NULL
WHERE dataset_id = 'xxx';
```

**Justificativa:**
> Reprocessamento histórico é requisito operacional comum. Watermark tipado evita bugs de comparação.

---

## 🚨 Riscos Arquiteturais Ocultos

### Risco 1: Complexidade Operacional
**Problema:**
- 4 estratégias + 3 bronze_modes + reconciliação + safety checks
- Se UI não for clara, vira confusão operacional

**Mitigação:**
- ✅ Discovery sugere (não aplica automaticamente)
- ✅ Modal de confirmação com detalhes visíveis
- ✅ Badges visuais: 🟢 INCREMENTAL | 🔵 SNAPSHOT | 🟡 PENDING_CONFIRMATION
- ✅ Documentação inline no próprio formulário

### Risco 2: Fragmentação Delta
**Problema:**
- MERGE frequente + ZORDER ocasional = explosão de small files

**Mitigação:**
- ✅ OPTIMIZE condicional (`optimize_threshold_merges`)
- ✅ Monitoramento de fragmentação: alertar se > 1000 arquivos pequenos
- ✅ Contador `merge_count_since_optimize` rastreia pressão
- ✅ Job agendado verifica `small_files_count` periodicamente

### Risco 3: Reprocessamento Histórico Sem Mecanismo Claro
**Problema:**
- Watermark errado, precisa voltar 7 dias
- Sem override, precisa dropar tabela e refazer tudo

**Mitigação:**
- ✅ `override_watermark_value` permite reprocessamento cirurgico
- ✅ UI com campo de override + warning
- ✅ Limpar override automático após execução bem-sucedida

---

## ✅ Checklist Final de Implementação

**ANTES de começar Fase 1:**

- [ ] Confirmar que `_valid_from`/`_valid_to` está **fora** da Bronze CURRENT
- [ ] Validar que discovery usa `all_tab_col_statistics` ou `SAMPLE (1)` (não `COUNT DISTINCT`)
- [ ] Adicionar estratégia `REQUIRES_CDC` para tabelas > 100M rows
- [ ] Garantir que reconciliação usa `LEFT ANTI JOIN` (nunca `NOT IN`)
- [ ] Implementar OPTIMIZE condicional com `optimize_threshold_merges`
- [ ] Tipar `_watermark_value` como TIMESTAMP/NUMERIC (não string)
- [ ] Adicionar `override_watermark_value` para reprocessamento manual

---

> **Próximo passo:** Implementar Fase 1 começando pela Migration 004 **APÓS validar checklist acima**
