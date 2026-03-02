# Bronze Modes e Estratégias de Carga Incremental

## 📚 Índice

1. [Diferença Entre Bronze Mode e Estratégia](#diferença-entre-bronze-mode-e-estratégia)
2. [Bronze Modes (Modos de Escrita)](#bronze-modes-modos-de-escrita)
3. [Estratégias de Descoberta](#estratégias-de-descoberta)
4. [Comparação Prática](#comparação-prática)
5. [Como o Sistema Decide](#como-o-sistema-decide)
6. [Tratamento de Deletions](#tratamento-de-deletions)

---

## 🎯 Diferença Entre Bronze Mode e Estratégia

### **Bronze Mode** (Modo de Escrita)
- **O QUÊ**: Define **como** os dados são **escritos** na camada Bronze
- **QUANDO**: Aplicado durante a **gravação** no Delta Lake
- **QUEM**: Usuário pode **escolher** manualmente via interface
- **EXEMPLOS**: SNAPSHOT, CURRENT, APPEND_LOG

### **Estratégia** (Incremental Strategy)
- **O QUÊ**: Define **como** o sistema **identifica** dados novos/modificados na **origem**
- **QUANDO**: Aplicado durante a **leitura** da fonte (Oracle, SQL Server, etc)
- **QUEM**: Sistema **descobre automaticamente** na primeira execução
- **EXEMPLOS**: WATERMARK, HASH_MERGE, SNAPSHOT, REQUIRES_CDC

### Analogia
```
┌──────────────────────────────────────────────────────────┐
│ ORIGEM (Oracle)                                          │
│ ┌────────────────────────────────────────────────────┐   │
│ │ Tabela: GLO_GRUPO_USUARIO                          │   │
│ │ 1.000.000 registros                                │   │
│ │ Coluna: DT_UPDATED                                 │   │
│ └────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
                    ↓
          ┌─────────────────────┐
          │ ESTRATÉGIA          │ ← Como IDENTIFICAR dados novos?
          │ (Descoberta)        │   (WATERMARK, HASH, CDC, etc)
          ├─────────────────────┤
          │ WHERE DT_UPDATED    │
          │ >= last_watermark   │
          └─────────────────────┘
                    ↓ (retorna 5.000 registros novos)
┌──────────────────────────────────────────────────────────┐
│ BRONZE (Delta Lake)                                      │
│ ┌────────────────────────────────────────────────────┐   │
│ │ Tabela: bronze_mega.GLO_GRUPO_USUARIO              │   │
│ │ 995.000 registros existentes                       │   │
│ └────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
                    ↓
          ┌─────────────────────┐
          │ BRONZE MODE         │ ← Como GRAVAR dados?
          │ (Modo de Escrita)   │   (SNAPSHOT, CURRENT, APPEND)
          ├─────────────────────┤
          │ MERGE INTO bronze   │
          │ WHEN MATCHED UPDATE │
          │ WHEN NOT MATCHED    │
          │ INSERT              │
          └─────────────────────┘
                    ↓
┌──────────────────────────────────────────────────────────┐
│ BRONZE (Atualizado)                                      │
│ ┌────────────────────────────────────────────────────┐   │
│ │ 1.000.000 registros (995k antigos + 5k novos)     │   │
│ └────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

---

## 🎨 Bronze Modes (Modos de Escrita)

### 1. **SNAPSHOT** - Sobrescrita Completa

#### Comportamento SQL
```sql
INSERT OVERWRITE TABLE bronze_table
SELECT * FROM source_data;
```

#### Características
- 📸 **Operação**: INSERT OVERWRITE (truncate + insert)
- ❌ **Histórico**: Não mantém versões antigas
- ✅ **Deletions**: Automáticas (dados deletados na origem somem)
- 🔄 **Update**: Não aplicável (sobrescreve tudo)
- 💾 **Espaço**: Cresce apenas com volume de dados atuais

#### Quando Usar
- ✅ Tabelas de dimensão pequenas (< 100k registros)
- ✅ Dados de referência (países, categorias, etc)
- ✅ Carga FULL sempre
- ✅ Quando histórico não é necessário

#### Exemplo Prático
**Antes (Bronze)**:
| ID | Nome | Email | _batch_id |
|----|------|-------|-----------|
| 1 | João | joao@email.com | 100 |
| 2 | Maria | maria@email.com | 100 |
| 3 | José | jose@email.com | 100 |

**Origem (Atual)**:
| ID | Nome | Email |
|----|------|-------|
| 1 | João Silva | joao@email.com |
| 4 | Ana | ana@email.com |

**Depois (Bronze) - SNAPSHOT**:
| ID | Nome | Email | _batch_id |
|----|------|-------|-----------|
| 1 | João Silva | joao@email.com | 101 |
| 4 | Ana | ana@email.com | 101 |

⚠️ **Maria e José desapareceram!**

---

### 2. **CURRENT** - Merge Incremental (UPSERT)

#### Comportamento SQL
```sql
MERGE INTO bronze_table AS target
USING source_data AS source
ON target.pk_col = source.pk_col
WHEN MATCHED THEN
  UPDATE SET target.* = source.*
WHEN NOT MATCHED THEN
  INSERT *;
```

#### Características
- 🔄 **Operação**: MERGE (UPDATE existentes + INSERT novos)
- ⚠️ **Histórico**: Mantém apenas última versão (estado atual)
- ⚠️ **Deletions**: Requer soft delete ou flag (ex: IS_DELETED)
- ✅ **Update**: Sim, atualiza registros existentes
- 💾 **Espaço**: Cresce apenas com novos registros

#### Quando Usar
- ✅ Tabelas transacionais com chave primária
- ✅ CDC (Change Data Capture)
- ✅ Quando você quer o **estado atual** dos dados
- ✅ Carga incremental sem histórico completo

#### Requisitos
- 🔑 **Chave Primária**: Obrigatória para identificar registros
- 🏷️ **Soft Delete**: Se quiser tratar deletions (flag IS_DELETED)

#### Exemplo Prático
**Antes (Bronze)**:
| ID | Nome | Email | _batch_id |
|----|------|-------|-----------|
| 1 | João | joao@email.com | 100 |
| 2 | Maria | maria@email.com | 100 |
| 3 | José | jose@email.com | 100 |

**Dados Incrementais (Origem)**:
| ID | Nome | Email |
|----|------|-------|
| 1 | João Silva | joao_new@email.com |
| 4 | Ana | ana@email.com |

**Depois (Bronze) - CURRENT**:
| ID | Nome | Email | _batch_id |
|----|------|-------|-----------|
| 1 | João Silva | joao_new@email.com | 101 | ← UPDATED
| 2 | Maria | maria@email.com | 100 | ← Mantido
| 3 | José | jose@email.com | 100 | ← Mantido
| 4 | Ana | ana@email.com | 101 | ← INSERTED

✅ **João foi atualizado, Ana foi inserida, Maria e José continuam**

---

### 3. **APPEND_LOG** - Apenas Append (Histórico Completo)

#### Comportamento SQL
```sql
INSERT INTO bronze_table
SELECT 
  *,
  current_timestamp() AS _ingestion_time,
  'INSERT' AS _operation_type,
  <batch_id> AS _batch_id
FROM source_data;
```

#### Características
- 📝 **Operação**: INSERT apenas (nunca UPDATE)
- ✅ **Histórico**: Mantém TODAS as versões (audit trail completo)
- ✅ **Deletions**: Aparecem como novo registro (tipo=DELETE)
- ❌ **Update**: Não faz UPDATE (cria novo registro)
- 💾 **Espaço**: Cresce continuamente (precisa compactação)

#### Quando Usar
- ✅ Logs de eventos imutáveis
- ✅ Tabelas de auditoria
- ✅ CDC com histórico completo
- ✅ Data Lake com append-only pattern
- ✅ Quando toda mudança precisa ser rastreada

#### Exemplo Prático
**Antes (Bronze)**:
| ID | Nome | Email | _batch_id | _operation |
|----|------|-------|-----------|-----------|
| 1 | João | joao@email.com | 100 | INSERT |
| 2 | Maria | maria@email.com | 100 | INSERT |
| 3 | José | jose@email.com | 100 | INSERT |

**Dados Incrementais (CDC)**:
| ID | Nome | Email | _operation |
|----|------|-------|-----------|
| 1 | João Silva | joao_new@email.com | UPDATE |
| 4 | Ana | ana@email.com | INSERT |
| 2 | Maria | maria@email.com | DELETE |

**Depois (Bronze) - APPEND_LOG**:
| ID | Nome | Email | _batch_id | _operation |
|----|------|-------|-----------|-----------|
| 1 | João | joao@email.com | 100 | INSERT |
| 2 | Maria | maria@email.com | 100 | INSERT |
| 3 | José | jose@email.com | 100 | INSERT |
| 1 | João Silva | joao_new@email.com | 101 | UPDATE | ← NOVO
| 4 | Ana | ana@email.com | 101 | INSERT | ← NOVO
| 2 | Maria | maria@email.com | 101 | DELETE | ← NOVO

✅ **Histórico completo mantido! João tem 2 versões**

---

## 🔍 Estratégias de Descoberta

### 1. **WATERMARK** - Baseado em Data/Timestamp

#### Como Funciona
```sql
-- Sistema identifica: watermark_column = "DT_UPDATED"
SELECT * FROM source_table
WHERE DT_UPDATED >= :last_watermark_value
  AND DT_UPDATED < :current_execution_time
```

#### Requisitos
- ✅ Coluna DATE ou TIMESTAMP confiável
- ✅ Coluna é atualizada quando registro muda
- ✅ Sem NULL values na coluna

#### Vantagens
- 🚀 **Performance**: Query otimizada com índice na coluna
- 📊 **Eficiência**: Apenas dados novos são lidos
- 🎯 **Precisão**: Alta se a coluna for confiável

#### Desvantagens
- ⚠️ **Backfill**: Difícil alterar dados históricos
- ⚠️ **Clock Skew**: Problemas se relógio do servidor estiver errado

---

### 2. **HASH_MERGE** - Comparação de Hash

#### Como Funciona
```sql
-- 1. Sistema calcula hash de cada linha na origem
SELECT 
  pk_col,
  MD5(CONCAT_WS('|', col1, col2, col3, ...)) AS row_hash
FROM source_table;

-- 2. Compara com Bronze
SELECT s.* 
FROM source_table s
LEFT JOIN bronze_table b ON s.pk = b.pk
WHERE b.pk IS NULL OR s.row_hash != b.row_hash;
```

#### Requisitos
- ✅ Chave primária
- ✅ Todas as colunas devem ser incluídas no hash

#### Vantagens
- ✅ **Detecta mudanças**: Qualquer alteração em qualquer coluna
- ✅ **Sem dependência**: Não precisa de coluna de data

#### Desvantagens
- ⚠️ **Performance**: Precisa calcular hash de TODAS as linhas
- ⚠️ **Custo**: Alto para tabelas grandes (milhões de registros)

---

### 3. **SNAPSHOT** - Sem Incremental

#### Como Funciona
```sql
-- Lê tudo, sempre
SELECT * FROM source_table;
```

#### Quando o Sistema Escolhe
- ❌ Não encontrou coluna de data confiável
- ❌ Não encontrou chave primária
- ❌ Tabela é pequena (< threshold)

#### Características
- 📸 Sempre carga FULL
- ❌ Sem otimização incremental

---

### 4. **APPEND_LOG** - CDC Nativo Simplificado

Quando origem não tem CDC nativo, mas você quer append-only.

---

### 5. **REQUIRES_CDC** - CDC Nativo da Origem

#### Fontes Suportadas
- **Oracle**: Oracle GoldenGate, LogMiner
- **SQL Server**: Change Tracking (CT), Change Data Capture (CDC)
- **PostgreSQL**: Logical Replication
- **MySQL**: Binlog

#### Como Funciona
Sistema lê diretamente do log de transações da origem.

---

## 📊 Comparação Prática

### Cenário: Tabela de Clientes (1.000.000 registros)

#### Dia 1 - Carga Inicial (Bronze vazio)
Todos os modos fazem INSERT de 1M registros.

#### Dia 2 - Mudanças
- 1.000 clientes mudaram email
- 500 novos clientes
- 200 clientes foram deletados na origem

---

### Com **WATERMARK** + **SNAPSHOT**
```
Estratégia: Lê TUDO (1M registros) da origem
Bronze Mode: OVERWRITE (truncate + insert)
Resultado: 1.000.300 registros no Bronze
Histórico: ❌ Perdido
```

---

### Com **WATERMARK** + **CURRENT**
```
Estratégia: Lê apenas 1.500 registros (WHERE DT_UPDATED >= ontem)
Bronze Mode: MERGE (UPDATE 1000 + INSERT 500)
Resultado: 1.000.500 registros no Bronze
Histórico: ⚠️ Apenas última versão
Deletions: ⚠️ 200 deletados ainda estão lá (precisa soft delete)
```

---

### Com **WATERMARK** + **APPEND_LOG**
```
Estratégia: Lê apenas 1.700 registros (1000 updates + 500 inserts + 200 deletes)
Bronze Mode: INSERT (adiciona 1.700 linhas)
Resultado: 1.001.700 registros no Bronze
Histórico: ✅ Completo
Deletions: ✅ Aparecem como tipo=DELETE
```

---

## 🤖 Como o Sistema Decide a Estratégia

### Algoritmo de Descoberta Automática

```
1. Verificar se tem CDC nativo
   └─ Sim → REQUIRES_CDC

2. Procurar coluna de data/timestamp confiável
   ├─ Coluna com nome: updated_at, modified_at, dt_update, etc
   ├─ Coluna é NOT NULL
   ├─ Coluna tem índice
   └─ Sim → WATERMARK

3. Verificar se tem chave primária
   └─ Sim → HASH_MERGE (se não achou coluna de data)

4. Fallback
   └─ SNAPSHOT
```

### Lock de Estratégia

Após primeira execução bem-sucedida:
- ✅ `strategy_locked = TRUE`
- 🔒 Estratégia não pode ser alterada manualmente
- ⚠️ Proteção contra mudanças acidentais

Para forçar re-discovery:
```sql
UPDATE dataset_control
SET strategy_locked = FALSE,
    discovery_status = 'PENDING'
WHERE dataset_id = '<id>';
```

---

## 🗑️ Tratamento de Deletions

### SNAPSHOT
```
✅ Deletions automáticas
- Dados deletados na origem simplesmente não aparecem mais
- Não precisa de lógica extra
```

### CURRENT (MERGE)
```
⚠️ Requer soft delete

Opção 1: Flag na origem
- Origem tem coluna IS_DELETED (0/1)
- MERGE atualiza flag no Bronze

Opção 2: Query com LEFT JOIN
- Periodicamente: identificar registros que sumiram
- Marcar como deleted no Bronze

Opção 3: Não tratar
- Aceitar que deletions não são detectadas
- Adequado se deletions são raras
```

### APPEND_LOG
```
✅ CDC com operação DELETE
- Sistema captura operação de DELETE
- Insere novo registro com _operation = 'DELETE'
- Bronze mantém registro histórico completo
```

---

## 📋 Tabela Resumo

| Bronze Mode | Operação SQL | Histórico | Deletions | Espaço | Uso Ideal |
|-------------|-------------|-----------|-----------|--------|-----------|
| **SNAPSHOT** | INSERT OVERWRITE | ❌ Não | ✅ Auto | Estável | Dimensões pequenas, carga FULL |
| **CURRENT** | MERGE (UPSERT) | ⚠️ Última versão | ⚠️ Soft delete | Cresce com novos | Transacionais, estado atual |
| **APPEND_LOG** | INSERT apenas | ✅ Completo | ✅ CDC | Cresce contínuo | Logs, auditoria, CDC |

| Estratégia | Identifica Dados Por | Requisitos | Performance | Precisão |
|------------|---------------------|------------|-------------|----------|
| **WATERMARK** | Coluna data/timestamp | Coluna confiável | 🚀 Alta | 🎯 Alta |
| **HASH_MERGE** | Hash de colunas | Chave primária | ⚠️ Média | ✅ Alta |
| **SNAPSHOT** | Lê tudo | Nenhum | ❌ Baixa | ✅ Alta |
| **REQUIRES_CDC** | Log transações | CDC nativo | 🚀 Altíssima | 🎯 Perfeita |

---

## 🎓 Recomendações Práticas

### Para Tabelas Transacionais Grandes
```
✅ Estratégia: WATERMARK
✅ Bronze Mode: CURRENT
✅ Configurar: watermark_column, lookback_days
```

### Para Logs/Eventos
```
✅ Estratégia: WATERMARK (ou APPEND_LOG)
✅ Bronze Mode: APPEND_LOG
✅ Configurar: compactação periódica
```

### Para Dimensões Pequenas
```
✅ Estratégia: SNAPSHOT
✅ Bronze Mode: SNAPSHOT
✅ Configurar: Carga FULL diária/semanal
```

### Para CDC Completo com Histórico
```
✅ Estratégia: REQUIRES_CDC (ou WATERMARK)
✅ Bronze Mode: APPEND_LOG
✅ Configurar: CDC na origem
```

---

## 📚 Referências

- **Código Backend**: `src/portalRoutes.js:2467-2572` (endpoint incremental-config)
- **Código Frontend**: `frontend/src/components/IncrementalConfigDialog.tsx:167-240` (UI explicações)
- **Notebook Python**: `databricks_notebooks/governed_ingestion_orchestrator.py`
- **Delta Lake Docs**: https://docs.delta.io/latest/delta-update.html#language-python
- **CDC Patterns**: https://www.databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html

---

## 👥 Suporte

Para dúvidas ou problemas:
1. Consultar logs do backend: `[COLUMNS_PREVIEW]`, `[UPDATE_INCREMENTAL_CONFIG]`
2. Verificar Discovery Status: `SELECT discovery_status, incremental_strategy, strategy_locked FROM dataset_control`
3. Revisar documentação: `docs/IMPLEMENTACAO_MONITORAMENTO_INCREMENTAL.md`

---

**Versão**: 1.0.0  
**Data**: 26/02/2026  
**Status**: ✅ Documentado
