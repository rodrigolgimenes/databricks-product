# Oracle DBLink Performance - Soluções

## 🔴 Problema Identificado

**Job ID**: 690887429046802  
**Run ID**: 93973415950563  
**Dataset**: CMASTER.CMALUINTERNO@CMASTERPRD (120.200 linhas)  
**Status**: Travado há 20+ minutos na leitura JDBC  
**Ação**: ✅ Job cancelado manualmente

### Diagnóstico
- **Fase**: BRONZE_LOAD
- **Operação**: `spark.read.format("jdbc")` sem particionamento
- **Causa raiz**: Leitura sequencial de 120K linhas via DBLink remoto
- **Log parou em**: "Escrevendo dados na tabela Delta..."

### Por que é Lento?
1. **DBLink remoto** (`@CMASTERPRD`): Acesso através de database link adiciona latência
2. **Sem particionamento JDBC**: Spark faz uma única query `SELECT * FROM ...`
3. **Rede lenta**: Databricks → Oracle tem latência adicional
4. **Tabela grande sem índices**: 120K linhas sem otimização de leitura

---

## 🚀 Soluções (em ordem de prioridade)

### Solução 1: Particionamento JDBC Paralelo (RECOMENDADO) ⭐

Adicionar opções de particionamento para dividir a leitura em múltiplas queries paralelas.

#### Como Funciona:
- Databricks divide a tabela em N partições baseadas em uma coluna numérica
- Cada partição é lida em paralelo por um executor diferente
- Reduz tempo drasticamente (exemplo: 20min → 3-5min)

#### Implementação:

**Arquivo**: `databricks_notebooks/governed_ingestion_orchestrator.py`  
**Função**: `_load_oracle_bronze` (linhas 614-622)

**ANTES** (código atual):
```python
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", dbtable)
    .option("user", user)
    .option("password", pwd)
    .option("driver", "oracle.jdbc.OracleDriver")
    .load()
)
```

**DEPOIS** (com particionamento):
```python
# Determinar coluna de particionamento (assumindo ROWNUM ou ID)
partition_column = "ROWNUM"  # ou alguma coluna numérica da tabela
num_partitions = 8  # Número de partições paralelas (ajustar conforme cluster)

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", dbtable)
    .option("user", user)
    .option("password", pwd)
    .option("driver", "oracle.jdbc.OracleDriver")
    # Opções de particionamento JDBC
    .option("partitionColumn", partition_column)
    .option("lowerBound", "1")
    .option("upperBound", "150000")  # Valor maior que o total de linhas
    .option("numPartitions", str(num_partitions))
    .option("fetchsize", "10000")  # Tamanho do buffer JDBC
    .load()
)
```

#### Parâmetros Importantes:
- **partitionColumn**: Coluna numérica para dividir (ex: ID, ROWNUM)
- **lowerBound/upperBound**: Range de valores da coluna
- **numPartitions**: Número de partições paralelas (recomendado: 4-8)
- **fetchsize**: Linhas por batch JDBC (10K é bom para Oracle)

#### Problema:
❌ **Precisa de uma coluna numérica** na tabela para particionamento  
❌ Se a tabela não tem ID/ROWNUM indexado, não funciona bem

---

### Solução 2: Query Customizada com WHERE (MAIS SIMPLES) ⭐⭐

Se você sabe que a tabela tem uma coluna de data ou ID, pode usar uma query customizada ao invés de particionamento automático.

#### Implementação:

**Modificar dataset_name no banco** para incluir filtro:

```sql
-- Opção 1: Filtro por data (se tem coluna de data)
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control 
SET dataset_name = '(SELECT * FROM CMASTER.CMALUINTERNO@CMASTERPRD WHERE data_atualizacao >= TRUNC(SYSDATE) - 30) subq'
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';

-- Opção 2: Filtro por ID (se tem coluna numérica)
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control 
SET dataset_name = '(SELECT * FROM CMASTER.CMALUINTERNO@CMASTERPRD WHERE id >= 100000) subq'
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
```

**Prós**:
- ✅ Simples, não requer mudança de código
- ✅ Reduz volume de dados transferidos
- ✅ Funciona com qualquer coluna

**Contras**:
- ❌ Ainda sequencial (não paralelo)
- ❌ Requer conhecimento da estrutura da tabela

---

### Solução 3: Aumentar Timeout JDBC (PALIATIVO)

Se as outras soluções não forem viáveis, aumente o timeout:

```python
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", dbtable)
    .option("user", user)
    .option("password", pwd)
    .option("driver", "oracle.jdbc.OracleDriver")
    .option("fetchsize", "10000")  # Buffer maior
    .option("queryTimeout", "3600")  # Timeout de 1 hora
    .load()
)
```

**Prós**:
- ✅ Permite jobs mais longos completarem

**Contras**:
- ❌ Não resolve o problema de raiz
- ❌ Job ainda será lento (30-60min)

---

### Solução 4: Carga Incremental (LONGO PRAZO)

Ao invés de `SELECT *`, carregar apenas dados novos/modificados.

#### Pré-requisitos:
- Tabela Oracle tem coluna de timestamp de modificação
- Databricks mantém watermark da última carga

#### Implementação:
```python
# Recuperar último timestamp carregado
last_loaded = spark.sql(f"""
    SELECT MAX(data_modificacao) FROM {bronze_table}
""").collect()[0][0]

# Query incremental
if last_loaded:
    dbtable = f"""(
        SELECT * FROM CMASTER.CMALUINTERNO@CMASTERPRD 
        WHERE data_modificacao > TO_TIMESTAMP('{last_loaded}', 'YYYY-MM-DD HH24:MI:SS')
    ) src"""
else:
    dbtable = f"(SELECT * FROM CMASTER.CMALUINTERNO@CMASTERPRD) src"
```

**Prós**:
- ✅ Cargas muito mais rápidas após primeira carga
- ✅ Reduz volume de dados drasticamente

**Contras**:
- ❌ Requer coluna de timestamp na origem
- ❌ Mudança arquitetural significativa

---

## 🎯 Recomendação Imediata

### Para este dataset específico:

**Opção A: Se a tabela tem coluna ID/numérica**
→ Implementar **Solução 1** (Particionamento JDBC)

**Opção B: Se não tem coluna numérica OU é simples**
→ Implementar **Solução 2** (Query com WHERE) para filtrar dados

**Opção C: Para testar rapidamente**
→ Usar **Solução 3** (Aumentar timeout) temporariamente

---

## 📊 Verificação das Colunas da Tabela

Antes de implementar, verifique a estrutura da tabela no DBeaver:

```sql
-- No DBeaver conectado ao Oracle
SELECT column_name, data_type, nullable
FROM all_tab_columns
WHERE owner = 'CMASTER' 
  AND table_name = 'CMALUINTERNO'
ORDER BY column_id;
```

**Procure por**:
- ✅ Coluna numérica (ID, CODIGO, etc.) → Use Solução 1
- ✅ Coluna de data (DATA_ATUALIZACAO, etc.) → Use Solução 2
- ❌ Nenhuma das anteriores → Use Solução 3

---

## 🔧 Script SQL para Cancelamento de Jobs Travados

Para futuras emergências:

```sql
-- Cancelar job travado
UPDATE cm_dbx_dev.ingestion_sys_ops.run_queue 
SET status = 'CANCELLED', 
    last_error_class = 'TIMEOUT',
    last_error_message = 'Job travado - cancelado manualmente',
    finished_at = CURRENT_TIMESTAMP()
WHERE queue_id = '<queue_id>';

-- Marcar batch_process como FAILED
UPDATE cm_dbx_dev.ingestion_sys_ops.batch_process
SET status = 'FAILED',
    finished_at = CURRENT_TIMESTAMP(),
    error_class = 'TIMEOUT',
    error_message = 'JDBC timeout - job excedeu tempo limite'
WHERE run_id = '<run_id>';

-- Finalizar steps pendentes
UPDATE cm_dbx_dev.ingestion_sys_ops.batch_process_steps
SET status = 'FAILED',
    finished_at = CURRENT_TIMESTAMP(),
    message = 'Cancelled due to parent job timeout'
WHERE run_id = '<run_id>' AND status = 'RUNNING';
```

---

## ⏱️ Comparação de Performance Estimada

| Solução | Tempo Estimado | Complexidade | Esforço |
|---------|----------------|--------------|---------|
| **Atual (sem otimização)** | 30-60 min | - | - |
| **Solução 1: Particionamento** | 3-8 min | Média | 30min |
| **Solução 2: Query WHERE** | 10-20 min | Baixa | 5min |
| **Solução 3: Timeout++** | 30-60 min | Baixa | 2min |
| **Solução 4: Incremental** | 1-2 min | Alta | 4-8h |

---

## 📝 Próximos Passos

1. **Verificar estrutura da tabela no DBeaver** (colunas disponíveis)
2. **Escolher solução apropriada** baseado nas colunas
3. **Implementar mudança** (código ou configuração)
4. **Testar nova execução** com monitoramento
5. **Documentar performance** para futuras otimizações

---

## 🆘 Contato para Suporte

Se precisar de ajuda para implementar qualquer solução, me avise! Posso:
- Escrever o código completo para cada solução
- Ajudar a identificar a melhor coluna para particionamento
- Testar e validar a implementação

**Status atual**: Job cancelado, aguardando decisão de otimização 🎯
