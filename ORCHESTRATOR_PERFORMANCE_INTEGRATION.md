# Integração de Configurações de Performance no Orquestrador

## 📋 Resumo

Integrado o código funcional (que usa tabela de parâmetros) **diretamente no orquestrador governado**, lendo configurações de performance da tabela `dataset_control` onde o usuário já cadastra datasets pelo frontend.

---

## ✅ O que foi feito

### 1. Adicionadas colunas na tabela `dataset_control`

**Arquivo**: `sql/migrations/001_add_performance_columns_to_dataset_control.sql`

**Colunas adicionadas**:
- `oracle_fetchsize` (INT): Número de linhas por batch JDBC (padrão: 10000)
- `spark_num_partitions` (INT): Número de partições Spark para escrita (padrão dinâmico)
- `jdbc_partition_column` (STRING): Coluna para particionamento JDBC paralelo (NULL = desabilitado)
- `jdbc_lower_bound` (BIGINT): Valor mínimo da coluna de particionamento
- `jdbc_upper_bound` (BIGINT): Valor máximo da coluna de particionamento
- `jdbc_num_partitions` (INT): Número de partições JDBC paralelas

### 2. Modificado o orquestrador

**Arquivo**: `databricks_notebooks/governed_ingestion_orchestrator.py`

**Função modificada**: `_load_oracle_bronze`

**Mudanças implementadas**:

#### A. Carregamento de configurações (linhas 517-560)
```python
# Lê configurações de performance da dataset_control
perf_cfg = spark.sql(f"""
    SELECT 
        COALESCE(oracle_fetchsize, 10000) as fetchsize,
        spark_num_partitions,
        jdbc_partition_column,
        jdbc_lower_bound,
        jdbc_upper_bound,
        jdbc_num_partitions
    FROM {CTRL}.dataset_control
    WHERE dataset_id = {_sql_string_literal(dataset_id)}
""").collect()
```

#### B. Fetchsize otimizado (linhas 659-667)
```python
reader = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", dbtable)
    .option("user", user)
    .option("password", pwd)
    .option("driver", "oracle.jdbc.OracleDriver")
    .option("fetchsize", str(fetchsize))  # ✅ OTIMIZAÇÃO APLICADA
)
```

#### C. Particionamento JDBC opcional (linhas 670-679)
```python
# Add JDBC partitioning if configured
if jdbc_part_col and jdbc_lower is not None and jdbc_upper is not None and jdbc_num_parts:
    print(f"⚡ Aplicando particionamento JDBC paralelo...")
    reader = (
        reader
        .option("partitionColumn", jdbc_part_col)
        .option("lowerBound", str(jdbc_lower))
        .option("upperBound", str(jdbc_upper))
        .option("numPartitions", str(jdbc_num_parts))
    )
```

#### D. Limpeza de nomes de colunas (linhas 699-711)
```python
# Clean column names (remove spaces and special characters)
old_columns = df.schema.names
cleaned_count = 0
for col_name in old_columns:
    clean_name = col_name.replace(" ", "").replace("-", "_").replace(".", "_")
    if clean_name != col_name:
        df = df.withColumnRenamed(col_name, clean_name)
        cleaned_count += 1
```

#### E. Reparticionamento dinâmico (linhas 713-732)
```python
if spark_partitions:
    # Usa valor configurado
    df = df.repartition(spark_partitions)
elif source_estimate:
    # Auto-calcula baseado no volume
    if source_estimate < 1_000_000:
        optimal_parts = 200
    elif source_estimate < 10_000_000:
        optimal_parts = 400
    elif source_estimate < 50_000_000:
        optimal_parts = 800
    else:
        optimal_parts = 1600
    df = df.repartition(optimal_parts)
else:
    # Padrão
    df = df.repartition(800)
```

---

## 🚀 Como Usar

### Passo 1: Executar Migration (1 min)

```sql
-- 1. Adicionar colunas na tabela dataset_control
%run sql/migrations/001_add_performance_columns_to_dataset_control.sql
```

### Passo 2: Configurar Dataset Problemático (1 min)

```sql
-- 2. Aplicar otimizações no dataset CMALUINTERNO
%run sql/migrations/002_configure_cmaluinterno_performance.sql
```

**Ou manualmente via MCP Databricks SQL:**
```sql
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET 
    oracle_fetchsize = 10000,
    spark_num_partitions = 200,
    updated_at = current_timestamp()
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
```

### Passo 3: Testar Execução (5-8 min)

1. Executar o dataset normalmente via portal ou job manual
2. Verificar logs - deve mostrar:
   ```
   [BRONZE:ORACLE] ✓ Configurações carregadas:
   [BRONZE:ORACLE]   - Fetchsize: 10000
   [BRONZE:ORACLE]   - Spark partitions: 200
   [BRONZE:ORACLE]   - JDBC Partitioning: DISABLED (leitura sequencial)
   ```
3. Aguardar conclusão (estimativa: 5-8 min vs 20+ min anterior)

### Passo 4: Otimização Adicional (Opcional)

Se ainda lento, habilitar particionamento JDBC:

1. **Descobrir coluna numérica no DBeaver:**
   ```sql
   SELECT column_name, data_type
   FROM all_tab_columns
   WHERE owner = 'CMASTER' 
     AND table_name = 'CMALUINTERNO'
     AND data_type IN ('NUMBER', 'INTEGER')
   ORDER BY column_id;
   ```

2. **Atualizar configuração:**
   ```sql
   UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
   SET 
       jdbc_partition_column = 'ID',  -- nome da coluna encontrada
       jdbc_lower_bound = 1,
       jdbc_upper_bound = 150000,
       jdbc_num_partitions = 8
   WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
   ```

3. **Executar novamente** (deve cair para 2-3 min)

---

## 📊 Comparação de Performance

### Antes (sem otimizações)
- **Fetchsize**: Padrão Oracle (10-50 linhas)
- **Partições**: 800 (fixo, muito para 120K linhas)
- **Limpeza de colunas**: ❌ Não
- **JDBC Partitioning**: ❌ Não
- **Tempo**: > 20 min (travado)

### Depois (com otimizações básicas)
- **Fetchsize**: ✅ 10.000 linhas
- **Partições**: ✅ 200 (otimizado)
- **Limpeza de colunas**: ✅ Sim
- **JDBC Partitioning**: ⏸️ Desabilitado (opcional)
- **Tempo estimado**: 5-8 min ⚡

### Depois (com JDBC Partitioning)
- **Fetchsize**: ✅ 10.000 linhas
- **Partições**: ✅ 200 (otimizado)
- **Limpeza de colunas**: ✅ Sim
- **JDBC Partitioning**: ✅ 8 partições paralelas
- **Tempo estimado**: 2-3 min ⚡⚡

---

## 🎯 Benefícios

### 1. Integração Nativa
- ✅ Configurações no mesmo lugar onde o dataset é criado
- ✅ Não precisa de tabela separada (`0_par.processos`)
- ✅ Frontend pode expor essas configurações futuramente

### 2. Flexibilidade
- ✅ Cada dataset pode ter configurações diferentes
- ✅ Valores padrão inteligentes (10000, auto-partitioning)
- ✅ Particionamento JDBC opcional (para tabelas grandes)

### 3. Compatibilidade
- ✅ Datasets existentes funcionam com valores padrão
- ✅ Migração gradual (tabela por tabela)
- ✅ Sem breaking changes

### 4. Observabilidade
- ✅ Logs mostram configurações aplicadas
- ✅ Fácil troubleshooting
- ✅ Auditoria via `updated_at`/`updated_by`

---

## 📝 Próximos Passos

### Curto Prazo (hoje)
1. ✅ Executar migrations
2. ✅ Configurar dataset CMALUINTERNO
3. ✅ Testar execução (verificar 5-8 min)
4. ✅ Validar dados carregados

### Médio Prazo (semana)
1. Adicionar campos no frontend para configuração de performance
2. Configurar mais 5-10 datasets Oracle
3. Documentar coluna numérica de cada tabela para JDBC partitioning

### Longo Prazo (mês)
1. Migrar todos os datasets Oracle para usar otimizações
2. Adicionar métricas de performance (duração, throughput)
3. Dashboard de performance no portal

---

## 🔧 Troubleshooting

### Erro: "Column 'oracle_fetchsize' not found"
**Causa**: Migration não foi executada  
**Solução**:
```sql
%run sql/migrations/001_add_performance_columns_to_dataset_control.sql
```

### Performance ainda lenta (> 10 min)
**Causa**: Rede lenta ou tabela muito grande  
**Solução**:
1. Habilitar JDBC partitioning (Passo 4 acima)
2. Aumentar fetchsize para 20000
3. Verificar latência de rede Databricks ↔ Oracle

### Erro: "partitionColumn requires numeric column"
**Causa**: Coluna especificada não é numérica  
**Solução**:
1. Verificar tipo da coluna no DBeaver
2. Usar apenas colunas NUMBER, INTEGER, BIGINT
3. Se não tem coluna numérica, deixar `jdbc_partition_column = NULL`

---

## 📚 Arquivos Criados/Modificados

### Criados
- `sql/migrations/001_add_performance_columns_to_dataset_control.sql` ✅
- `sql/migrations/002_configure_cmaluinterno_performance.sql` ✅
- `ORCHESTRATOR_PERFORMANCE_INTEGRATION.md` (este arquivo) ✅

### Modificados
- `databricks_notebooks/governed_ingestion_orchestrator.py` ✅
  - Função `_load_oracle_bronze` (linhas 510-732)

### Removidos
- Nenhum (tudo foi adicionado, sem breaking changes)

---

## 💡 Conclusão

✅ **Integração completa no orquestrador**  
✅ **Lê configurações da tabela dataset_control**  
✅ **Aplica todas as otimizações do código funcional**  
✅ **Compatível com datasets existentes**  
✅ **Pronto para resolver o problema do CMALUINTERNO**

**Próximo passo**: Executar as migrations e testar! 🚀
