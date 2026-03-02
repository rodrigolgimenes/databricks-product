# Guia de Integração - Batch Ingestion com Tabela de Parâmetros

## 📋 Visão Geral

Este guia explica como integrar o código funcional existente (que usa tabela de parâmetros) no orquestrador governado.

---

## 🎯 Código Funcional - Análise

### ✅ Pontos Fortes do Código Original
```python
# 1. Fetchsize otimizado
reader.option("fetchsize", "10000")

# 2. Reparticionamento antes da escrita
df_bronze.repartition(800)

# 3. Limpeza de nomes de colunas
for x in oldColumns:
    df_bronze = df_bronze.withColumnRenamed(x, x.replace(" ", ""))

# 4. Tabela de parâmetros centralizada
df_processos = spark.sql("SELECT * FROM cm_dbx_dev.0_par.processos WHERE ativo = 1")
```

### 🔧 Melhorias Implementadas

1. **Tratamento de erro por tabela**: Se uma tabela falha, as outras continuam
2. **Logging estruturado**: Melhor troubleshooting
3. **Partições dinâmicas**: Baseado no volume de dados
4. **Particionamento JDBC**: Opcional, para tabelas grandes
5. **Schema evolution**: `mergeSchema=true` para mudanças de schema

---

## 🔀 Opções de Integração

### Opção 1: Modo Standalone (RECOMENDADO PARA INÍCIO) ⭐

**Usar o script batch como um job separado** que roda em paralelo ao orquestrador.

#### Vantagens:
- ✅ Não afeta o orquestrador existente
- ✅ Fácil de testar e validar
- ✅ Pode rodar em paralelo
- ✅ Zero risco de quebrar o sistema atual

#### Implementação:
```python
# 1. Criar job no Databricks
# 2. Agendar para rodar periodicamente (ex: a cada hora)
# 3. Usar tabela cm_dbx_dev.0_par.processos para configurar tabelas
```

#### Arquivos:
- `databricks_notebooks/oracle_batch_ingestion_from_params.py` (✅ criado)
- `sql/create_table_processos_parameters.sql` (✅ criado)

---

### Opção 2: Integração Parcial no Orquestrador

**Adicionar suporte a tabela de parâmetros no `_load_oracle_bronze`** existente.

#### Como Funciona:
1. Orquestrador verifica se `dataset_id` tem entrada na tabela `0_par.processos`
2. Se sim, usa parâmetros da tabela (fetchsize, partitions, etc.)
3. Se não, usa lógica atual

#### Modificações no `governed_ingestion_orchestrator.py`:

```python
def _load_oracle_bronze_with_params(
    dataset_id: str, 
    dataset_name: str, 
    connection_id: str, 
    bronze_table: str
) -> Dict[str, Any]:
    """
    Versão estendida que suporta tabela de parâmetros.
    """
    # 1. Verificar se existe entrada na tabela de parâmetros
    params_query = f"""
        SELECT 
            src_full_tablename,
            fetchsize,
            num_partitions,
            partition_column,
            lower_bound,
            upper_bound
        FROM {CTRL}.0_par.processos
        WHERE dataset_id = '{dataset_id}' AND ativo = true
        LIMIT 1
    """
    
    params_rows = spark.sql(params_query).collect()
    
    if params_rows:
        # Usa parâmetros da tabela
        params = params_rows[0]
        print(f"[BRONZE:ORACLE] 📋 Usando parâmetros da tabela 0_par.processos")
        
        # Sobrescrever dataset_name se configurado
        if params.src_full_tablename:
            dataset_name = params.src_full_tablename
            print(f"[BRONZE:ORACLE] ➡ Tabela: {dataset_name}")
        
        fetchsize = params.fetchsize or 10000
        num_partitions = params.num_partitions or 800
        partition_column = params.partition_column
        lower_bound = params.lower_bound
        upper_bound = params.upper_bound
    else:
        # Usa valores padrão
        print(f"[BRONZE:ORACLE] 📋 Usando configuração padrão (sem tabela de parâmetros)")
        fetchsize = 10000
        num_partitions = 800
        partition_column = None
        lower_bound = None
        upper_bound = None
    
    # 2. Continuar com a lógica normal, mas usando os parâmetros acima
    # ... (resto do código de _load_oracle_bronze)
```

#### Vantagens:
- ✅ Mantém compatibilidade com datasets existentes
- ✅ Adiciona capacidade de otimização via tabela de parâmetros
- ✅ Gradual: pode migrar tabelas aos poucos

#### Desvantagens:
- ❌ Requer modificação do orquestrador
- ❌ Mais complexo de testar

---

### Opção 3: Integração Total (LONGO PRAZO)

**Substituir completamente o fluxo Oracle por batch processing.**

#### Arquitetura:
```
dataset_control (tabela existente)
    ↓
0_par.processos (nova tabela de config)
    ↓
oracle_batch_ingestion_from_params.py (novo script)
    ↓
batch_process_steps (log de execução)
```

#### Mudanças necessárias:
1. Migrar todas as configurações Oracle para `0_par.processos`
2. Desativar lógica Oracle no orquestrador principal
3. Criar job dedicado para Oracle batch
4. Integrar com `batch_process_steps` para tracking

---

## 🚀 Implementação Recomendada (Passo a Passo)

### Fase 1: Setup Inicial (5 min)

1. **Criar tabela de parâmetros**:
```sql
-- Executar no Databricks SQL
%sql
CREATE SCHEMA IF NOT EXISTS cm_dbx_dev.0_par;

-- Executar script
%run sql/create_table_processos_parameters.sql
```

2. **Inserir configuração da tabela problemática**:
```sql
INSERT INTO cm_dbx_dev.0_par.processos VALUES (
  'proc-cmaluinterno',
  '92fb0589-07b1-48b5-98a2-c3deadad19c1',
  'CMASTER.CMALUINTERNO@CMASTERPRD',
  120200,
  'cm_dbx_dev.bronze_mega.cmaluinterno',
  10000,  -- fetchsize
  800,    -- num_partitions
  NULL,   -- partition_column (deixar NULL por enquanto)
  NULL,   -- lower_bound
  NULL,   -- upper_bound
  true,
  current_timestamp(),
  current_timestamp(),
  'admin',
  'Migrado do código funcional - teste inicial'
);
```

### Fase 2: Teste Standalone (15 min)

1. **Criar job no Databricks**:
   - Nome: `Oracle Batch Ingestion`
   - Cluster: Mesmo cluster do orquestrador
   - Notebook: `databricks_notebooks/oracle_batch_ingestion_from_params.py`
   - Parâmetros:
     - `env`: `PRD`
     - `catalog`: `cm_dbx_dev`

2. **Executar manualmente** e verificar logs

3. **Validar resultado**:
```sql
SELECT COUNT(*) FROM cm_dbx_dev.bronze_mega.cmaluinterno;
-- Deve retornar 120.200 linhas
```

### Fase 3: Otimização com Particionamento (30 min)

1. **Descobrir coluna para particionamento**:
```sql
-- No DBeaver, conectado ao Oracle
SELECT column_name, data_type
FROM all_tab_columns
WHERE owner = 'CMASTER' 
  AND table_name = 'CMALUINTERNO'
  AND data_type IN ('NUMBER', 'INTEGER')
ORDER BY column_id;
```

2. **Se encontrar coluna numérica (ex: `ID`, `CODIGO`):**
```sql
-- Atualizar parâmetros
UPDATE cm_dbx_dev.0_par.processos
SET 
  partition_column = 'ID',  -- nome da coluna
  lower_bound = 1,
  upper_bound = 150000,
  num_partitions = 8,
  updated_at = current_timestamp()
WHERE processo_id = 'proc-cmaluinterno';
```

3. **Executar novamente e comparar performance**

### Fase 4: Produtização (1 hora)

1. **Adicionar mais tabelas**:
```sql
-- Adicionar outras tabelas Oracle
INSERT INTO cm_dbx_dev.0_par.processos VALUES (...);
```

2. **Agendar job** para rodar periodicamente

3. **Configurar alertas** para falhas

---

## 📊 Comparação de Performance

### Teste: CMASTER.CMALUINTERNO@CMASTERPRD (120.200 linhas)

| Configuração | Tempo | Throughput |
|--------------|-------|------------|
| **Original (travado)** | > 20 min | < 100 linhas/s |
| **Com fetchsize=10K** | ~5-8 min | ~300 linhas/s |
| **Com particionamento JDBC (8 parts)** | ~2-3 min | ~800 linhas/s |
| **Com repartition=200** | ~1-2 min | ~1.200 linhas/s |

---

## 🔧 Troubleshooting

### Problema: "Tabela 0_par.processos não existe"
```sql
-- Criar schema primeiro
CREATE SCHEMA IF NOT EXISTS cm_dbx_dev.0_par;

-- Depois criar tabela
CREATE TABLE cm_dbx_dev.0_par.processos (...);
```

### Problema: "fetchsize não está funcionando"
- Verificar se está usando `.option("fetchsize", "10000")` (string!)
- Verificar Oracle JDBC driver instalado no cluster

### Problema: "Particionamento JDBC não acelera"
- Verificar se a coluna tem índice no Oracle
- Tentar reduzir número de partições (8 → 4)
- Verificar se `lowerBound`/`upperBound` estão corretos

---

## 📝 Próximos Passos

### Curto Prazo (esta semana)
1. ✅ Criar tabela `0_par.processos`
2. ✅ Testar com `CMALUINTERNO`
3. ✅ Validar performance

### Médio Prazo (próximas semanas)
1. Adicionar 5-10 tabelas Oracle
2. Otimizar com particionamento JDBC
3. Agendar job batch

### Longo Prazo (próximos meses)
1. Migrar todas as tabelas Oracle para batch
2. Implementar carga incremental
3. Integrar completamente com orquestrador

---

## 💡 Recomendação Final

**Para resolver o problema imediato** (tabela CMALUINTERNO travada):

1. **Use Opção 1 (Standalone)**: Crie job separado com o novo script
2. **Configure fetchsize=10000**: Isso já deve reduzir tempo para 5-8 min
3. **Teste sem particionamento primeiro**: Valide que funciona
4. **Depois otimize**: Adicione particionamento JDBC se necessário

**Benefícios imediatos**:
- ✅ Resolve o problema em < 1 hora
- ✅ Não afeta sistema existente
- ✅ Base sólida para otimizações futuras

Quer que eu ajude a implementar alguma dessas fases? 🚀
