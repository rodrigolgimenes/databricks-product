# Databricks notebook source
# DIAGNÓSTICO: Por que a tabela bronze não está atualizando?
# 
# Problema: O log do orchestrator diz que escreveu dados na tabela bronze,
# mas quando verificamos no Catalog Explorer, a última update foi 2 meses atrás.
#
# Hipóteses possíveis:
# 1. O orchestrator está escrevendo em uma tabela DIFERENTE (wrong catalog/schema)
# 2. O write está falhando silenciosamente mas o log não mostra
# 3. O Catalog Explorer está mostrando metadata desatualizado
# 4. O orchestrator está fazendo .write() mas o DataFrame está vazio
# 5. Problema de permissões causando write em location temporário

# MAGIC %md
# MAGIC ## 1. Verificar qual tabela o orchestrator PENSA que está escrevendo

# COMMAND ----------

CATALOG = "cm_dbx_dev"
DATASET_NAME = "cmaluinterno"  # ou "CMALUINTERNO"

# Buscar dataset_control
df_dataset = spark.sql(f"""
SELECT 
    dataset_id,
    dataset_name,
    bronze_table,
    silver_table,
    execution_state,
    last_success_run_id,
    updated_at
FROM {CATALOG}.ingestion_sys_ctrl.dataset_control
WHERE UPPER(dataset_name) = UPPER('{DATASET_NAME}')
LIMIT 1
""")

display(df_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar últimas execuções REAIS deste dataset

# COMMAND ----------

# Pegar o dataset_id
dataset_row = df_dataset.collect()
if not dataset_row:
    print(f"❌ Dataset '{DATASET_NAME}' NÃO ENCONTRADO em dataset_control!")
    dbutils.notebook.exit("DATASET_NOT_FOUND")

dataset_id = dataset_row[0]['dataset_id']
bronze_table = dataset_row[0]['bronze_table']
last_run_id = dataset_row[0]['last_success_run_id']

print(f"✅ Dataset ID: {dataset_id}")
print(f"✅ Bronze Table (configurada): {bronze_table}")
print(f"✅ Last Success Run ID: {last_run_id}")

# COMMAND ----------

# Buscar últimas execuções no batch_process
df_runs = spark.sql(f"""
SELECT 
    run_id,
    status,
    started_at,
    finished_at,
    bronze_row_count,
    silver_row_count,
    error_class,
    error_message
FROM {CATALOG}.ingestion_sys_ops.batch_process
WHERE dataset_id = '{dataset_id}'
ORDER BY started_at DESC
LIMIT 10
""")

print("\n📊 Últimas 10 execuções:")
display(df_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar table_details - O QUE FOI REALMENTE ESCRITO?

# COMMAND ----------

df_details = spark.sql(f"""
SELECT 
    detail_id,
    run_id,
    layer,
    table_name,
    operation,
    started_at,
    finished_at,
    row_count,
    status,
    error_message
FROM {CATALOG}.ingestion_sys_ops.batch_process_table_details
WHERE dataset_id = '{dataset_id}'
  AND layer = 'BRONZE'
ORDER BY started_at DESC
LIMIT 20
""")

print("\n📋 Detalhes das escritas Bronze (últimas 20):")
display(df_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. INSPEÇÃO DIRETA: Metadata real da tabela Bronze

# COMMAND ----------

# Verificar se a tabela existe
try:
    print(f"\n🔍 Inspecionando tabela: {bronze_table}")
    
    # DESCRIBE DETAIL mostra metadata detalhado Delta
    df_detail = spark.sql(f"DESCRIBE DETAIL {bronze_table}")
    display(df_detail)
    
    print("\n✅ Tabela encontrada!")
    
    # Pegar location e last modified
    detail_row = df_detail.collect()[0]
    location = detail_row['location']
    last_modified = detail_row['lastModified']
    num_files = detail_row['numFiles']
    size_bytes = detail_row['sizeInBytes']
    
    print(f"\n📍 Location: {location}")
    print(f"🕐 Last Modified: {last_modified}")
    print(f"📁 Num Files: {num_files}")
    print(f"💾 Size: {size_bytes} bytes")
    
except Exception as e:
    print(f"\n❌ ERRO ao descrever tabela: {e}")
    print("\n🔍 Tentando buscar na metastore...")
    
    try:
        tables = spark.sql(f"SHOW TABLES IN {CATALOG}.bronze_mega LIKE '{DATASET_NAME}'")
        display(tables)
    except Exception as e2:
        print(f"❌ Também falhou: {e2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. VERIFICAÇÃO: Count e últimos registros

# COMMAND ----------

try:
    # Count total
    count = spark.table(bronze_table).count()
    print(f"\n📊 Total de registros na tabela: {count:,}")
    
    # Últimos 5 registros
    print(f"\n📄 Últimos 5 registros (sem ordenação):")
    df_sample = spark.table(bronze_table).limit(5)
    display(df_sample)
    
    # Se houver coluna de timestamp, verificar o range de datas
    columns = spark.table(bronze_table).columns
    print(f"\n📋 Colunas disponíveis: {columns}")
    
    # Tentar encontrar colunas de data/timestamp
    date_cols = [c for c in columns if any(x in c.lower() for x in ['date', 'time', 'dt', 'created', 'updated', 'modified'])]
    print(f"\n🕐 Colunas de data encontradas: {date_cols}")
    
    if date_cols:
        for col in date_cols[:3]:  # Verificar primeiras 3 colunas de data
            try:
                df_date_range = spark.sql(f"""
                    SELECT 
                        MIN({col}) as min_{col},
                        MAX({col}) as max_{col},
                        COUNT(*) as total_rows
                    FROM {bronze_table}
                """)
                print(f"\n📅 Range de {col}:")
                display(df_date_range)
            except:
                print(f"⚠️  Não foi possível verificar range de {col}")
                
except Exception as e:
    print(f"\n❌ ERRO ao ler tabela: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. VERIFICAÇÃO DELTA: Transaction Log

# COMMAND ----------

try:
    # Verificar histórico de transações Delta
    print(f"\n📜 Histórico de transações Delta (últimas 20):")
    df_history = spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 20")
    display(df_history)
    
    # Análise: quando foi a ÚLTIMA transação WRITE?
    history_rows = df_history.collect()
    if history_rows:
        last_write = None
        for row in history_rows:
            if row['operation'] in ['WRITE', 'CREATE OR REPLACE TABLE', 'MERGE', 'UPDATE', 'DELETE']:
                last_write = row
                break
        
        if last_write:
            print(f"\n✅ Última operação de escrita:")
            print(f"   Operation: {last_write['operation']}")
            print(f"   Timestamp: {last_write['timestamp']}")
            print(f"   User: {last_write['userName'] if 'userName' in last_write.asDict() else 'N/A'}")
            print(f"   Operation Metrics: {last_write['operationMetrics'] if 'operationMetrics' in last_write.asDict() else 'N/A'}")
        else:
            print("\n⚠️  Nenhuma operação de escrita encontrada no histórico!")
            
except Exception as e:
    print(f"\n❌ ERRO ao verificar histórico: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. DIAGNÓSTICO FINAL

# COMMAND ----------

print("=" * 80)
print("DIAGNÓSTICO COMPLETO")
print("=" * 80)

print(f"\n1. Dataset ID: {dataset_id}")
print(f"2. Bronze Table (configurada): {bronze_table}")
print(f"3. Last Success Run ID: {last_run_id}")

print("\n🔍 VERIFIQUE:")
print("   a) Se a 'lastModified' em DESCRIBE DETAIL é RECENTE")
print("   b) Se o DESCRIBE HISTORY mostra operações WRITE recentes")
print("   c) Se o bronze_row_count em batch_process é > 0")
print("   d) Se há ERROS em batch_process_table_details")

print("\n🎯 HIPÓTESES:")
print("   ✓ Se lastModified é ANTIGA mas logs mostram execução recente:")
print("     → Orchestrator pode estar escrevendo em tabela ERRADA")
print("     → Verificar se há mais de uma tabela com nome similar")
print("   ✓ Se lastModified é RECENTE:")
print("     → Catalog Explorer pode estar com cache desatualizado")
print("     → Tentar REFRESH no Catalog ou usar SQL para verificar")
print("   ✓ Se bronze_row_count é 0:")
print("     → DataFrame vazio sendo escrito (problema na origem Oracle)")
print("   ✓ Se há erro em table_details:")
print("     → Write falhou mas orchestrator não tratou corretamente")

print("\n💡 AÇÕES RECOMENDADAS:")
print("   1. Executar DESCRIBE HISTORY manualmente e verificar última operação")
print("   2. Comparar bronze_table em dataset_control com table_name em table_details")
print("   3. Verificar se há permissões de write na location")
print("   4. Se necessário, fazer REFRESH TABLE para atualizar metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. AÇÃO CORRETIVA (se necessário)

# COMMAND ----------

# Se você descobrir que é problema de metadata cache:
# spark.sql(f"REFRESH TABLE {bronze_table}")

# Se descobrir que o orchestrator está escrevendo em local errado:
# Verificar variable substitution no código do orchestrator

print("\n✅ Diagnóstico concluído! Analise os resultados acima.")
