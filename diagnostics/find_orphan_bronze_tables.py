# Databricks notebook source
# ENCONTRAR TABELAS BRONZE ÓRFÃS
# 
# Objetivo: Listar todas as tabelas bronze que existem fisicamente
# mas não têm registro em dataset_control

CATALOG = "cm_dbx_dev"
BRONZE_SCHEMA = "bronze_mega"

# MAGIC %md
# MAGIC ## 1. Listar todas as tabelas bronze físicas

# COMMAND ----------

df_physical = spark.sql(f"""
    SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}
""")

print(f"📊 Tabelas físicas em {CATALOG}.{BRONZE_SCHEMA}:")
display(df_physical)

physical_tables = [row['tableName'] for row in df_physical.collect()]
print(f"\n✅ Total: {len(physical_tables)} tabelas")

# MAGIC %md
# MAGIC ## 2. Listar datasets registrados

# COMMAND ----------

df_registered = spark.sql(f"""
    SELECT 
        dataset_id,
        dataset_name,
        bronze_table,
        execution_state
    FROM {CATALOG}.ingestion_sys_ctrl.dataset_control
    WHERE bronze_table LIKE '{CATALOG}.{BRONZE_SCHEMA}.%'
""")

print(f"\n📋 Datasets registrados apontando para {BRONZE_SCHEMA}:")
display(df_registered)

registered_tables = set()
for row in df_registered.collect():
    bronze_table = row['bronze_table']
    # Extrair apenas o nome da tabela (último componente)
    table_name = bronze_table.split('.')[-1].lower()
    registered_tables.add(table_name)

print(f"\n✅ Total de datasets registrados: {len(registered_tables)}")

# MAGIC %md
# MAGIC ## 3. Encontrar órfãos

# COMMAND ----------

orphans = []
for table in physical_tables:
    if table.lower() not in registered_tables:
        orphans.append(table)

print(f"\n{'='*80}")
print(f"TABELAS ÓRFÃS (existem mas não estão no dataset_control)")
print(f"{'='*80}\n")

if orphans:
    print(f"❌ Encontradas {len(orphans)} tabelas órfãs:\n")
    for i, table in enumerate(orphans, 1):
        # Pegar info da tabela
        try:
            df_detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{BRONZE_SCHEMA}.{table}")
            detail = df_detail.collect()[0]
            last_modified = detail['lastModified']
            num_files = detail['numFiles']
            size_mb = detail['sizeInBytes'] / (1024*1024)
            
            print(f"{i}. {table}")
            print(f"   Last Modified: {last_modified}")
            print(f"   Files: {num_files}, Size: {size_mb:.2f} MB")
            print()
        except:
            print(f"{i}. {table} (erro ao obter detalhes)")
            print()
    
    print(f"\n💡 Ação recomendada:")
    print(f"   - Se essas tabelas devem ser gerenciadas pelo orchestrator:")
    print(f"     Execute register_cmaluinterno_dataset.py para cada uma")
    print(f"   - Se são tabelas antigas/obsoletas:")
    print(f"     Considere fazer DROP ou mover para um schema 'archived'")
else:
    print("✅ Nenhuma tabela órfã encontrada!")
    print("   Todas as tabelas bronze têm registro em dataset_control")

# MAGIC %md
# MAGIC ## 4. Resumo

# COMMAND ----------

print("\n" + "="*80)
print("RESUMO")
print("="*80)
print(f"\n📊 Tabelas físicas: {len(physical_tables)}")
print(f"📋 Datasets registrados: {len(registered_tables)}")
print(f"❌ Tabelas órfãs: {len(orphans)}")

if orphans:
    print(f"\n⚠️  ATENÇÃO: {len(orphans)} tabela(s) órfã(s) encontrada(s)!")
    print(f"   Essas tabelas existem mas o orchestrator não as gerencia.")
else:
    print(f"\n✅ Tudo OK! Todas as tabelas estão registradas.")
