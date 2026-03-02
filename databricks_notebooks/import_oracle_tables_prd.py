# Databricks notebook source
# MAGIC %md
# MAGIC # Importação de Tabelas Oracle - PRD (DBLINK)
# MAGIC 
# MAGIC Importa e testa tabelas Oracle usando DBLINK @CMASTERPRD:
# MAGIC - CMASTER.BASE_DAT_CUS_PRO
# MAGIC - CMASTER.CMALUINTERNO
# MAGIC - CIVIL_10465_RHP.R010SIT
# MAGIC - CIVIL_10465_RHP.R018CCU
# MAGIC - CIVIL_10465_RHP.R024CAR
# MAGIC - CIVIL_10465_RHP.R034FUN
# MAGIC - CIVIL_10465_RHP.R038AFA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Configuração Oracle PRD

# COMMAND ----------

import os
from pyspark.sql import functions as F

# Ambiente fixo: PRD (porque está usando @CMASTERPRD)
env = "PRD"

# Secret Scope
SCOPE = "civilmaster-oracle"

# Credenciais PRD
ORACLE_USER = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_USER")
ORACLE_PASSWORD = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_SENHA")
ORACLE_DBLINK = "CMASTERPRD"

# Configurações de conexão
ORACLE_HOST = "dbconnect.megaerp.online"
ORACLE_PORT = "4221"
ORACLE_SERVICE_NAME = "xepdb1"

# JDBC URL
JDBC_URL = f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"

print(f"✅ Configuração Oracle PRD carregada")
print(f"➡ HOST={ORACLE_HOST}, PORT={ORACLE_PORT}")
print(f"➡ DBLINK={ORACLE_DBLINK}")
print(f"➡ JDBC_URL={JDBC_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Lista de Tabelas para Importar

# COMMAND ----------

# Tabelas a serem testadas/importadas
TABLES = [
    # (owner, table_name, target_name)
    ("CMASTER", "BASE_DAT_CUS_PRO", "base_dat_cus_pro_cmaster"),
    ("CMASTER", "CMALUINTERNO", "cmaluinterno"),
    ("CIVIL_10465_RHP", "R010SIT", "r010sit"),
    ("CIVIL_10465_RHP", "R018CCU", "r018ccu"),
    ("CIVIL_10465_RHP", "R024CAR", "r024car"),
    ("CIVIL_10465_RHP", "R034FUN", "r034fun"),
    ("CIVIL_10465_RHP", "R038AFA", "r038afa"),
]

print(f"📋 Total de tabelas a processar: {len(TABLES)}")
for owner, table, target in TABLES:
    print(f"  • {owner}.{table}@{ORACLE_DBLINK} → {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Função para Ler Tabela com DBLINK

# COMMAND ----------

def read_oracle_table_with_dblink(owner, table_name):
    """
    Lê uma tabela Oracle usando DBLINK.
    
    Args:
        owner: Nome do owner/schema (ex: CMASTER, CIVIL_10465_RHP)
        table_name: Nome da tabela (ex: BASE_DAT_CUS_PRO)
    
    Returns:
        DataFrame com os dados
    """
    # Formato com DBLINK: (SELECT * FROM OWNER.TABLE@DBLINK) alias
    dbtable = f"(SELECT * FROM {owner}.{table_name}@{ORACLE_DBLINK}) src"
    
    print(f"\n{'='*80}")
    print(f"📊 Lendo: {owner}.{table_name}@{ORACLE_DBLINK}")
    print(f"{'='*80}")
    
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", dbtable)
            .option("user", ORACLE_USER)
            .option("password", ORACLE_PASSWORD)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        # Informações básicas
        count = df.count()
        cols = len(df.columns)
        
        print(f"✅ Sucesso!")
        print(f"   Registros: {count:,}")
        print(f"   Colunas: {cols}")
        print(f"   Schema:")
        df.printSchema()
        
        return df, count, None
        
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return None, 0, str(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Teste RÁPIDO - Primeiras 10 Linhas

# COMMAND ----------

# Testar apenas as primeiras linhas de cada tabela (rápido)
print("🚀 TESTE RÁPIDO - Amostra de 10 registros de cada tabela")
print("="*80)

test_results = []

for owner, table_name, target_name in TABLES:
    try:
        # Query otimizada: apenas 10 linhas
        dbtable = f"(SELECT * FROM {owner}.{table_name}@{ORACLE_DBLINK} WHERE ROWNUM <= 10) src"
        
        df_sample = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", dbtable)
            .option("user", ORACLE_USER)
            .option("password", ORACLE_PASSWORD)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        count = df_sample.count()
        cols = len(df_sample.columns)
        
        test_results.append({
            "table": f"{owner}.{table_name}@{ORACLE_DBLINK}",
            "status": "✅ OK",
            "sample_rows": count,
            "columns": cols,
            "error": None
        })
        
        print(f"✅ {owner}.{table_name:<20} | {count} linhas | {cols} colunas")
        
    except Exception as e:
        test_results.append({
            "table": f"{owner}.{table_name}@{ORACLE_DBLINK}",
            "status": "❌ ERRO",
            "sample_rows": 0,
            "columns": 0,
            "error": str(e)
        })
        print(f"❌ {owner}.{table_name:<20} | ERRO: {str(e)[:60]}...")

print("\n" + "="*80)
print(f"Resumo: {sum(1 for r in test_results if r['status'] == '✅ OK')}/{len(test_results)} tabelas acessíveis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Importação COMPLETA (Descomente para executar)

# COMMAND ----------

# ⚠️ DESCOMENTE ESTE CELL PARA IMPORTAR TODAS AS TABELAS
# Esta operação pode demorar dependendo do tamanho das tabelas!

# TARGET_CATALOG = "cm_dbx_dev"
# TARGET_SCHEMA = "bronze_mega"

# import_results = []

# for owner, table_name, target_name in TABLES:
#     print(f"\n{'='*80}")
#     print(f"📦 Importando: {owner}.{table_name}")
#     print(f"{'='*80}")
    
#     try:
#         # Ler tabela completa
#         df, count, error = read_oracle_table_with_dblink(owner, table_name)
        
#         if df is None:
#             import_results.append({
#                 "source": f"{owner}.{table_name}@{ORACLE_DBLINK}",
#                 "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{target_name}",
#                 "status": "❌ ERRO LEITURA",
#                 "rows": 0,
#                 "error": error
#             })
#             continue
        
#         # Salvar no Unity Catalog
#         target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{target_name}"
#         print(f"\n💾 Salvando em: {target_table}")
        
#         df.write \
#             .format("delta") \
#             .mode("overwrite") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(target_table)
        
#         print(f"✅ Tabela {target_table} criada com {count:,} registros!")
        
#         import_results.append({
#             "source": f"{owner}.{table_name}@{ORACLE_DBLINK}",
#             "target": target_table,
#             "status": "✅ SUCESSO",
#             "rows": count,
#             "error": None
#         })
        
#     except Exception as e:
#         print(f"❌ Erro ao salvar: {str(e)}")
#         import_results.append({
#             "source": f"{owner}.{table_name}@{ORACLE_DBLINK}",
#             "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{target_name}",
#             "status": "❌ ERRO ESCRITA",
#             "rows": 0,
#             "error": str(e)
#         })

# # Resumo final
# print("\n" + "="*80)
# print("RESUMO DA IMPORTAÇÃO")
# print("="*80)

# for result in import_results:
#     print(f"\n{result['status']} {result['source']}")
#     print(f"   → {result['target']}")
#     if result['rows'] > 0:
#         print(f"   Registros: {result['rows']:,}")
#     if result['error']:
#         print(f"   Erro: {result['error'][:100]}")

# success = sum(1 for r in import_results if r['status'] == '✅ SUCESSO')
# print(f"\n✅ {success}/{len(import_results)} tabelas importadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Importação INDIVIDUAL - Tabelas Prioritárias

# COMMAND ----------

# Importar apenas as 2 tabelas prioritárias mencionadas
print("🎯 IMPORTAÇÃO PRIORITÁRIA")
print("="*80)

PRIORITY_TABLES = [
    ("CIVIL_10465_RHP", "R038AFA", "r038afa"),
    ("CMASTER", "CMALUINTERNO", "cmaluinterno"),
]

TARGET_CATALOG = "cm_dbx_dev"
TARGET_SCHEMA = "bronze_mega"

for owner, table_name, target_name in PRIORITY_TABLES:
    print(f"\n{'='*80}")
    print(f"📦 Processando: {owner}.{table_name}@{ORACLE_DBLINK}")
    print(f"{'='*80}")
    
    try:
        # Ler tabela
        df, count, error = read_oracle_table_with_dblink(owner, table_name)
        
        if df is None:
            print(f"❌ Erro na leitura. Pulando...")
            continue
        
        # Preview dos dados
        print("\n📋 Amostra dos dados (primeiras 5 linhas):")
        display(df.limit(5))
        
        # Salvar no Unity Catalog
        target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{target_name}"
        print(f"\n💾 Salvando em: {target_table}")
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
        print(f"✅ Tabela {target_table} criada com {count:,} registros!")
        
        # Verificar tabela criada
        df_check = spark.table(target_table)
        print(f"✓ Verificação: {df_check.count():,} registros salvos")
        
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()

print("\n" + "="*80)
print("✅ Importação prioritária concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Consultas de Verificação

# COMMAND ----------

# Verificar tabelas criadas no Unity Catalog
print("📊 Tabelas Bronze criadas no Unity Catalog:")
print("="*80)

try:
    tables = spark.sql(f"SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA}").collect()
    
    for table in tables:
        table_name = table['tableName']
        if any(t[2] in table_name for t in TABLES):
            full_name = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
            count = spark.table(full_name).count()
            print(f"✅ {full_name:<60} | {count:>10,} registros")
            
except Exception as e:
    print(f"❌ Erro ao listar tabelas: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Comparação: Oracle vs Unity Catalog

# COMMAND ----------

# Comparar contagens entre Oracle (origem) e Unity Catalog (destino)
print("🔍 COMPARAÇÃO: Oracle (Origem) vs Unity Catalog (Destino)")
print("="*80)

for owner, table_name, target_name in PRIORITY_TABLES:
    try:
        # Contar na origem (Oracle)
        oracle_query = f"(SELECT COUNT(*) as cnt FROM {owner}.{table_name}@{ORACLE_DBLINK}) src"
        oracle_count = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", oracle_query)
            .option("user", ORACLE_USER)
            .option("password", ORACLE_PASSWORD)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
            .collect()[0]["cnt"]
        )
        
        # Contar no destino (Unity Catalog)
        target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{target_name}"
        try:
            uc_count = spark.table(target_table).count()
            match = "✅" if oracle_count == uc_count else "⚠️"
        except:
            uc_count = 0
            match = "❌ Tabela não encontrada no UC"
        
        print(f"\n{owner}.{table_name}:")
        print(f"  Oracle:        {oracle_count:>10,} registros")
        print(f"  Unity Catalog: {uc_count:>10,} registros")
        print(f"  Status:        {match}")
        
    except Exception as e:
        print(f"\n❌ Erro ao comparar {owner}.{table_name}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Conclusão
# MAGIC 
# MAGIC **Próximos passos:**
# MAGIC 1. ✅ Tabelas testadas e importadas para Bronze
# MAGIC 2. ⏭️ Criar schemas/contratos para promoção Silver
# MAGIC 3. ⏭️ Configurar orchestrator para cargas automáticas
