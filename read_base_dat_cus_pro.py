# Databricks notebook source
# MAGIC %md
# MAGIC # Leitura de BASE_DAT_CUS_PRO - Múltiplos Owners
# MAGIC 
# MAGIC Este notebook lê a tabela `BASE_DAT_CUS_PRO` de diferentes owners Oracle:
# MAGIC - CMASTER.BASE_DAT_CUS_PRO
# MAGIC - RHMETA.BASE_DAT_CUS_PRO  
# MAGIC - CIVIL_10465_RHP.BASE_DAT_CUS_PRO

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Configuração Oracle
# MAGIC 
# MAGIC Define o ambiente (PRD ou HML) e configura as credenciais via Secret Scope

# COMMAND ----------

# Widget para selecionar ambiente
dbutils.widgets.dropdown(
    "oracle_env",
    "HML",                 # default HML para testes
    ["PRD", "HML"],
    "Ambiente Oracle"
)

env = dbutils.widgets.get("oracle_env").upper()
print(f"🔧 Ambiente selecionado: {env}")

# COMMAND ----------

import os

# Secret Scope
SCOPE = "civilmaster-oracle"

# Buscar credenciais baseado no ambiente
if env == "PRD":
    ORACLE_USER = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_USER")
    ORACLE_PASSWORD = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_SENHA")
    ORACLE_DBLINK = "CMASTERPRD"
elif env == "HML":
    ORACLE_USER = dbutils.secrets.get(SCOPE, "HML_MEGA_DB_USER")
    ORACLE_PASSWORD = dbutils.secrets.get(SCOPE, "HML_MEGA_DB_SENHA")
    ORACLE_DBLINK = "CMASTER"
else:
    raise ValueError(f"Ambiente inválido: {env}")

# Configurações de conexão Oracle
ORACLE_HOST = "dbconnect.megaerp.online"
ORACLE_PORT = "4221"
ORACLE_SERVICE_NAME = "xepdb1"

# JDBC URL
JDBC_URL = f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"

# Exportar variáveis para uso no notebook
os.environ["ORACLE_HOST"] = ORACLE_HOST
os.environ["ORACLE_PORT"] = ORACLE_PORT
os.environ["ORACLE_SERVICE_NAME"] = ORACLE_SERVICE_NAME
os.environ["ORACLE_USER"] = ORACLE_USER
os.environ["ORACLE_PASSWORD"] = ORACLE_PASSWORD
os.environ["ORACLE_DBLINK"] = ORACLE_DBLINK
os.environ["JDBC_URL"] = JDBC_URL

print(f"✅ Configuração Oracle {env} carregada")
print(f"➡ HOST={ORACLE_HOST}, PORT={ORACLE_PORT}")
print(f"➡ JDBC_URL={JDBC_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Função Helper para Ler do Oracle

# COMMAND ----------

def read_oracle_table(owner, table_name, use_dblink=False):
    """
    Lê uma tabela do Oracle via JDBC.
    
    Args:
        owner: Nome do owner/schema (ex: CMASTER, RHMETA, CIVIL_10465_RHP)
        table_name: Nome da tabela (ex: BASE_DAT_CUS_PRO)
        use_dblink: Se True, usa @DBLINK na query
    
    Returns:
        DataFrame com os dados
    """
    if use_dblink:
        # Formato: (SELECT * FROM OWNER.TABLE@DBLINK) alias
        dbtable = f"(SELECT * FROM {owner}.{table_name}@{ORACLE_DBLINK}) src"
    else:
        # Formato direto: OWNER.TABLE
        dbtable = f"{owner}.{table_name}"
    
    print(f"📊 Lendo: {dbtable}")
    print(f"🔗 URL: {JDBC_URL}")
    
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", dbtable)
        .option("user", ORACLE_USER)
        .option("password", ORACLE_PASSWORD)
        .option("driver", "oracle.jdbc.OracleDriver")
        .load()
    )
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Leitura das Tabelas

# COMMAND ----------

# Ler CMASTER.BASE_DAT_CUS_PRO
print("=" * 80)
print("CMASTER.BASE_DAT_CUS_PRO")
print("=" * 80)

try:
    df_cmaster = read_oracle_table("CMASTER", "BASE_DAT_CUS_PRO", use_dblink=False)
    count_cmaster = df_cmaster.count()
    print(f"✅ Registros: {count_cmaster:,}")
    display(df_cmaster.limit(10))
except Exception as e:
    print(f"❌ Erro: {e}")
    df_cmaster = None

# COMMAND ----------

# Ler RHMETA.BASE_DAT_CUS_PRO
print("=" * 80)
print("RHMETA.BASE_DAT_CUS_PRO")
print("=" * 80)

try:
    df_rhmeta = read_oracle_table("RHMETA", "BASE_DAT_CUS_PRO", use_dblink=False)
    count_rhmeta = df_rhmeta.count()
    print(f"✅ Registros: {count_rhmeta:,}")
    display(df_rhmeta.limit(10))
except Exception as e:
    print(f"❌ Erro: {e}")
    df_rhmeta = None

# COMMAND ----------

# Ler CIVIL_10465_RHP.BASE_DAT_CUS_PRO
print("=" * 80)
print("CIVIL_10465_RHP.BASE_DAT_CUS_PRO")
print("=" * 80)

try:
    df_civil = read_oracle_table("CIVIL_10465_RHP", "BASE_DAT_CUS_PRO", use_dblink=False)
    count_civil = df_civil.count()
    print(f"✅ Registros: {count_civil:,}")
    display(df_civil.limit(10))
except Exception as e:
    print(f"❌ Erro: {e}")
    df_civil = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Resumo dos Resultados

# COMMAND ----------

print("=" * 80)
print("RESUMO")
print("=" * 80)

results = []

if df_cmaster is not None:
    results.append(("CMASTER.BASE_DAT_CUS_PRO", count_cmaster))
    
if df_rhmeta is not None:
    results.append(("RHMETA.BASE_DAT_CUS_PRO", count_rhmeta))
    
if df_civil is not None:
    results.append(("CIVIL_10465_RHP.BASE_DAT_CUS_PRO", count_civil))

if results:
    print("\n✅ Tabelas lidas com sucesso:")
    for table, count in results:
        print(f"  • {table}: {count:,} registros")
else:
    print("❌ Nenhuma tabela foi lida com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ (Opcional) Comparação de Schemas

# COMMAND ----------

# Comparar schemas das 3 tabelas
if df_cmaster is not None:
    print("Schema CMASTER:")
    df_cmaster.printSchema()

if df_rhmeta is not None:
    print("\nSchema RHMETA:")
    df_rhmeta.printSchema()

if df_civil is not None:
    print("\nSchema CIVIL_10465_RHP:")
    df_civil.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ (Opcional) Salvar no Unity Catalog

# COMMAND ----------

# Exemplo: Salvar CMASTER.BASE_DAT_CUS_PRO no Unity Catalog
# Descomente para executar

# if df_cmaster is not None:
#     target_table = "cm_dbx_dev.bronze_mega.base_dat_cus_pro_cmaster"
#     print(f"💾 Salvando em: {target_table}")
#     
#     df_cmaster.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .saveAsTable(target_table)
#     
#     print(f"✅ Tabela {target_table} criada com sucesso!")

