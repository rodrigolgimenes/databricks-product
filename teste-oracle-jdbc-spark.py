# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Teste de Conexão Oracle via JDBC Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuração do Oracle JDBC Driver
# MAGIC 
# MAGIC **⚠️ IMPORTANTE: Clusters Shared requerem Allowlist**
# MAGIC 
# MAGIC ### Opção 1: Adicionar à Allowlist (Recomendado - requer Account Admin)
# MAGIC ```sql
# MAGIC -- Execute no Databricks SQL Editor como Account/Metastore Admin:
# MAGIC ALTER ARTIFACT ALLOWLIST ADD 'com.oracle.database.jdbc:ojdbc8:21.9.0.0';
# MAGIC ```
# MAGIC 
# MAGIC ### Opção 2: Usar Cluster Single User (Solução Imediata)
# MAGIC 1. Crie um novo cluster com **Access Mode: Single User**
# MAGIC 2. Atribua ao seu usuário
# MAGIC 3. Instale a library: `com.oracle.database.jdbc:ojdbc8:21.9.0.0`
# MAGIC 4. Execute este notebook nesse cluster
# MAGIC 
# MAGIC ### Verificação do Driver
# MAGIC Execute a célula abaixo para verificar se o driver está disponível:

# COMMAND ----------

# Verificar se Oracle JDBC driver está disponível
try:
    # Tentar carregar a classe do driver Oracle
    from py4j.java_gateway import java_import
    java_import(spark._jvm, "oracle.jdbc.driver.OracleDriver")
    print("✅ Oracle JDBC Driver está disponível!")
    print("   Você pode prosseguir com o notebook.")
except Exception as e:
    print("❌ Oracle JDBC Driver NÃO está disponível")
    print(f"   Erro: {e}")
    print("\n📋 SOLUÇÕES:")
    print("\n   1️⃣ CLUSTERS SHARED (requer admin):")
    print("      Execute como Account Admin:")
    print("      ALTER ARTIFACT ALLOWLIST ADD 'com.oracle.database.jdbc:ojdbc8:21.9.0.0';")
    print("\n   2️⃣ CLUSTER SINGLE USER (solução imediata):")
    print("      - Compute → Create Cluster")
    print("      - Access Mode: Single User")
    print("      - Libraries → Install: com.oracle.database.jdbc:ojdbc8:21.9.0.0")
    print("      - Use esse cluster para executar o notebook")
    print("\n   3️⃣ CLUSTER ATUAL (se Single User):")
    print("      - Compute → seu cluster → Libraries")
    print("      - Install new → Maven")
    print("      - Coordinates: com.oracle.database.jdbc:ojdbc8:21.9.0.0")
    print("      - Install e aguarde conclusão")

# COMMAND ----------

# Configuração Oracle
import os

# Widget para ambiente
try:
    dbutils.widgets.dropdown("oracle_env", "PRD", ["PRD", "HML"], "Ambiente")
except:
    pass

env = dbutils.widgets.get("oracle_env").upper()

# Secret Scope
SCOPE = "civilmaster-oracle"

# Credenciais
if env == "PRD":
    ORACLE_USER = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_USER")
    ORACLE_PASSWORD = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_SENHA")
else:
    ORACLE_USER = dbutils.secrets.get(SCOPE, "HML_MEGA_DB_USER")
    ORACLE_PASSWORD = dbutils.secrets.get(SCOPE, "HML_MEGA_DB_SENHA")

# Variáveis de conexão
ORACLE_HOST = "dbconnect.megaerp.online"
ORACLE_PORT = "4221"
ORACLE_SERVICE_NAME = "xepdb1"
ORACLE_OWNER = "CMASTER"
ORACLE_DBLINK = "CMASTERPRD" if env == "PRD" else None

JDBC_URL = f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"

print(f"✅ Oracle {env} configurado")
print(f"HOST={ORACLE_HOST}")
print(f"JDBC_URL={JDBC_URL}")

# COMMAND ----------

# Lista de tabelas Oracle para validar
test_tables = """
GLO_AGENTES
CON_CENTRO_CUSTO
cmvw_desp_total
"""

# Normalizar nomes
tables = []
for line in test_tables.strip().splitlines():
    name = line.strip().upper()
    if name:
        tables.append(name)

print(f"📋 Testando {len(tables)} tabelas:")
for t in tables:
    print(f"  - {t}")

# COMMAND ----------

from pyspark.sql import Row
import time

def validate_table_jdbc(table_name):
    # Construir referência completa
    if "." in table_name:
        table_ref = table_name
    else:
        table_ref = f"{ORACLE_OWNER}.{table_name}"
    
    # Adicionar DBLINK se necessário
    if ORACLE_DBLINK and "@" not in table_ref:
        table_ref = f"{table_ref}@{ORACLE_DBLINK}"
    
    start_time = time.time()
    
    try:
        # Tentar ler 1 linha via JDBC
        df = spark.read \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", f"(SELECT * FROM {table_ref} WHERE ROWNUM <= 1)") \
            .option("user", ORACLE_USER) \
            .option("password", ORACLE_PASSWORD) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        
        # Forçar execução
        count = df.count()
        elapsed = time.time() - start_time
        
        return Row(
            original_name=table_name,
            resolved_ref=table_ref,
            status="OK",
            row_count=count,
            elapsed_sec=round(elapsed, 2),
            error=""
        )
        
    except Exception as e:
        elapsed = time.time() - start_time
        error_msg = str(e)
        
        # Extrair mensagem Oracle se existir
        if "ORA-" in error_msg:
            import re
            match = re.search(r'ORA-\d+: [^\n]+', error_msg)
            if match:
                error_msg = match.group(0)
        
        return Row(
            original_name=table_name,
            resolved_ref=table_ref,
            status="ERROR",
            row_count=0,
            elapsed_sec=round(elapsed, 2),
            error=error_msg[:200]
        )

print("✅ Função validate_table_jdbc() criada")

# COMMAND ----------

results = []
total_start = time.time()

print(f"🔄 Testando {len(tables)} tabelas via JDBC Spark...\n")

for i, table in enumerate(tables, start=1):
    result = validate_table_jdbc(table)
    results.append(result)
    
    status_emoji = "✅" if result.status == "OK" else "❌"
    print(f"[{i}/{len(tables)}] {result.resolved_ref}")
    print(f"  └─ {status_emoji} {result.status} ({result.elapsed_sec}s)")
    if result.error:
        print(f"     Error: {result.error[:100]}")
    print()

total_elapsed = time.time() - total_start
print(f"⏱️  Tempo total: {total_elapsed:.1f}s")

# COMMAND ----------

# Criar DataFrame com resultados
df_results = spark.createDataFrame(results)

# Exibir ordenado por status
display(df_results.orderBy("status", "original_name"))

# COMMAND ----------

# Contar sucessos e falhas
ok_count = sum(1 for r in results if r.status == "OK")
error_count = sum(1 for r in results if r.status == "ERROR")

print(f"📊 Resumo da Validação:")
print(f"   ✅ Sucesso: {ok_count}/{len(results)}")
print(f"   ❌ Erro:    {error_count}/{len(results)}")
print()

# Listar tabelas OK
if ok_count > 0:
    print("✅ Tabelas acessíveis:")
    for r in results:
        if r.status == "OK":
            print(f"   - {r.resolved_ref}")
    print()

# Listar erros
if error_count > 0:
    print("❌ Tabelas com erro:")
    for r in results:
        if r.status == "ERROR":
            print(f"   - {r.resolved_ref}")
            if "ORA-00942" in r.error:
                print(f"     ⚠️  TABELA NÃO EXISTE NO ORACLE")
            elif "ORA-" in r.error:
                print(f"     {r.error[:100]}")
            print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Listar Todas as Tabelas do Schema CMASTER

# COMMAND ----------

# Consultar catálogo Oracle para ver todas as tabelas disponíveis
query = f"""(
SELECT 
    table_name,
    tablespace_name,
    num_rows,
    'TABLE' as object_type
FROM all_tables 
WHERE owner = '{ORACLE_OWNER}'
UNION ALL
SELECT 
    view_name as table_name,
    NULL as tablespace_name,
    NULL as num_rows,
    'VIEW' as object_type
FROM all_views
WHERE owner = '{ORACLE_OWNER}'
ORDER BY object_type, table_name
)"""

try:
    df_catalog = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", query) \
        .option("user", ORACLE_USER) \
        .option("password", ORACLE_PASSWORD) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    
    print(f"📚 Objetos disponíveis no schema {ORACLE_OWNER}:")
    print(f"Total: {df_catalog.count()} objetos\n")
    
    display(df_catalog)
    
except Exception as e:
    print(f"❌ Erro ao consultar catálogo: {str(e)[:200]}")

# COMMAND ----------

# Verificar se as tabelas problemáticas existem no catálogo
print("🔎 Verificando tabelas específicas no catálogo:\n")

for table in tables:
    exists = df_catalog.filter(f"table_name = '{table}'").count() > 0
    if exists:
        obj_type = df_catalog.filter(f"table_name = '{table}'").select("object_type").first()[0]
        print(f"   ✅ {table} - EXISTE ({obj_type})")
    else:
        print(f"   ❌ {table} - NÃO ENCONTRADA")
        # Buscar similares
        if len(table) >= 3:
            similar = df_catalog.filter(f"table_name LIKE '%{table[:5]}%'").select("table_name", "object_type").collect()
            if similar:
                print(f"      📝 Objetos similares:")
                for row in similar[:5]:
                    print(f"         - {row.table_name} ({row.object_type})")
        print()
