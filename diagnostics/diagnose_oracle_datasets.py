# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnóstico de Datasets Oracle
# MAGIC 
# MAGIC Este notebook ajuda a identificar e corrigir problemas com datasets Oracle que não conseguem ser lidos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Listar todos os datasets Oracle ativos

# COMMAND ----------

from pyspark.sql import functions as F

# Lista datasets Oracle ativos
oracle_datasets = spark.sql("""
    SELECT 
        dataset_id,
        dataset_name,
        connection_id,
        bronze_table,
        silver_table,
        execution_state,
        last_success_run_id,
        created_at,
        updated_at
    FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
    WHERE source_type = 'ORACLE'
      AND execution_state NOT IN ('DEPRECATED')
    ORDER BY execution_state, dataset_name
""")

display(oracle_datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar últimas execuções com erro

# COMMAND ----------

# Últimas execuções com falha
failed_runs = spark.sql("""
    SELECT 
        bp.run_id,
        bp.dataset_id,
        dc.dataset_name,
        bp.status,
        bp.error_class,
        bp.error_message,
        bp.started_at,
        bp.finished_at,
        SUBSTRING(bp.error_stacktrace, 1, 500) as error_preview
    FROM cm_dbx_dev.ingestion_sys_ops.batch_process bp
    JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control dc
      ON bp.dataset_id = dc.dataset_id
    WHERE bp.status = 'FAILED'
      AND dc.source_type = 'ORACLE'
    ORDER BY bp.started_at DESC
    LIMIT 20
""")

display(failed_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar datasets na fila de execução

# COMMAND ----------

# Datasets na fila (PENDING ou FAILED)
queue_status = spark.sql("""
    SELECT 
        rq.queue_id,
        rq.dataset_id,
        dc.dataset_name,
        rq.status,
        rq.trigger_type,
        rq.priority,
        rq.attempt,
        rq.max_retries,
        rq.last_error_class,
        SUBSTRING(rq.last_error_message, 1, 200) as error_preview,
        rq.requested_at,
        rq.next_retry_at
    FROM cm_dbx_dev.ingestion_sys_ops.run_queue rq
    JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control dc
      ON rq.dataset_id = dc.dataset_id
    WHERE dc.source_type = 'ORACLE'
      AND rq.status IN ('PENDING', 'FAILED', 'CLAIMED', 'RUNNING')
    ORDER BY rq.status, rq.requested_at DESC
""")

display(queue_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar conexões Oracle

# COMMAND ----------

# Listar conexões Oracle
oracle_connections = spark.sql("""
    SELECT 
        connection_id,
        connection_name,
        jdbc_url,
        secret_scope,
        secret_user_key,
        secret_pwd_key,
        approval_status,
        created_at,
        updated_at
    FROM cm_dbx_dev.ingestion_sys_ctrl.connections_oracle
    ORDER BY connection_name
""")

display(oracle_connections)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Testar Conectividade Oracle (Para uma Conexão Específica)

# COMMAND ----------

# IMPORTANTE: Substitua com o connection_id que deseja testar
test_connection_id = "your-connection-id-here"

# Carregar dados da conexão
conn_data = spark.sql(f"""
    SELECT 
        jdbc_url, 
        secret_scope, 
        secret_user_key, 
        secret_pwd_key,
        approval_status
    FROM cm_dbx_dev.ingestion_sys_ctrl.connections_oracle
    WHERE connection_id = '{test_connection_id}'
    LIMIT 1
""").collect()

if not conn_data:
    print(f"❌ Conexão {test_connection_id} não encontrada!")
else:
    conn = conn_data[0]
    jdbc_url = conn["jdbc_url"]
    secret_scope = conn["secret_scope"]
    secret_user_key = conn["secret_user_key"]
    secret_pwd_key = conn["secret_pwd_key"]
    approval_status = conn["approval_status"]
    
    print(f"📋 Connection ID: {test_connection_id}")
    print(f"📋 JDBC URL: {jdbc_url[:50]}...")
    print(f"📋 Approval Status: {approval_status}")
    print(f"📋 Secret Scope: {secret_scope}")
    
    if approval_status != "APPROVED":
        print(f"⚠️  ATENÇÃO: Conexão não está APPROVED (status={approval_status})")
    
    try:
        # Recuperar credenciais
        user = dbutils.secrets.get(secret_scope, secret_user_key)
        pwd = dbutils.secrets.get(secret_scope, secret_pwd_key)
        print(f"✅ Credenciais recuperadas com sucesso")
        print(f"   - User length: {len(user)}")
        print(f"   - Password length: {len(pwd)}")
        
        # Testar conexão básica
        print("\n🔍 Testando conexão Oracle...")
        test_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "(SELECT 1 AS test_col FROM dual) t")
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        result = test_df.collect()
        print(f"✅ Conexão Oracle OK! Resultado: {result}")
        
        # Listar tabelas do schema CMASTER
        print("\n🔍 Listando tabelas do schema CMASTER...")
        tables_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "(SELECT owner, table_name, num_rows FROM all_tables WHERE owner = 'CMASTER' ORDER BY table_name) t")
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        print(f"✅ Total de tabelas encontradas no schema CMASTER: {tables_df.count()}")
        display(tables_df.limit(100))
        
    except Exception as e:
        print(f"❌ ERRO ao testar conexão: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificar se uma tabela específica existe

# COMMAND ----------

# IMPORTANTE: Substitua com connection_id e nome da tabela que deseja verificar
test_connection_id = "your-connection-id-here"
test_table_name = "CMASTER.CMALUINTERNO"  # Formato: OWNER.TABLE_NAME

# Carregar dados da conexão
conn_data = spark.sql(f"""
    SELECT 
        jdbc_url, 
        secret_scope, 
        secret_user_key, 
        secret_pwd_key
    FROM cm_dbx_dev.ingestion_sys_ctrl.connections_oracle
    WHERE connection_id = '{test_connection_id}'
    LIMIT 1
""").collect()

if conn_data:
    conn = conn_data[0]
    jdbc_url = conn["jdbc_url"]
    
    try:
        user = dbutils.secrets.get(conn["secret_scope"], conn["secret_user_key"])
        pwd = dbutils.secrets.get(conn["secret_scope"], conn["secret_pwd_key"])
        
        # Parse table name
        parts = test_table_name.split(".")
        if len(parts) == 2:
            owner, table = parts[0].upper(), parts[1].upper()
        else:
            owner, table = "CMASTER", test_table_name.upper()
        
        print(f"🔍 Verificando existência da tabela: {owner}.{table}")
        
        # Query ALL_TABLES
        check_query = f"(SELECT owner, table_name, num_rows, last_analyzed FROM all_tables WHERE owner = '{owner}' AND table_name = '{table}') t"
        
        check_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", check_query)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        results = check_df.collect()
        
        if len(results) > 0:
            print(f"✅ Tabela ENCONTRADA!")
            print(f"   Owner: {results[0]['OWNER']}")
            print(f"   Table: {results[0]['TABLE_NAME']}")
            print(f"   Rows: {results[0]['NUM_ROWS']}")
            print(f"   Last Analyzed: {results[0]['LAST_ANALYZED']}")
            
            # Tentar ler alguns registros
            print(f"\n🔍 Tentando ler primeiras linhas...")
            data_df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"{owner}.{table}")
                .option("user", user)
                .option("password", pwd)
                .option("driver", "oracle.jdbc.OracleDriver")
                .load()
            )
            
            print(f"✅ Leitura OK! Total de colunas: {len(data_df.columns)}")
            display(data_df.limit(5))
            
        else:
            print(f"❌ Tabela NÃO encontrada ou usuário sem permissão SELECT!")
            print(f"\n💡 Sugestões:")
            print(f"   1. Verificar se a tabela existe no Oracle:")
            print(f"      SELECT * FROM all_tables WHERE table_name LIKE '%{table}%'")
            print(f"   2. Conceder permissão (como CMASTER ou DBA):")
            print(f"      GRANT SELECT ON {owner}.{table} TO {user}")
            
    except Exception as e:
        print(f"❌ ERRO: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Corrigir Dataset Names Incorretos
# MAGIC 
# MAGIC Se você encontrar datasets com nomes incorretos (ex: contendo @DBLINK ou caracteres inválidos), use este comando para corrigir:

# COMMAND ----------

# EXEMPLO: Corrigir um dataset_name
# CUIDADO: Ajuste os valores antes de executar!

update_dataset_id = "seu-dataset-id-aqui"
correct_dataset_name = "CMASTER.NOME_CORRETO_TABELA"

# Descomente para executar:
# spark.sql(f"""
#     UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
#     SET 
#         dataset_name = '{correct_dataset_name}',
#         updated_at = current_timestamp(),
#         updated_by = 'diagnostics_notebook'
#     WHERE dataset_id = '{update_dataset_id}'
# """)
# 
# print(f"✅ Dataset {update_dataset_id} atualizado para {correct_dataset_name}")
