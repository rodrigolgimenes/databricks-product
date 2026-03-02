# -*- coding: utf-8 -*-
"""
Script de diagnóstico para verificar tabela/view Oracle
Execute este script no Databricks Notebook para diagnosticar o problema ORA-00942
"""

# 1. Verificar conexão Oracle
print("=" * 80)
print("1. TESTANDO CONEXÃO COM ORACLE")
print("=" * 80)

# Substitua com suas credenciais
jdbc_url = "jdbc:oracle:thin:@//seu-host:1521/seu-servico"
connection_properties = {
    "user": "seu_usuario",
    "password": "sua_senha",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

try:
    # Teste simples de conexão
    test_df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT 1 FROM DUAL)",
        properties=connection_properties
    )
    test_df.show()
    print("✅ Conexão com Oracle OK")
except Exception as e:
    print(f"❌ Erro na conexão: {e}")
    raise

# 2. Listar todos os objetos disponíveis no schema CMASTER
print("\n" + "=" * 80)
print("2. LISTANDO OBJETOS NO SCHEMA CMASTER")
print("=" * 80)

try:
    objects_query = """
    (SELECT 
        object_name, 
        object_type, 
        status,
        created,
        last_ddl_time
     FROM all_objects 
     WHERE owner = 'CMASTER' 
     AND object_name LIKE '%DESP%'
     ORDER BY object_type, object_name)
    """
    
    objects_df = spark.read.jdbc(
        url=jdbc_url,
        table=objects_query,
        properties=connection_properties
    )
    
    print(f"\nTotal de objetos encontrados: {objects_df.count()}")
    objects_df.show(100, truncate=False)
    
except Exception as e:
    print(f"❌ Erro ao listar objetos: {e}")

# 3. Buscar especificamente a view cmvw_desp_total
print("\n" + "=" * 80)
print("3. PROCURANDO 'cmvw_desp_total' (case insensitive)")
print("=" * 80)

try:
    search_query = """
    (SELECT 
        owner,
        object_name, 
        object_type, 
        status
     FROM all_objects 
     WHERE UPPER(object_name) = UPPER('cmvw_desp_total'))
    """
    
    search_df = spark.read.jdbc(
        url=jdbc_url,
        table=search_query,
        properties=connection_properties
    )
    
    if search_df.count() > 0:
        print("\n✅ Objeto encontrado!")
        search_df.show(truncate=False)
    else:
        print("\n❌ Objeto 'cmvw_desp_total' NÃO ENCONTRADO em nenhum schema")
        
        # Buscar nomes similares
        print("\n📋 Procurando nomes similares...")
        similar_query = """
        (SELECT 
            owner,
            object_name, 
            object_type
         FROM all_objects 
         WHERE UPPER(object_name) LIKE '%DESP_TOTAL%'
         OR UPPER(object_name) LIKE '%CMVW%')
        """
        
        similar_df = spark.read.jdbc(
            url=jdbc_url,
            table=similar_query,
            properties=connection_properties
        )
        
        print(f"\nObjetos similares encontrados: {similar_df.count()}")
        similar_df.show(100, truncate=False)
        
except Exception as e:
    print(f"❌ Erro na busca: {e}")

# 4. Verificar sinônimos (synonyms)
print("\n" + "=" * 80)
print("4. VERIFICANDO SINÔNIMOS (SYNONYMS)")
print("=" * 80)

try:
    synonyms_query = """
    (SELECT 
        owner,
        synonym_name,
        table_owner,
        table_name,
        db_link
     FROM all_synonyms 
     WHERE UPPER(synonym_name) = UPPER('cmvw_desp_total'))
    """
    
    synonyms_df = spark.read.jdbc(
        url=jdbc_url,
        table=synonyms_query,
        properties=connection_properties
    )
    
    if synonyms_df.count() > 0:
        print("\n✅ Sinônimo encontrado!")
        synonyms_df.show(truncate=False)
    else:
        print("\n❌ Nenhum sinônimo encontrado")
        
except Exception as e:
    print(f"❌ Erro ao verificar sinônimos: {e}")

# 5. Verificar privilégios do usuário atual
print("\n" + "=" * 80)
print("5. VERIFICANDO PRIVILÉGIOS DO USUÁRIO")
print("=" * 80)

try:
    privileges_query = """
    (SELECT 
        grantee,
        owner,
        table_name,
        privilege,
        grantable
     FROM all_tab_privs 
     WHERE UPPER(table_name) = UPPER('cmvw_desp_total'))
    """
    
    privileges_df = spark.read.jdbc(
        url=jdbc_url,
        table=privileges_query,
        properties=connection_properties
    )
    
    if privileges_df.count() > 0:
        print("\n✅ Privilégios encontrados!")
        privileges_df.show(truncate=False)
    else:
        print("\n❌ Nenhum privilégio encontrado para esta tabela/view")
        
except Exception as e:
    print(f"❌ Erro ao verificar privilégios: {e}")

# 6. Listar todas as tabelas/views que o usuário tem acesso no schema CMASTER
print("\n" + "=" * 80)
print("6. TODAS AS TABELAS/VIEWS ACESSÍVEIS NO SCHEMA CMASTER")
print("=" * 80)

try:
    accessible_query = """
    (SELECT 
        owner,
        table_name,
        'TABLE' as object_type
     FROM all_tables 
     WHERE owner = 'CMASTER'
     UNION ALL
     SELECT 
        owner,
        view_name as table_name,
        'VIEW' as object_type
     FROM all_views 
     WHERE owner = 'CMASTER'
     ORDER BY object_type, table_name)
    """
    
    accessible_df = spark.read.jdbc(
        url=jdbc_url,
        table=accessible_query,
        properties=connection_properties
    )
    
    print(f"\nTotal de objetos acessíveis: {accessible_df.count()}")
    accessible_df.show(100, truncate=False)
    
    # Salvar em CSV para análise
    output_path = "/dbfs/tmp/oracle_accessible_objects.csv"
    accessible_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"\n📁 Lista salva em: {output_path}")
    
except Exception as e:
    print(f"❌ Erro ao listar objetos acessíveis: {e}")

print("\n" + "=" * 80)
print("DIAGNÓSTICO CONCLUÍDO")
print("=" * 80)
print("""
PRÓXIMOS PASSOS:
1. Se a tabela/view não foi encontrada:
   - Verifique com o DBA se o nome está correto
   - Verifique se está em outro schema
   - Verifique se precisa de sinônimo

2. Se encontrou objeto similar:
   - Atualize o dataset_name no portal

3. Se encontrou em outro schema:
   - Atualize a consulta para incluir o schema correto
""")
