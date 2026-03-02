-- ============================================
-- Script de Diagnóstico Oracle - Tabelas
-- ============================================
-- Execute este script conectado no Oracle com o usuário usado pelo Databricks

-- 1. Verificar se a tabela CMALUINTERNO existe
SELECT 
    owner,
    table_name,
    tablespace_name,
    status,
    num_rows,
    last_analyzed
FROM all_tables
WHERE table_name = 'CMALUINTERNO'
ORDER BY owner;

-- 2. Verificar permissões do usuário atual na tabela
SELECT 
    grantee,
    owner,
    table_name,
    privilege
FROM all_tab_privs
WHERE table_name = 'CMALUINTERNO'
  AND grantee = USER;

-- 3. Listar todas as tabelas do schema CMASTER que o usuário tem acesso
SELECT 
    owner,
    table_name,
    num_rows,
    last_analyzed
FROM all_tables
WHERE owner = 'CMASTER'
ORDER BY table_name;

-- 4. Verificar se há sinônimos para esta tabela
SELECT 
    owner,
    synonym_name,
    table_owner,
    table_name,
    db_link
FROM all_synonyms
WHERE synonym_name = 'CMALUINTERNO'
   OR table_name = 'CMALUINTERNO';

-- 5. Buscar tabelas com nome similar (caso haja erro de digitação)
SELECT 
    owner,
    table_name,
    num_rows
FROM all_tables
WHERE owner = 'CMASTER'
  AND (
    table_name LIKE '%CMALU%'
    OR table_name LIKE '%INTERNO%'
    OR table_name LIKE '%MALUI%'
  )
ORDER BY table_name;

-- 6. Verificar usuário e privilégios atuais
SELECT 
    USER as current_user,
    SYS_CONTEXT('USERENV', 'SESSION_USER') as session_user,
    SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') as current_schema
FROM dual;

-- 7. Verificar privilégios de sistema do usuário
SELECT 
    grantee,
    privilege
FROM dba_sys_privs
WHERE grantee = USER
  AND privilege LIKE '%SELECT%'
UNION
SELECT 
    grantee,
    privilege
FROM user_sys_privs
WHERE privilege LIKE '%SELECT%';

-- ============================================
-- SOLUÇÃO: Se a tabela existe mas não aparece
-- ============================================
-- Execute como usuário proprietário (CMASTER) ou DBA:

-- Opção 1: Grant direto
-- GRANT SELECT ON CMASTER.CMALUINTERNO TO <seu_usuario_databricks>;

-- Opção 2: Grant via role
-- GRANT SELECT ON CMASTER.CMALUINTERNO TO <role_name>;
-- GRANT <role_name> TO <seu_usuario_databricks>;

-- Opção 3: Criar sinônimo público (requer privilégios DBA)
-- CREATE PUBLIC SYNONYM CMALUINTERNO FOR CMASTER.CMALUINTERNO;
-- GRANT SELECT ON CMASTER.CMALUINTERNO TO PUBLIC;

-- ============================================
-- Verificar Dataset Names incorretos
-- ============================================
-- Execute no Databricks para listar dataset_names problemáticos:
/*
SELECT 
    dataset_id,
    dataset_name,
    source_type,
    execution_state,
    bronze_table
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE source_type = 'ORACLE'
  AND execution_state NOT IN ('PAUSED', 'DEPRECATED')
ORDER BY dataset_name;
*/
