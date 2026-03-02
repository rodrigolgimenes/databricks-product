-- Verificar se as tabelas existem no schema CMASTER
SELECT 
    table_name,
    owner,
    tablespace_name,
    num_rows
FROM all_tables 
WHERE owner = 'CMASTER'
  AND table_name IN ('CON_CENTRO_CUSTO', 'cmvw_desp_total')
ORDER BY table_name;

-- Verificar views também
SELECT 
    view_name,
    owner
FROM all_views 
WHERE owner = 'CMASTER'
  AND view_name IN ('CON_CENTRO_CUSTO', 'cmvw_desp_total')
ORDER BY view_name;

-- Listar todas as tabelas do schema CMASTER
SELECT table_name 
FROM all_tables 
WHERE owner = 'CMASTER'
ORDER BY table_name;
