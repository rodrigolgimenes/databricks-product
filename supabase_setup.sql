-- ====================================================
-- Script de Setup para Integração Supabase
-- ====================================================
-- Execute este script no Supabase SQL Editor
-- (Dashboard > SQL Editor > New Query)

-- 1. Função para listar tabelas de um schema
CREATE OR REPLACE FUNCTION get_tables_in_schema(schema_name text)
RETURNS TABLE(table_name text, table_type text, row_count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    t.tablename::text as table_name,
    'BASE TABLE'::text as table_type,
    (xpath('/row/c/text()', query_to_xml(
      format('SELECT COUNT(*) as c FROM %I.%I', schema_name, t.tablename),
      false, true, ''
    )))[1]::text::bigint as row_count
  FROM pg_tables t
  WHERE t.schemaname = schema_name
  ORDER BY t.tablename;
END;
$$;

-- 2. Garantir que a função pode ser chamada via RPC
GRANT EXECUTE ON FUNCTION get_tables_in_schema(text) TO anon, authenticated;

-- 3. Verificar quais tabelas existem no schema public
SELECT * FROM get_tables_in_schema('public');

-- Se você tiver dados em outro schema, teste assim:
-- SELECT * FROM get_tables_in_schema('orcamentos_cm');

-- ====================================================
-- Notas:
-- ====================================================
-- Se você ainda não criou nenhuma tabela, o resultado será vazio.
-- Você pode criar uma tabela de teste com:
--
-- CREATE TABLE IF NOT EXISTS public.test_table (
--   id SERIAL PRIMARY KEY,
--   name TEXT NOT NULL,
--   created_at TIMESTAMP DEFAULT NOW()
-- );
--
-- INSERT INTO public.test_table (name) VALUES ('Teste 1'), ('Teste 2');
-- ====================================================
