-- ============================================
-- Verificar Permissões Unity Catalog
-- ============================================

-- 1. Ver seu usuário e roles atuais
SELECT current_user() AS meu_usuario;

-- 2. Verificar se você é Account Admin
SHOW CURRENT ROLES;

-- 3. Verificar privilégios no Metastore
SHOW GRANT ON METASTORE;

-- 4. Verificar se você pode gerenciar allowlists
-- (Se este comando funcionar sem erro, você tem permissão)
DESCRIBE ARTIFACT ALLOWLIST;

-- 5. Ver allowlist atual (se tiver permissão)
SHOW ARTIFACT ALLOWLISTS;

-- ============================================
-- Se você tiver permissão, adicione o Oracle JDBC:
-- ============================================

-- ATENÇÃO: Só execute se os comandos acima funcionarem!
-- ALTER ARTIFACT ALLOWLIST ADD 'com.oracle.database.jdbc:ojdbc8:21.9.0.0';

-- Para verificar se foi adicionado:
-- SHOW ARTIFACT ALLOWLISTS;
