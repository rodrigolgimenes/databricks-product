-- ============================================
-- Testar acesso à Allowlist
-- ============================================

-- Tentar visualizar allowlist atual
-- Se funcionar, você pode ver mas não editar
SHOW ARTIFACT ALLOWLISTS;

-- ============================================
-- RESULTADO ESPERADO:
-- ============================================
-- Se ERRO de permissão → Você NÃO pode gerenciar allowlists
-- Se SUCESSO → Você pode VER a allowlist (mas talvez não editar)

-- ============================================
-- Para workspace admins SEM permissão de allowlist:
-- SOLUÇÃO: Usar Cluster Single User
-- ============================================
