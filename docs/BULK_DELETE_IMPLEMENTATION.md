# Exclusão em Massa de Datasets 🗑️

## ✅ Implementação Completa

A funcionalidade de exclusão em massa foi implementada com **múltiplas camadas de segurança** para evitar exclusões acidentais.

## 🎯 Objetivo

Permitir que o usuário:
1. Selecione um ou múltiplos datasets na tela de listagem
2. Clique em "Excluir" na barra de ações
3. **Confirme explicitamente** digitando "EXCLUIR"
4. Opcionalmente escolha se quer dropar as tabelas Bronze/Silver

## 🛡️ Camadas de Segurança Implementadas

### 1. **Confirmação Textual Obrigatória**
- Usuário **DEVE** digitar "EXCLUIR" (maiúsculas) para habilitar o botão
- Input muda para borda verde quando texto correto é digitado
- Botão permanece desabilitado até confirmação

### 2. **Avisos Visuais Claros**
```
⚠️ Importante:
• As tabelas Bronze e Silver NÃO serão excluídas automaticamente
• Apenas os registros de configuração serão removidos
• Histórico de execuções será mantido
```

### 3. **Opção Separada para Dropar Tabelas**
- **Checkbox opt-in** com aviso vermelho (PERIGOSO)
- Por padrão: tabelas são **preservadas**
- Se marcado: tabelas Bronze + Silver são dropadas do Databricks

### 4. **Feedback Detalhado**
- Alert mostra quantos foram excluídos com sucesso
- Alert mostra quantos falharam
- Erros são logados no console para debugging

## 📁 Arquivos Modificados

### Frontend

**`frontend/src/pages/Datasets.tsx`**
- ✅ Estados para modal de exclusão (showDeleteModal, deleteConfirmText, etc.)
- ✅ Função `handleBulkDelete()` que processa exclusões
- ✅ Botão "Excluir" na bulk action bar (variant="destructive")
- ✅ Modal de confirmação com:
  - Avisos em amarelo (tabelas não são dropadas)
  - Input de confirmação "EXCLUIR"
  - Checkbox vermelho "Dropar tabelas Bronze/Silver"
  - Botões cancelar/excluir

### Backend

**`src/portalRoutes.js`** (já existente)
- ✅ Endpoint `/api/portal/datasets/:datasetId` (DELETE)
- ✅ Parâmetros: `confirm_name`, `drop_tables`
- ✅ Validação: nome do dataset deve corresponder

**`frontend/src/lib/api.ts`** (já existente)
- ✅ Função `deleteDataset(id, confirmName, dropTables)`

## 🎨 UX Implementado

### Barra de Ações (quando há seleção)

```
┌──────────────────────────────────────────────┐
│ 3 selecionados                                │
│ [✓ Executar] [↓ CSV] [✗ Excluir] [✕]        │
└──────────────────────────────────────────────┘
```

### Modal de Exclusão

```
┌─────────────────────────────────────────────────┐
│ ✗ Excluir 3 dataset(s)?                         │
├─────────────────────────────────────────────────┤
│ Esta ação não pode ser desfeita. Os datasets    │
│ serão removidos da ferramenta de ingestão e     │
│ orquestração.                                   │
│                                                 │
│ ┌─────────────────────────────────────────────┐ │
│ │ ⚠️ Importante:                              │ │
│ │ • As tabelas Bronze e Silver NÃO serão      │ │
│ │   excluídas automaticamente                 │ │
│ │ • Apenas os registros de configuração       │ │
│ │   serão removidos                           │ │
│ │ • Histórico de execuções será mantido       │ │
│ └─────────────────────────────────────────────┘ │
│                                                 │
│ Para confirmar, digite EXCLUIR abaixo:          │
│ ┌─────────────────────────────────────────────┐ │
│ │ Digite EXCLUIR para confirmar               │ │
│ └─────────────────────────────────────────────┘ │
│                                                 │
│ ┌─────────────────────────────────────────────┐ │
│ │ ☐ Excluir também as tabelas Bronze e       │ │
│ │   Silver (PERIGOSO)                         │ │
│ │                                             │ │
│ │   Se marcado, as tabelas bronze_table e    │ │
│ │   silver_table serão dropadas              │ │
│ │   permanentemente do Databricks.           │ │
│ └─────────────────────────────────────────────┘ │
│                                                 │
│               [Cancelar]  [✗ Excluir 3 ...]    │
└─────────────────────────────────────────────────┘
```

## ⚙️ Comportamento Detalhado

### Modo 1: Exclusão Padrão (Segura)

**Checkbox desmarcado** (padrão):
```sql
-- Apenas remove da dataset_control
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = 'uuid';

-- Tabelas permanecem intactas
-- cm_dbx_dev.bronze_mega.tabela_x (PRESERVADA)
-- cm_dbx_dev.silver_mega.tabela_x (PRESERVADA)

-- Histórico de execuções permanece
-- batch_process, run_queue (logs históricos mantidos)
```

**Vantagens**:
- ✅ Dados preservados
- ✅ Pode recriar dataset com mesmo nome
- ✅ Tabelas podem ser reutilizadas
- ✅ Auditoria completa permanece

### Modo 2: Exclusão Completa (Perigosa)

**Checkbox marcado** (opt-in):
```sql
-- Remove da dataset_control
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = 'uuid';

-- Dropa tabelas do Databricks
DROP TABLE IF EXISTS cm_dbx_dev.bronze_mega.tabela_x;
DROP TABLE IF EXISTS cm_dbx_dev.silver_mega.tabela_x;

-- Histórico ainda é mantido
```

**Quando usar**:
- ⚠️ Quando tem certeza que dados não serão mais necessários
- ⚠️ Para limpeza de ambiente dev/test
- ⚠️ **NUNCA** em produção sem backup

## 🧪 Como Testar

### Teste 1: Exclusão Padrão (Sem Dropar Tabelas)

1. Abrir http://localhost:3010/datasets
2. Selecionar 2-3 datasets de teste
3. Clicar botão vermelho "Excluir"
4. **Verificar**: Modal abre com avisos amarelos
5. **Verificar**: Botão "Excluir" está desabilitado
6. Digitar "EXCLUIR" no input
7. **Verificar**: Botão se habilita
8. **Verificar**: Checkbox "Dropar tabelas" está desmarcado
9. Clicar "Excluir N dataset(s)"
10. **Verificar**: Alert confirma exclusão
11. **Verificar SQL**: Datasets removidos de `dataset_control`
    ```sql
    SELECT * FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
    WHERE dataset_id IN ('uuid1', 'uuid2');
    -- Resultado: 0 linhas
    ```
12. **Verificar Databricks**: Tabelas ainda existem
    ```sql
    SHOW TABLES IN cm_dbx_dev.bronze_mega;
    SHOW TABLES IN cm_dbx_dev.silver_mega;
    -- Tabelas ainda aparecem
    ```

### Teste 2: Exclusão com Drop de Tabelas

1. Selecionar 1 dataset de teste
2. Clicar "Excluir"
3. Digitar "EXCLUIR"
4. **Marcar** checkbox "Excluir também as tabelas Bronze e Silver"
5. **Verificar**: Warning vermelho aparece
6. Confirmar exclusão
7. **Verificar SQL**: Dataset removido
8. **Verificar Databricks**: Tabelas foram dropadas
    ```sql
    SHOW TABLES IN cm_dbx_dev.bronze_mega LIKE 'tabela_teste';
    -- Resultado: 0 linhas (tabela foi dropada)
    ```

### Teste 3: Validação de Confirmação

1. Selecionar datasets
2. Clicar "Excluir"
3. Tentar digitar "excluir" (minúsculas)
4. **Verificar**: Botão permanece desabilitado
5. Digitar "EXCLUI" (incompleto)
6. **Verificar**: Botão permanece desabilitado
7. Digitar "EXCLUIR" (correto)
8. **Verificar**: Botão habilita + borda verde no input

### Teste 4: Cancelamento

1. Selecionar datasets
2. Clicar "Excluir"
3. Digitar "EXCLUIR"
4. Marcar checkbox "Dropar tabelas"
5. Clicar "Cancelar"
6. **Verificar**: Modal fecha
7. **Verificar**: Estado resetado (input limpo, checkbox desmarcado)
8. **Verificar**: Seleção mantida
9. Abrir modal novamente
10. **Verificar**: Input está vazio novamente

### Teste 5: Exclusão com Falhas

1. Criar dataset "test_delete_1" no banco
2. Remover manualmente do Databricks (simular inconsistência)
3. Selecionar "test_delete_1" + outros válidos
4. Excluir com checkbox marcado
5. **Verificar**: Alert mostra:
    ```
    Exclusão concluída!
    ✓ 2 dataset(s) excluído(s)
    ✗ 1 falha(s)
    
    Verifique o console para detalhes.
    ```
6. **Verificar Console**: Log mostra erro do dataset problem:
    ```javascript
    [DELETE] Resultados: [
      { dataset_id: 'uuid', status: 'ERROR', error: 'Table not found' }
    ]
    ```

## 🔍 Debugging

### Ver Datasets Existentes

```sql
SELECT dataset_id, dataset_name, execution_state, bronze_table, silver_table
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
ORDER BY created_at DESC;
```

### Verificar Tabelas Existentes

```sql
-- Bronze
SHOW TABLES IN cm_dbx_dev.bronze_mega;

-- Silver
SHOW TABLES IN cm_dbx_dev.silver_mega;
```

### Ver Histórico de Exclusões (via logs)

```sql
-- Auditoria de mudanças de estado (não captura exclusões)
SELECT * FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_state_changes
WHERE dataset_id = 'uuid'
ORDER BY changed_at DESC;

-- Batch process histórico (permanece após exclusão)
SELECT run_id, status, started_at, finished_at
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE dataset_id = 'uuid'
ORDER BY started_at DESC;
```

### Troubleshooting

**Problema**: Botão "Excluir" não habilita
- **Verificar**: Texto digitado é exatamente "EXCLUIR" (maiúsculas)
- **Verificar**: Não há espaços antes/depois

**Problema**: Exclusão falha com "NOT_FOUND"
- **Verificar**: Dataset existe em `dataset_control`
- **Verificar**: dataset_id é UUID válido

**Problema**: Tabelas não foram dropadas (checkbox marcado)
- **Verificar**: Backend recebeu `drop_tables: true`
- **Verificar**: Usuário Databricks tem permissão DROP TABLE
- **Ver logs**: Backend deve mostrar tentativa de drop

**Problema**: Erro "Name confirmation mismatch"
- **Causa**: Backend valida que `confirm_name` == `dataset_name`
- **Verificar**: Frontend está passando `dataset_name` correto
- **Fix**: No código, linha 273: `api.deleteDataset(ds.dataset_id, ds.dataset_name, ...)`

## 📊 Comparação de Comportamento

| Aspecto | Sem Checkbox (Padrão) | Com Checkbox Marcado |
|---------|----------------------|----------------------|
| **dataset_control** | ❌ Removido | ❌ Removido |
| **schema_versions** | ✅ Mantido | ✅ Mantido |
| **batch_process** | ✅ Mantido | ✅ Mantido |
| **run_queue** | ✅ Mantido | ✅ Mantido |
| **Tabela Bronze** | ✅ Mantida | ❌ Dropada |
| **Tabela Silver** | ✅ Mantida | ❌ Dropada |
| **Reversível?** | ✅ Sim (recriar dataset) | ⚠️ Parcial (dados perdidos) |

## ⚠️ Avisos Importantes

### Para Usuários

1. **Exclusão Padrão é Segura**
   - Remove configuração, mantém dados
   - Pode recriar dataset apontando para mesmas tabelas
   - Histórico de execuções permanece para auditoria

2. **Dropar Tabelas é Irreversível**
   - Dados perdidos permanentemente
   - Não há backup automático
   - Use apenas em ambientes dev/test

3. **Histórico Sempre Permanece**
   - Logs de execução (`batch_process`) nunca são excluídos
   - Permite auditoria mesmo após exclusão
   - Ocupa espaço no banco (fazer limpeza periódica manual se necessário)

### Para Administradores

1. **Permissões Databricks**
   - Service principal deve ter `DROP TABLE` se usuários usarem checkbox
   - Considere não dar essa permissão em produção

2. **Backup Recomendado**
   - Fazer backup de `dataset_control` antes de exclusões em massa
   - Fazer backup de tabelas Bronze/Silver antes de dropar

3. **Limpeza Periódica**
   - Considere limpar `batch_process` antigos (> 90 dias)
   - Avaliar se tabelas órfãs (sem dataset) devem ser dropadas

## 🚀 Próximos Passos (Roadmap)

1. ✅ **Implementado**: Exclusão em massa com confirmação
2. ⏳ **Soft Delete**: Adicionar estado "DELETED" em vez de dropar registro
3. ⏳ **Lixeira**: Recuperar datasets excluídos em até 30 dias
4. ⏳ **Auditoria**: Tabela específica para log de exclusões
5. ⏳ **Bulk Restore**: Restaurar múltiplos datasets de uma vez

---

**Implementado por**: Warp AI Agent  
**Data**: 2026-02-21  
**Status**: ✅ Completo e pronto para teste  
**Segurança**: 🛡️ Múltiplas camadas de confirmação
