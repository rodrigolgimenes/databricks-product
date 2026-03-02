# Melhorias do Sistema de Ingestão Oracle

Este documento detalha as melhorias implementadas para resolver os problemas críticos do orchestrador e dashboard de monitoramento.

## 🎯 Problemas Resolvidos

### 1. ❌ Erros de Permissão no Schema Bronze
**Problema:** Usuário não tem permissão CREATE TABLE no schema `cm_dbx_dev.bronze_mega`, impedindo a criação da tabela "centro de custo" e outras.

**Solução:**
- ✅ Criado script `grant_orchestrator_permissions.py` para conceder todas as permissões necessárias
- ✅ Script verifica e aplica: USAGE, SELECT, MODIFY, CREATE TABLE, CREATE VIEW
- ✅ Inclui verificação de grants existentes e concessão para tabelas individuais

**Como usar:**
```bash
# 1. Edite o script e configure o PRINCIPAL_NAME
# 2. Execute no Databricks com privilégios de admin
python mcp-databricks-server/grant_orchestrator_permissions.py
```

### 2. 🔄 Erros de Schema Mismatch no Delta
**Problema:** Colunas adicionadas na origem Oracle causam falha ao escrever na tabela Delta Bronze existente.

**Solução:**
- ✅ Habilitado schema evolution automático com `.option("mergeSchema", "true")`
- ✅ Delta agora aceita novas colunas automaticamente sem quebrar a execução
- ✅ Log explícito informando que schema evolution está ativo

**Localização:** `governed_ingestion_orchestrator.py` linha ~630

### 3. 📋 Output do Notebook Termina Abruptamente com JSON
**Problema:** Execução termina com raw JSON dump, difícil de ler e interpretar status de sucesso/falha.

**Solução:**
- ✅ Adicionado resumo formatado e legível com emojis e seções claras
- ✅ Mostra contadores por status (SUCCEEDED, FAILED, PENDING, etc.)
- ✅ Lista resultados da execução com status visual
- ✅ Exibe próximos pending jobs e failed jobs com erros resumidos
- ✅ JSON completo movido para o final, após resumo human-readable

**Localização:** `governed_ingestion_orchestrator.py` linhas 1193-1256

Exemplo de output:
```
================================================================================
RESUMO DA EXECUÇÃO DO ORCHESTRATOR
================================================================================

📅 Data/Hora: 2025-02-20T15:30:00Z
⚙️  Catalog: cm_dbx_dev
🎯 Max Items: 5

📊 Status da Fila (run_queue):
  ❌ FAILED: 3
  🟡 PENDING: 12
  🟢 SUCCEEDED: 45

⏱️  Pending Elegíveis (agora): 12
✅ Jobs Claimed nesta execução: 5

📄 Resultados da Execução:

  [1] ✅ Dataset: CON_CENTRO_CUSTO
      Run ID: abc123...
      Status: SUCCEEDED

  [2] ❌ Dataset: CON_FORNECEDOR
      Run ID: def456...
      Status: FAILED
      Erro: PERMISSION_DENIED: User does not have CREATE TABLE...
```

### 4. 📈 Dashboard de Monitoramento sem Visibilidade de Steps
**Problema:** Usuários não conseguem ver passo a passo da execução de cada dataset. Dashboard original era apenas demo de configuração de cargas.

**Solução:**
- ✅ Criado `app_monitoring.py` - Dashboard completo com 3 tabs
- ✅ **Tab 1 - Monitoramento:** Visão geral da fila, métricas de status, execuções recentes, jobs com falha
- ✅ **Tab 2 - Histórico de Execuções:** Drill-down por dataset mostrando:
  - Todas as execuções históricas do dataset
  - Cada execução expansível mostrando resumo (run_id, status, duração, linhas processadas)
  - **Passo a passo detalhado** lendo da tabela `batch_process_steps`
  - Cada step mostra: fase, status, progresso, duração, mensagem, detalhes JSON
- ✅ **Tab 3 - Configuração:** Mantém funcionalidade original de parametrização de cargas
- ✅ Conecta com Databricks SQL via variáveis de ambiente (funciona sem login)
- ✅ Modo demo quando Databricks não configurado

**Como usar:**
```bash
# Configure variáveis de ambiente
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_CATALOG="cm_dbx_dev"

# Execute o novo app
cd python_streamlit_demo
streamlit run app_monitoring.py
```

**Dependências necessárias:**
```bash
pip install streamlit pandas databricks-sql-connector
```

## 🚀 Próximos Passos

1. **Conceder Permissões:**
   - Execute o script `grant_orchestrator_permissions.py` configurando o principal correto
   - Verifique se todas as permissões foram concedidas com sucesso

2. **Re-executar Orchestrator:**
   - Com schema evolution e permissões corrigidas, a tabela "centro de custo" deve ser criada
   - Log agora mostra claramente se execução foi bem-sucedida

3. **Deploy do Dashboard:**
   - Configure as credenciais de conexão do Databricks
   - Deploy como Databricks App ou execute localmente
   - Usuários podem clicar no dataset "CON_CENTRO_CUSTO" e ver todo o histórico de execuções

4. **Monitoramento Contínuo:**
   - Use Tab 1 para overview rápido de status
   - Use Tab 2 para investigar falhas específicas com drill-down nos steps
   - Steps logs incluem timing, progress e erros detalhados

## 📊 Estrutura de Dados

O sistema utiliza as seguintes tabelas:

- `cm_dbx_dev.ingestion_sys_ops.run_queue` - Fila de execuções
- `cm_dbx_dev.ingestion_sys_ops.batch_process` - Processos batch executados
- `cm_dbx_dev.ingestion_sys_ops.batch_process_steps` - **Steps detalhados** (criado automaticamente)
- `cm_dbx_dev.ingestion_sys_ops.batch_process_table_details` - Detalhes de tabelas escritas
- `cm_dbx_dev.ingestion_sys_ctrl.dataset_control` - Controle de datasets
- `cm_dbx_dev.ingestion_sys_ctrl.connections_oracle` - Conexões Oracle
- `cm_dbx_dev.ingestion_sys_ctrl.schema_versions` - Versões de schema

## 🔍 Troubleshooting

### Tabela "centro de custo" ainda não existe?

1. Verifique logs do orchestrator:
   - Procure por `[BRONZE:ORACLE]` no output
   - Se ver "PERMISSION_DENIED", rode o script de permissões

2. Verifique no dashboard:
   - Tab 1 → Failed Jobs
   - Clique no dataset para ver erro completo

3. Verifique no histórico:
   - Tab 2 → Selecione "CON_CENTRO_CUSTO"
   - Expanda a última execução
   - Veja os steps para identificar onde falhou

### Schema mismatch errors?

Se ainda ocorrer erro de schema após a correção:
- Verifique se está usando a versão atualizada do `governed_ingestion_orchestrator.py`
- Confirme que a linha 631 contém `.option("mergeSchema", "true")`
- Considere dropar e recriar a tabela bronze manualmente se necessário:
  ```sql
  DROP TABLE IF EXISTS cm_dbx_dev.bronze_mega.CON_CENTRO_CUSTO;
  ```

### Dashboard não conecta?

- Verifique as variáveis de ambiente
- Teste conexão manualmente:
  ```python
  from databricks import sql
  conn = sql.connect(server_hostname="...", http_path="...", access_token="...")
  ```
- Verifique permissões do token no workspace
- Dashboard funciona em modo demo mesmo sem conexão (só não mostra dados reais)

## ✅ Checklist de Validação

- [ ] Script de permissões executado com sucesso
- [ ] Orchestrator executa sem erros de permissão
- [ ] Schema evolution aceita novas colunas
- [ ] Tabela "centro de custo" criada em `cm_dbx_dev.bronze_mega`
- [ ] Output do notebook mostra resumo formatado
- [ ] Dashboard conecta ao Databricks
- [ ] Tab 2 do dashboard mostra steps detalhados por execução
- [ ] Usuários conseguem ver histórico completo sem login no front-end

## 📝 Arquivos Modificados

1. `databricks_notebooks/governed_ingestion_orchestrator.py`
   - Linha 631: Adicionado `.option("mergeSchema", "true")`
   - Linhas 1193-1256: Novo formatted summary output

2. `mcp-databricks-server/grant_orchestrator_permissions.py` (NOVO)
   - Script completo para grant de permissões
   - Corrigido: Removido CREATE VIEW (não aplicável no metastore 1.0)
   - Corrigido: Lógica de verificação de grants mais flexível

3. `python_streamlit_demo/app_monitoring.py` (NOVO)
   - Dashboard completo Streamlit com monitoramento e histórico detalhado

4. `public/v2.html` (MODIFICADO)
   - Adicionada seção "Histórico Detalhado por Dataset" na view de monitoramento
   - KPIs de status da fila (Succeeded, Pending, Running, Failed)
   - Seletor de dataset com drill-down completo
   - Seções para fila, execuções recentes e jobs com falha

5. `public/v2.js` (MODIFICADO)
   - Função `loadMonitor()` completamente reescrita
   - `loadQueueStats()` - Carrega estatísticas da fila para KPIs
   - `loadMonitorDatasets()` - Popula seletor de datasets
   - `loadDatasetExecutions()` - Carrega histórico de execuções por dataset
   - `loadExecutionSteps()` - Carrega steps detalhados de cada execução (batch_process_steps)
   - `createExecutionCard()` e `createStepItem()` - Renderização de UI
   - `getStatusBadge()` - Formatação de badges de status com emojis
   - `loadQueueTable()`, `loadRecentBatchProcesses()`, `loadFailedJobs()` - Tabelas de monitoramento

## 🎓 Recursos Adicionais

- [Databricks Delta Lake Schema Evolution](https://docs.databricks.com/delta/schema-evolution.html)
- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html)
