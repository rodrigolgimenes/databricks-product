# 🔧 Guia de Configuração - Plataforma de Ingestão Governada

## ✅ Status da Configuração

A configuração está **COMPLETA E FUNCIONANDO** ✅

## 📋 Variáveis de Ambiente Configuradas

Arquivo: `.env` na raiz do projeto

```env
# Databricks Configuration
DATABRICKS_TOKEN=<your-databricks-token>
DATABRICKS_HOST=https://dbc-c9eab3b3-1f5f.cloud.databricks.com
DATABRICKS_SQL_WAREHOUSE_ID=4e4f36c4c611f1d3

# Unity Catalog
UC_CATALOG=cm_dbx_dev
GOV_SYS_CTRL_SCHEMA=cm_dbx_dev.ingestion_sys_ctrl
GOV_SYS_OPS_SCHEMA=cm_dbx_dev.ingestion_sys_ops
```

## 🔍 Como Obter o Warehouse ID

1. Acesse o **Databricks**
2. Vá em **SQL Warehouses** (menu lateral)
3. Clique no seu warehouse (ex: "Serverless Starter Warehouse")
4. Na aba **Overview**, você verá:
   - **Name**: Serverless Starter Warehouse
   - **ID**: `4e4f36c4c611f1d3` ← Este é o Warehouse ID!

Ou procure na URL quando estiver visualizando o warehouse:
```
https://xxx.databricks.com/compute/interactive?o=xxx&warehouseId=4e4f36c4c611f1d3
                                                                    ^^^^^^^^^^^^^^^^^^^
```

## 🧪 Testar Conexão

Execute o script de teste:

```powershell
node test-connection.js
```

Você verá:
```
============================================================
[TEST] Teste de Conexão com Databricks
============================================================

[1] Carregando arquivo .env...
   Result: { loaded: true, filePath: 'C:\dev\cm-databricks\.env' }

[2] Variáveis de Ambiente:
   DATABRICKS_HOST: https://dbc-c9eab3b3-1f5f.cloud.databricks.com
   DATABRICKS_TOKEN: ✓ (36 chars)
   DATABRICKS_SQL_WAREHOUSE_ID: 4e4f36c4c611f1d3
   UC_CATALOG: cm_dbx_dev

✅ SUCESSO! Conexão funcionando!
✅ TODOS OS TESTES PASSARAM!
```

## 🚀 Iniciar o Servidor

```powershell
npm start
```

Você verá logs detalhados:
```
============================================================
[SERVER] Inicializando servidor...
[SERVER] CWD: C:\dev\cm-databricks
[SERVER] NODE_ENV: development
============================================================

[PORTAL] Registrando rotas do portal...
[PORTAL] Carregamento de .env: { loaded: true, filePath: 'C:\dev\cm-databricks\.env' }
[PORTAL] Variáveis relevantes do ambiente:
  - DATABRICKS_HOST: ✓
  - DATABRICKS_TOKEN: ✓ (36 chars)
  - DATABRICKS_SQL_WAREHOUSE_ID: 4e4f36c4c611f1d3
  - UC_CATALOG: cm_dbx_dev

[DATABRICKS CONFIG] {
  host: 'https://dbc-c9eab3b3-1f5f.clou...',
  hasToken: true,
  tokenLength: 36,
  warehouseId: '4e4f36c4c611f1d3'
}

============================================================
[SERVER] ✅ Servidor rodando em http://localhost:3000
[SERVER] ✅ Interface V2: http://localhost:3000/v2.html
[SERVER] ✅ Interface V1: http://localhost:3000/index.html
============================================================
```

## 🌐 Acessar as Interfaces

### Interface V2 (Nova - Recomendada)
```
http://localhost:3000/v2.html
```

Características:
- ✨ Design moderno com cards visuais
- 🧭 Navegação por sidebar
- 🎯 Wizard guiado para criar datasets
- 📊 Dashboard com KPIs em tempo real
- 🔍 Busca integrada
- ✅ Sistema de aprovações visual

### Interface V1 (Antiga)
```
http://localhost:3000/index.html
```

## 📊 Recursos do Databricks Disponíveis

Com a configuração atual, você tem acesso a:

### **Catálogos** (4 encontrados):
- `cm_dbx_dev` - Catálogo principal do projeto
- `hive_metastore` - Catálogo legado
- `samples` - Datasets de exemplo
- `system` - Catálogo do sistema

### **Schemas no cm_dbx_dev** (10 encontrados):
- `0_par`
- `bronze` - Camada Bronze
- `bronze_mega`
- `default`
- `information_schema`
- `ingestion_sys` - Sistema de ingestão
- `ingestion_sys_ctrl` - Controle/governança
- `ingestion_sys_ops` - Operações/runtime
- `silver` - Camada Silver
- `silver_mega`

## 🔐 Segurança

### ⚠️ IMPORTANTE: Proteção do Token

O arquivo `.env` contém seu **Personal Access Token (PAT)** do Databricks. 

**NUNCA faça commit deste arquivo no Git!**

O arquivo `.gitignore` já está configurado para ignorar:
```gitignore
# Databricks / local env
.env
.env.*
```

### Renovação do Token

Se precisar renovar o token:

1. Acesse o **Databricks**
2. Vá em **Settings** → **User Settings**
3. Aba **Developer** → **Access Tokens**
4. Clique em **Generate New Token**
5. Copie o token e atualize no `.env`

## 🐛 Troubleshooting

### Erro: "DATABRICKS_SQL_WAREHOUSE_ID não configurado"

**Causa**: Variável vazia ou ausente no `.env`

**Solução**:
1. Verifique o arquivo `.env`
2. Confirme que `DATABRICKS_SQL_WAREHOUSE_ID=4e4f36c4c611f1d3` está presente
3. Reinicie o servidor

### Erro: "Databricks SQL client não configurado. Faltando: DATABRICKS_TOKEN"

**Causa**: Token ausente ou inválido

**Solução**:
1. Verifique se o token está no `.env`
2. Confirme que não há espaços extras
3. Gere um novo token se necessário

### Erro: "HTTP 401 Unauthorized"

**Causa**: Token expirado ou inválido

**Solução**:
1. Gere um novo token no Databricks
2. Atualize o `.env`
3. Reinicie o servidor

### Warehouse não responde

**Causa**: Warehouse pode estar em modo "Auto Stop"

**Solução**:
1. Acesse **SQL Warehouses** no Databricks
2. Verifique se o warehouse está **Running** (verde)
3. Se estiver parado, clique em **Start**
4. Aguarde 1-2 minutos para inicialização

### Logs não aparecem no console

**Solução**: Adicione variável de ambiente
```powershell
$env:DEBUG="*"
npm start
```

## 📝 Logs Detalhados

Todos os componentes agora têm logs detalhados:

### Server.js
- `[SERVER]` - Logs de inicialização e rotas

### Portal Routes
- `[PORTAL]` - Logs de configuração do portal
- `[DATABRICKS CONFIG]` - Logs de configuração do cliente

### Databricks SQL
- `[DATABRICKS]` - Logs de operações SQL
- `[DATABRICKS ERROR]` - Logs de erros

## 🎯 Próximos Passos

1. ✅ Configuração completa
2. ✅ Conexão testada e funcionando
3. ✅ Servidor rodando com logs detalhados
4. ➡️ **Acesse**: `http://localhost:3000/v2.html`
5. ➡️ **Teste**: Criar seu primeiro dataset!

## 📚 Documentação Adicional

- **Interface V2**: `/docs/UI_V2_GUIDE.md`
- **Fluxos Visuais**: `/docs/UI_V2_FLOW.md`
- **Quick Start**: `/QUICK_START_V2.md`
- **Arquitetura**: `/docs/01-architecture/overview.md`

---

## ✨ Status Final

```
✅ Arquivo .env configurado
✅ Token Databricks válido (36 chars)
✅ Warehouse ID configurado (4e4f36c4c611f1d3)
✅ Catálogo cm_dbx_dev acessível
✅ Schemas de sistema disponíveis
✅ Logs detalhados implementados
✅ Script de teste criado
✅ Servidor funcionando
✅ Interface V2 disponível
```

**🎉 TUDO PRONTO PARA USO!**
