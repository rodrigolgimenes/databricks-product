# Integração Supabase - Documentação

## 📋 Visão Geral

Esta documentação descreve a integração do Supabase na plataforma de Ingestão Governada, permitindo que usuários conectem e carreguem tabelas do Supabase (PostgreSQL) para o Databricks.

## ✨ Funcionalidades Implementadas

### 1. **Backend - Cliente Supabase** (`src/supabaseClient.js`)
- ✅ Conexão com Supabase usando credenciais do `.env`
- ✅ Teste de conexão
- ✅ Listagem de schemas disponíveis
- ✅ Listagem de tabelas por schema
- ✅ Obtenção de informações detalhadas de tabelas (row count, colunas)

### 2. **Backend - Rotas API** (`src/portalRoutes.js`)
Novas rotas adicionadas:

```javascript
GET  /api/portal/supabase/test-connection
GET  /api/portal/supabase/schemas
GET  /api/portal/supabase/tables?schema=public
GET  /api/portal/supabase/tables/:tableName/info?schema=public
```

### 3. **Frontend - Componente de Seleção** (`frontend/src/components/SupabaseTableSelector.tsx`)
Interface visual estilo Fivetran com:
- ✅ Teste automático de conexão
- ✅ Seleção de schema (dropdown)
- ✅ Busca/filtro de tabelas
- ✅ Seleção múltipla com checkboxes
- ✅ Visualização de row count
- ✅ Ações em massa (selecionar todas / limpar seleção)
- ✅ Preview visual com informações de cada tabela

### 4. **Frontend - Integração no CreateDataset** (`frontend/src/pages/CreateDataset.tsx`)
- ✅ Adicionado "Supabase (PostgreSQL)" como opção de fonte de dados
- ✅ Fluxo simplificado sem necessidade de configurar conexão (usa .env)
- ✅ Integração do componente SupabaseTableSelector no wizard
- ✅ Suporte para criação em massa de datasets
- ✅ Preview de tabelas selecionadas na etapa de revisão

## 🔧 Configuração

### Variáveis de Ambiente (.env)

```env
# Supabase Configuration
VITE_SUPABASE_URL=https://seu-projeto.supabase.co
VITE_SUPABASE_PUBLISHABLE_KEY=sua_chave_publica
SUPABASE_ACCESS_TOKEN=seu_token_de_acesso
DATABASE_SUPABASE=seu_database
```

### Instalação de Dependências

```bash
npm install @supabase/supabase-js
```

## 🚀 Como Usar

### 1. **Criar Datasets do Supabase**

1. Navegue para "Criar Novo Dataset"
2. **Passo 1 - Projeto**: Selecione projeto e área
3. **Passo 2 - Fonte**: Selecione "Supabase (PostgreSQL)"
   - A aplicação mostra que a conexão está configurada
   - Não é necessário selecionar uma conexão específica
4. **Passo 3 - Dataset**: 
   - Interface de seleção de tabelas é exibida automaticamente
   - Selecione o schema desejado (default: public)
   - Use a busca para filtrar tabelas
   - Selecione as tabelas desejadas clicando nos checkboxes
   - Use "Selecionar todas" ou "Limpar seleção" para ações em massa
5. **Passo 4 - Revisão**: 
   - Revise as tabelas selecionadas
   - Clique em "Criar X Datasets"
6. **Progresso**:
   - Acompanhe a criação dos datasets em tempo real
   - Veja quais foram criados com sucesso

### 2. **Testar Conexão via API**

```bash
# Testar conexão
curl http://localhost:3000/api/portal/supabase/test-connection

# Listar schemas
curl http://localhost:3000/api/portal/supabase/schemas

# Listar tabelas do schema public
curl "http://localhost:3000/api/portal/supabase/tables?schema=public"
```

## 📊 Fluxo de Dados

```
┌─────────────────┐
│   Supabase DB   │
│  (PostgreSQL)   │
└────────┬────────┘
         │
         │ REST API
         │
┌────────▼────────┐
│ Backend Client  │
│ (supabaseClient)│
└────────┬────────┘
         │
         │ HTTP API
         │
┌────────▼────────┐
│  Portal Routes  │
│   (Express)     │
└────────┬────────┘
         │
         │ REST API
         │
┌────────▼────────┐
│  React Frontend │
│ (CreateDataset) │
└─────────────────┘
```

## 🎨 Interface Visual

### SupabaseTableSelector Component

```
┌─────────────────────────────────────────────────┐
│  📊 Selecionar Tabelas do Supabase       ✓ 3    │
├─────────────────────────────────────────────────┤
│  🔍 [Buscar tabelas...]  [public ▼]  [↻]       │
├─────────────────────────────────────────────────┤
│  3 de 15 tabelas selecionadas                   │
│  [Selecionar todas] [Limpar seleção]            │
├─────────────────────────────────────────────────┤
│  ☑ 📋 clientes         1,234 linhas             │
│  ☐ 📋 produtos         567 linhas               │
│  ☑ 📋 pedidos          8,901 linhas             │
│  ☐ 📋 usuarios         345 linhas               │
│  ☑ 📋 pagamentos       2,456 linhas             │
│  ...                                             │
└─────────────────────────────────────────────────┘
```

## 🔐 Segurança

- ✅ Credenciais armazenadas em variáveis de ambiente
- ✅ Não expõe credenciais no frontend
- ✅ Usa chave pública do Supabase (anon key)
- ✅ Validações server-side

## 🐛 Troubleshooting

### Erro: "Supabase não configurado"
**Solução**: Verifique se as variáveis `VITE_SUPABASE_URL` e `VITE_SUPABASE_PUBLISHABLE_KEY` estão configuradas no `.env`

### Erro: "Erro ao conectar com Supabase"
**Solução**: 
1. Verifique se a URL do Supabase está correta
2. Verifique se a chave pública está válida
3. Verifique se o projeto Supabase está ativo

### Tabelas não aparecem
**Solução**:
1. Verifique se existem tabelas no schema selecionado
2. Tente atualizar clicando no botão de refresh (↻)
3. Verifique as permissões do usuário no Supabase

## 📝 Próximos Passos (Melhorias Futuras)

- [ ] Suporte para múltiplas conexões Supabase
- [ ] Preview de dados das tabelas
- [ ] Filtros avançados (por tamanho, tipo, etc.)
- [ ] Detecção automática de chaves primárias
- [ ] Estimativa de tempo de carga baseado no tamanho
- [ ] Sincronização incremental automática
- [ ] Suporte para views do Supabase

## 📚 Referências

- [Supabase JS Client Documentation](https://supabase.com/docs/reference/javascript)
- [Supabase REST API](https://supabase.com/docs/guides/api)
- [PostgreSQL System Catalogs](https://www.postgresql.org/docs/current/catalogs.html)

---

**Última atualização**: 26/02/2026
**Versão**: 1.0.0
