# Guia da Interface V2 - Plataforma de Ingestão Governada

## 🎨 Visão Geral

A Interface V2 é uma experiência moderna e intuitiva projetada para usuários finais (leigos) gerenciarem datasets de ingestão na plataforma Databricks com facilidade.

## 🚀 Acesso

Acesse a nova interface em: **`http://localhost:3000/v2.html`**

## 📋 Funcionalidades Principais

### 1. **Dashboard**
- **KPIs Visuais**: Métricas em tempo real sobre total de datasets, ativos, pausados e com erro
- **Status do Orchestrator**: Monitore se o orchestrator está ativo ou inativo
- **Execuções Recentes**: Visualize as últimas execuções
- **Falhas Recentes**: Identifique rapidamente problemas

### 2. **Meus Datasets**
- **Cards Visuais**: Cada dataset é apresentado em um card com informações-chave
- **Busca**: Encontre datasets rapidamente pelo nome ou ID
- **Estados Codificados por Cor**:
  - 🟢 Verde (ACTIVE): Dataset ativo e funcionando
  - ⚪ Cinza (DRAFT): Dataset em modo rascunho
  - 🟡 Amarelo (PAUSED): Dataset pausado
  - 🔴 Vermelho (BLOCKED): Bloqueado por mudança de schema
- **Detalhes por Click**: Clique em um card para ver detalhes completos

### 3. **Criar Dataset (Wizard em 4 Passos)**

#### **Passo 1: Projeto**
- Selecione o **Projeto** ao qual o dataset pertence
- Escolha a **Área** funcional dentro do projeto
- As áreas são carregadas dinamicamente baseadas no projeto selecionado

#### **Passo 2: Fonte**
- Escolha o **Tipo de Fonte** (Oracle ou SharePoint) com botões visuais
- Selecione uma **Conexão Aprovada** da lista
- Defina o **Nome do Dataset** (apenas letras minúsculas, números e underscore)

#### **Passo 3: Configuração**
- **Tipo de Carga**:
  - **Incremental**: Carrega apenas dados novos (requer coluna incremental)
  - **Full**: Recarrega todos os dados
- **Coluna Incremental**: Campo de timestamp para identificar dados novos (ex: `updated_at`)
- **Tamanho do Batch**: Número de registros processados por vez

#### **Passo 4: Revisão**
- Revise todas as configurações antes de criar
- Uma caixa verde confirma que está tudo pronto
- Click em "Criar Dataset" para finalizar

### 4. **Monitoramento**
- Visualize a **Fila de Execução** (run_queue)
- Acompanhe o **Histórico de Execuções**
- Ideal para verificar o andamento de execuções em tempo real

### 5. **Aprovações de Schema**
- Visualize schemas pendentes de aprovação
- **Badge de notificação** na navegação indica pendências
- Botões para **Aprovar** ou **Rejeitar** mudanças de schema
- Essencial para governança de dados

## 🎯 Fluxo de Trabalho Típico

### Para Criar um Novo Dataset:

1. **Navegue para "Criar Dataset"**
2. **Passo 1**: Selecione Projeto e Área
3. **Passo 2**: Configure a fonte de dados
4. **Passo 3**: Defina tipo de carga (Incremental ou Full)
5. **Passo 4**: Revise e clique em "Criar Dataset"
6. ✅ **Sucesso!** Dataset criado em modo DRAFT

### Para Executar um Dataset:

1. **Navegue para "Meus Datasets"**
2. **Clique no card** do dataset desejado
3. No modal de detalhes:
   - **Publicar** (se estiver em DRAFT) para torná-lo ACTIVE
   - **Executar** para enfileirar uma execução manual
4. Acompanhe a execução em **Monitoramento**

### Para Aprovar Mudanças de Schema:

1. **Navegue para "Aprovações"**
2. Revise as mudanças detectadas
3. Clique em **Aprovar** ou **Rejeitar**
4. Datasets bloqueados voltam a funcionar após aprovação

## 🎨 Design Principles

### Visual Hierarchy
- **Cores consistentes** para estados (verde = sucesso, amarelo = atenção, vermelho = erro)
- **Ícones intuitivos** do Font Awesome para cada funcionalidade
- **Espaçamento generoso** para facilitar leitura

### User Experience
- **Wizard guiado** elimina confusão na criação
- **Feedback imediato** com toasts de notificação
- **Estados visuais claros** em badges e cores
- **Busca integrada** para encontrar datasets rapidamente

### Responsive & Modern
- **Design adaptável** para diferentes tamanhos de tela
- **Animações suaves** para transições
- **Sidebar fixa** para navegação sempre acessível

## 🔧 Tecnologias

- **HTML5 + CSS3**: Interface moderna com CSS Variables
- **Vanilla JavaScript**: Sem dependências externas
- **Font Awesome 6**: Ícones profissionais
- **API REST**: Integração completa com backend Express

## 📊 Mapeamento de Endpoints

A interface V2 utiliza os seguintes endpoints da API:

| Funcionalidade | Endpoint | Método |
|----------------|----------|--------|
| Dashboard Summary | `/api/portal/dashboard/summary` | GET |
| Orchestrator Status | `/api/portal/orchestrator/status` | GET |
| Listar Projects | `/api/portal/projects` | GET |
| Listar Areas | `/api/portal/areas?project_id=X` | GET |
| Listar Conexões | `/api/portal/connections/oracle` | GET |
| Listar Datasets | `/api/portal/datasets?limit=100` | GET |
| Detalhe Dataset | `/api/portal/datasets/:id` | GET |
| Criar Dataset | `/api/portal/datasets` | POST |
| Publicar Dataset | `/api/portal/datasets/:id/publish` | POST |
| Enfileirar Run | `/api/portal/run-queue` | POST |
| Aprovações Pendentes | `/api/portal/schema-approvals/pending` | GET |
| Aprovar Schema | `/api/portal/datasets/:id/schema/:ver/approve` | POST |
| Rejeitar Schema | `/api/portal/datasets/:id/schema/:ver/reject` | POST |

## 🐛 Troubleshooting

### "Erro ao carregar dados"
- Verifique se o servidor Node.js está rodando
- Confirme que as variáveis de ambiente do Databricks estão configuradas
- Verifique logs do servidor em `node server.js`

### "Databricks não configurado"
- Configure as variáveis:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`
  - `DATABRICKS_SQL_WAREHOUSE_ID`
  - `UC_CATALOG` (padrão: `cm_dbx_dev`)

### Wizard não avança
- Preencha todos os campos obrigatórios
- Verifique se o nome do dataset está no formato correto (apenas `a-z`, `0-9`, `_`)
- Para carga incremental, informe a coluna incremental

## 🎓 Dicas para Usuários

1. **Dashboard é seu amigo**: Sempre comece pelo dashboard para verificar o status geral
2. **Use a busca**: Com muitos datasets, a busca é essencial
3. **Monitore aprovações**: O badge vermelho indica pendências importantes
4. **Teste em DRAFT**: Crie datasets em modo DRAFT, teste, depois publique
5. **Acompanhe execuções**: Use o monitoramento para verificar o progresso

## 📝 Próximos Passos

Funcionalidades planejadas para versões futuras:

- [ ] Visualização de logs de execução em tempo real
- [ ] Gráficos de performance e métricas
- [ ] Preview dos dados Bronze/Silver inline
- [ ] Filtros avançados na lista de datasets
- [ ] Exportação de relatórios
- [ ] Tema escuro (dark mode)

## 🆚 Comparação: V1 vs V2

| Aspecto | V1 (index.html) | V2 (v2.html) |
|---------|-----------------|--------------|
| Design | Tabular, denso | Cards visuais, espaçado |
| Navegação | Tabs | Sidebar com ícones |
| Criação | Formulário único | Wizard guiado em 4 passos |
| Feedback | Limitado | Toasts + badges + cores |
| Público | Técnico | Usuário final (leigo) |
| Responsivo | Básico | Completo |

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique a documentação completa em `/docs`
2. Consulte os logs do servidor
3. Revise o console do navegador (F12) para erros JavaScript
