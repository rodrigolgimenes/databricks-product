# 🚀 Quick Start - Interface V2

## Início Rápido (5 minutos)

### 1. Certifique-se que o servidor está rodando

```powershell
# Se não estiver rodando, inicie:
cd C:\dev\cm-databricks
npm start
```

### 2. Acesse a nova interface

Abra seu navegador e acesse:
```
http://localhost:3000/v2.html
```

### 3. Pronto! 🎉

Você verá a nova interface moderna com:
- ✅ Dashboard com KPIs visuais
- ✅ Navegação por sidebar
- ✅ Wizard guiado para criar datasets
- ✅ Monitoramento em tempo real

---

## 📁 Arquivos Criados

A nova interface V2 consiste em:

```
public/
├── v2.html         # Interface principal
├── v2.css          # Estilos modernos (~940 linhas)
└── v2.js           # Lógica interativa (~770 linhas)
```

---

## 🎨 Principais Diferenças vs Interface Antiga

| Aspecto | Interface Antiga | Interface V2 ✨ |
|---------|------------------|-----------------|
| **Design** | Tabelas densas | Cards visuais espaçados |
| **Navegação** | Tabs horizontais | Sidebar com ícones |
| **Criação** | Formulário único complexo | Wizard guiado (4 passos) |
| **Feedback** | Limitado | Toasts + badges + cores |
| **Público-alvo** | Usuário técnico | Usuário final (leigo) |
| **Estados** | Texto simples | Badges coloridos |
| **Busca** | Não tinha | Busca em tempo real |

---

## 🎯 Fluxo Básico de Uso

### Para Criar seu Primeiro Dataset:

1. **Acesse** `http://localhost:3000/v2.html`
2. Clique em **"Criar Dataset"** na sidebar (ícone ➕)
3. **Passo 1**: Selecione Projeto e Área
4. **Passo 2**: Escolha a fonte (Oracle/SharePoint) e defina o nome
5. **Passo 3**: Configure tipo de carga (Incremental/Full)
6. **Passo 4**: Revise tudo e clique em "Criar Dataset"
7. ✅ **Sucesso!** Você criou seu primeiro dataset

### Para Ver seus Datasets:

1. Clique em **"Meus Datasets"** na sidebar (ícone 📁)
2. Veja todos os datasets em cards visuais
3. Use a busca para filtrar
4. Clique em um card para ver detalhes

### Para Aprovar Mudanças de Schema:

1. Veja o badge vermelho em **"Aprovações"** (indica pendências)
2. Clique em "Aprovações" (ícone ✅)
3. Revise as mudanças detectadas
4. Clique em "Aprovar" ou "Rejeitar"

---

## 🔧 Configuração (Opcional)

A interface V2 usa as mesmas variáveis de ambiente do servidor:

```env
# .env
UC_CATALOG=cm_dbx_dev
DATABRICKS_HOST=https://xxx.cloud.databricks.com
DATABRICKS_TOKEN=dapi***
DATABRICKS_SQL_WAREHOUSE_ID=4e4f36c4c611f1d3
```

---

## 🐛 Troubleshooting Rápido

### Problema: "Erro ao carregar dados"
**Solução**: Verifique se:
- O servidor Node.js está rodando (`npm start`)
- As variáveis de ambiente do Databricks estão configuradas
- Você consegue acessar o Databricks

### Problema: Wizard não avança
**Solução**: 
- Preencha todos os campos obrigatórios
- Nome do dataset: apenas letras minúsculas, números e `_`
- Para carga incremental, informe a coluna

### Problema: "Nenhum dataset encontrado"
**Solução**:
- Certifique-se de que existem datasets criados no banco
- Execute scripts de seed se necessário
- Verifique logs do servidor

---

## 📊 Recursos Visuais

### Dashboard
```
┌────────────────────────────────────────┐
│  📊 Total: 12  ✅ Ativos: 8           │
│  ⏸️ Pausados: 2  ❌ Erros: 2          │
│                                        │
│  🔧 Orchestrator: ✅ Ativo            │
│     Pendentes: 3 | Em execução: 1     │
└────────────────────────────────────────┘
```

### Dataset Card
```
┌─────────────────────┐
│ glo_agentes    🟢   │ ← Badge de estado
│                     │
│ 📁 projeto / area   │
│ 🔌 ORACLE          │
│ 📦 bronze.table    │
│ 🛡️ silver.table    │
└─────────────────────┘
  ↑ Clique para ver detalhes
```

---

## 🎓 Próximos Passos

1. ✅ Teste criando um dataset de exemplo
2. ✅ Navegue pelo dashboard e veja os KPIs
3. ✅ Explore a lista de datasets
4. ✅ Familiarize-se com os estados (cores)
5. ✅ Teste o monitoramento

---

## 📚 Documentação Completa

Para mais detalhes, consulte:
- **Guia Completo**: `/docs/UI_V2_GUIDE.md`
- **Fluxos Visuais**: `/docs/UI_V2_FLOW.md`
- **Documentação da Plataforma**: `/docs/docs_00-index.md`

---

## 🆚 Comparação Lado a Lado

### Interface Antiga (index.html)
```
┌─────────────────────────────────────────┐
│ [Configs] [Portal Governado] [Pro]     │
│                                          │
│ ┌────────────────────────────────────┐ │
│ │ TABELA DE CONFIGS                  │ │
│ │ ID | Nome | Tipo | Fonte | ...     │ │
│ │ 1  | xxx  | FULL | Table | ...     │ │
│ │ 2  | yyy  | INC  | Path  | ...     │ │
│ └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

### Interface V2 (v2.html) ✨
```
┌──────┬──────────────────────────────────┐
│ SIDE │  Dashboard                       │
│ BAR  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐   │
│      │  │📊12│ │✅8│ │⏸️2│ │❌2│   │
│ 📊   │  └────┘ └────┘ └────┘ └────┘   │
│ 📁   │                                   │
│ ➕   │  Status do Orchestrator          │
│ 📊   │  [✅ Ativo | Pendentes: 3]      │
│ ✅🔴2│                                   │
└──────┴──────────────────────────────────┘
```

---

## 💡 Dicas Profissionais

1. **Sempre comece pelo Dashboard** - Visão geral do sistema
2. **Use a busca** - Fundamental com muitos datasets
3. **Preste atenção nos badges** - Indicam ações necessárias
4. **Cores são importantes** - Verde = OK, Amarelo = Atenção, Vermelho = Urgente
5. **Teste em DRAFT primeiro** - Depois publique

---

## 🎉 Pronto!

Você agora tem uma interface moderna e intuitiva para gerenciar seus datasets de ingestão!

**Dúvidas?** Consulte `/docs/UI_V2_GUIDE.md` ou abra o console do navegador (F12) para logs.
