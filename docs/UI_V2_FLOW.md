# Fluxo Visual da Interface V2

## 🎯 Arquitetura de Navegação

```
┌─────────────────────────────────────────────────────────────────┐
│                         HEADER                                   │
│  Logo: Plataforma de Ingestão  |  🔔 Notificações  |  👤 Usuário │
└─────────────────────────────────────────────────────────────────┘

┌──────────┬──────────────────────────────────────────────────────┐
│          │                                                       │
│ SIDEBAR  │                  MAIN CONTENT                        │
│          │                                                       │
│ ├─ 📊    │   ┌───────────────────────────────────────────┐    │
│ │ Dash   │   │                                           │    │
│ │        │   │         VIEW BASEADA NA NAVEGAÇÃO         │    │
│ ├─ 📁    │   │                                           │    │
│ │ Datasets│   │   • Dashboard                            │    │
│ │        │   │   • Meus Datasets                        │    │
│ ├─ ➕    │   │   • Criar Dataset (Wizard)               │    │
│ │ Criar  │   │   • Monitoramento                        │    │
│ │        │   │   • Aprovações                           │    │
│ ├─ 📊    │   │                                           │    │
│ │ Monitor│   └───────────────────────────────────────────┘    │
│ │        │                                                       │
│ └─ ✅    │                                                       │
│   Aprova │                                                       │
│   🔴 2   │     (Badge mostra pendências)                        │
└──────────┴──────────────────────────────────────────────────────┘
```

## 📊 Fluxo: Dashboard

```
┌─────────────────────────────────────────────────────────┐
│                    DASHBOARD                             │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐       │
│  │  📊    │  │  ✅    │  │  ⏸️    │  │  ❌    │       │
│  │ Total  │  │ Ativos │  │ Pausad │  │ Erros  │       │
│  │   12   │  │   8    │  │   2    │  │   2    │       │
│  └────────┘  └────────┘  └────────┘  └────────┘       │
│                                                          │
│  ┌──────────────────────────────────────────────┐      │
│  │ 🔧 STATUS DO ORCHESTRATOR                    │      │
│  │  ✅ Ativo  |  Pendentes: 3  |  Em exec: 1   │      │
│  └──────────────────────────────────────────────┘      │
│                                                          │
│  ┌───────────────────┐  ┌────────────────────┐        │
│  │ ⏱️ Execuções      │  │ ⚠️ Falhas Recentes │        │
│  │ Recentes          │  │                    │        │
│  │                   │  │ • dataset_x        │        │
│  │ SUCCEEDED: 45     │  │   SCHEMA_ERROR     │        │
│  │ FAILED: 2         │  │                    │        │
│  └───────────────────┘  └────────────────────┘        │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## 📁 Fluxo: Meus Datasets

```
┌─────────────────────────────────────────────────────────┐
│                 MEUS DATASETS                            │
│  🔍 [Buscar datasets...]              [🔄 Atualizar]    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────┐ │
│  │ glo_agentes   │  │ clientes      │  │ produtos    │ │
│  │         🟢 ACTIVE│  │         🔴 BLOCK │  │     🟡 PAUSED│ │
│  │               │  │               │  │             │ │
│  │ 📁 proj / area│  │ 📁 proj / area│  │ 📁 proj/area│ │
│  │ 🔌 ORACLE     │  │ 🔌 ORACLE     │  │ 🔌 ORACLE   │ │
│  │ 📦 bronze...  │  │ 📦 bronze...  │  │ 📦 bronze.. │ │
│  │ 🛡️ silver...  │  │ 🛡️ silver...  │  │ 🛡️ silver.. │ │
│  └───────────────┘  └───────────────┘  └─────────────┘ │
│       │ CLICK            │                   │          │
│       ▼                  │                   │          │
│  ┌────────────────────────────────────┐     │          │
│  │      MODAL: DETALHES DO DATASET    │     │          │
│  ├────────────────────────────────────┤     │          │
│  │ ID: abc-123                        │     │          │
│  │ Estado: ACTIVE                     │     │          │
│  │ Projeto/Área: proj / area         │     │          │
│  │ Bronze: catalog.schema.table      │     │          │
│  │ Silver: catalog.schema.table      │     │          │
│  ├────────────────────────────────────┤     │          │
│  │ [▶️ Executar] [📤 Publicar] [⏸️ Pausar] │          │
│  └────────────────────────────────────┘     │          │
│                                              │          │
└─────────────────────────────────────────────────────────┘
```

## ➕ Fluxo: Criar Dataset (Wizard)

```
                   WIZARD DE CRIAÇÃO
╔══════════════════════════════════════════════════════╗
║                                                       ║
║  ① ────── ② ────── ③ ────── ④                       ║
║ Projeto  Fonte   Config   Revisão                    ║
║  (ativo) (inativo)(inativo)(inativo)                 ║
║                                                       ║
╠══════════════════════════════════════════════════════╣
║                                                       ║
║  PASSO 1: SELECIONE O PROJETO E ÁREA                ║
║                                                       ║
║  📁 Projeto:     [Selecione um projeto... ▼]        ║
║                                                       ║
║  🌳 Área:        [Selecione uma área... ▼]          ║
║                  (habilitado após selecionar projeto)║
║                                                       ║
║                                                       ║
║                                                       ║
║  [◀ Anterior (disabled)]        [Próximo ▶]         ║
║                                                       ║
╚══════════════════════════════════════════════════════╝
           │
           ▼ CLICK "Próximo"
╔══════════════════════════════════════════════════════╗
║  ① ────── ② ────── ③ ────── ④                       ║
║ Projeto  Fonte   Config   Revisão                    ║
║(completo)(ativo)(inativo)(inativo)                   ║
║                                                       ║
╠══════════════════════════════════════════════════════╣
║                                                       ║
║  PASSO 2: CONFIGURE A FONTE DE DADOS                ║
║                                                       ║
║  🔌 Tipo de Fonte:                                   ║
║  ┌──────────┐  ┌──────────┐                         ║
║  │ 🗄️       │  │ ☁️       │                         ║
║  │ Oracle   │  │SharePoint│                         ║
║  │ (selecionado)│(não selecionado)                  ║
║  └──────────┘  └──────────┘                         ║
║                                                       ║
║  🔗 Conexão:    [Selecione uma conexão... ▼]        ║
║                                                       ║
║  🏷️ Nome:       [_________________]                  ║
║                 ex: glo_agentes                      ║
║                                                       ║
║  [◀ Anterior]                 [Próximo ▶]           ║
║                                                       ║
╚══════════════════════════════════════════════════════╝
           │
           ▼ CLICK "Próximo"
╔══════════════════════════════════════════════════════╗
║  ① ────── ② ────── ③ ────── ④                       ║
║ Projeto  Fonte   Config   Revisão                    ║
║(completo)(completo)(ativo)(inativo)                  ║
║                                                       ║
╠══════════════════════════════════════════════════════╣
║                                                       ║
║  PASSO 3: CONFIGURAÇÕES DE INGESTÃO                 ║
║                                                       ║
║  ℹ️ Bronze: Dados brutos sem transformação           ║
║     Silver: Dados validados com contrato             ║
║                                                       ║
║  ⚙️ Tipo de Carga:                                   ║
║  ┌─────────────┐  ┌─────────────┐                   ║
║  │ ➕          │  │ 🔄          │                   ║
║  │ Incremental │  │ Full        │                   ║
║  │ Dados novos │  │ Todos dados │                   ║
║  │(selecionado)│  │             │                   ║
║  └─────────────┘  └─────────────┘                   ║
║                                                       ║
║  📅 Coluna Incremental: [updated_at______]          ║
║                                                       ║
║  📊 Batch Size:        [1000]                        ║
║                                                       ║
║  [◀ Anterior]                 [Próximo ▶]           ║
║                                                       ║
╚══════════════════════════════════════════════════════╝
           │
           ▼ CLICK "Próximo"
╔══════════════════════════════════════════════════════╗
║  ① ────── ② ────── ③ ────── ④                       ║
║ Projeto  Fonte   Config   Revisão                    ║
║(completo)(completo)(completo)(ativo)                 ║
║                                                       ║
╠══════════════════════════════════════════════════════╣
║                                                       ║
║  PASSO 4: REVISAR CONFIGURAÇÃO                      ║
║                                                       ║
║  📋 Informações Básicas                              ║
║  Projeto:         Projeto ABC                        ║
║  Área:            Vendas                             ║
║  Nome do Dataset: glo_agentes                        ║
║                                                       ║
║  🔌 Fonte de Dados                                   ║
║  Tipo:            ORACLE                             ║
║  Conexão:         conn_prod_01                       ║
║                                                       ║
║  ⚙️ Configurações                                    ║
║  Tipo de Carga:   INCREMENTAL                        ║
║  Col. Incremental: updated_at                        ║
║  Batch Size:      1000                               ║
║                                                       ║
║  ✅ Pronto para criar!                               ║
║     Dataset será criado em modo DRAFT                ║
║                                                       ║
║  [◀ Anterior]           [✅ Criar Dataset]           ║
║                                                       ║
╚══════════════════════════════════════════════════════╝
           │
           ▼ CLICK "Criar Dataset"
╔══════════════════════════════════════════════════════╗
║  🎉 NOTIFICAÇÃO TOAST (verde)                        ║
║  ✅ Dataset criado com sucesso!                      ║
╚══════════════════════════════════════════════════════╝
           │
           ▼ REDIRECT
    [VIEW: Meus Datasets]
```

## ✅ Fluxo: Aprovações

```
┌─────────────────────────────────────────────────────────┐
│             APROVAÇÕES DE SCHEMA   🔴 2                 │
│                                        [🔄 Atualizar]    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ 📊 dataset_clientes                            │    │
│  │                      [✅ Aprovar] [❌ Rejeitar]│    │
│  ├────────────────────────────────────────────────┤    │
│  │ Versão pendente: 2                             │    │
│  │ Status: PENDING                                │    │
│  │                                                 │    │
│  │ Mudanças detectadas:                           │    │
│  │ • ADD_COLUMN: telefone_celular (string)        │    │
│  │ • TYPE_CHANGE: idade (int → bigint)            │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ 📊 dataset_produtos                            │    │
│  │                      [✅ Aprovar] [❌ Rejeitar]│    │
│  ├────────────────────────────────────────────────┤    │
│  │ Versão pendente: 3                             │    │
│  │ Status: PENDING                                │    │
│  │                                                 │    │
│  │ Mudanças detectadas:                           │    │
│  │ • REMOVE_COLUMN: campo_antigo                  │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│       │ CLICK "Aprovar"                                 │
│       ▼                                                  │
│  ╔════════════════════════════════╗                    │
│  ║ 🎉 TOAST (verde)               ║                    │
│  ║ ✅ Schema aprovado com sucesso!║                    │
│  ╚════════════════════════════════╝                    │
│                                                          │
│  (Badge atualiza: 🔴 2 → 🔴 1)                          │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## 🎨 Sistema de Cores e Estados

```
┌─────────────────────────────────────────────┐
│              ESTADOS DOS DATASETS            │
├─────────────────────────────────────────────┤
│                                              │
│  🟢 ACTIVE          Dataset ativo, executa  │
│                     normalmente              │
│                                              │
│  ⚪ DRAFT           Modo rascunho, não      │
│                     executa automaticamente  │
│                                              │
│  🟡 PAUSED          Pausado pelo usuário,   │
│                     não executa              │
│                                              │
│  🔴 BLOCKED         Bloqueado por mudança   │
│     _SCHEMA_CHANGE  de schema, requer       │
│                     aprovação                │
│                                              │
│  🟣 DEPRECATED      Dataset obsoleto, não   │
│                     deve mais ser usado      │
│                                              │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│            NOTIFICAÇÕES (Toasts)             │
├─────────────────────────────────────────────┤
│                                              │
│  ✅ SUCCESS (verde)    Ação bem-sucedida    │
│  ⚠️ WARNING (amarelo)  Atenção necessária   │
│  ❌ ERROR (vermelho)   Erro ocorreu         │
│  ℹ️ INFO (azul)        Informação           │
│                                              │
└─────────────────────────────────────────────┘
```

## 🔄 Ciclo de Vida de um Dataset

```
    [Criar Dataset]
           │
           ▼
      ⚪ DRAFT ───────────────┐
           │                   │
           │ [Publicar]        │ [Deletar]
           ▼                   │
      🟢 ACTIVE                │
           │                   │
    ┌──────┼──────┐           │
    │      │      │           │
[Pausar]  [Schema] [Depreciar]│
    │      │      │           │
    ▼      ▼      ▼           │
🟡 PAUSED 🔴 BLOCKED 🟣 DEPRECATED
    │      │                   │
[Reativar][Aprovar]            │
    │      │                   │
    └──────┴────► 🟢 ACTIVE    │
                   │           │
                   │           │
                   └───────────┘
```

## 📱 Responsividade

```
Desktop (1920px+)              Tablet (768-1024px)       Mobile (<768px)
┌─────┬─────────────┐          ┌─────┬───────────┐      ┌────────────┐
│Side-│   Content   │          │Side-│ Content   │      │☰ Menu      │
│bar  │             │          │bar  │           │      ├────────────┤
│     │   [Cards]   │          │     │  [Cards]  │      │  Content   │
│     │   in Grid   │          │     │  2 cols   │      │            │
│     │   4 cols    │          │     │           │      │  [Cards]   │
│     │             │          │     │           │      │  1 col     │
└─────┴─────────────┘          └─────┴───────────┘      └────────────┘
```

---

## 💡 Dicas de Navegação

1. **Breadcrumb Mental**: A sidebar sempre mostra onde você está
2. **Badges**: Indicam ações necessárias (ex: aprovações pendentes)
3. **Cores**: Verde = OK, Amarelo = Atenção, Vermelho = Ação Urgente
4. **Toasts**: Feedback instantâneo de todas as ações
5. **Modal**: Detalhes sem sair da página atual
