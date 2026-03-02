# 📊 Guia de Uso: Painel DataOps - Datasets Grid

## 🎯 Visão Geral

A tela de **Datasets - Painel DataOps** é uma ferramenta de governança e monitoramento de cargas incrementais. Ela transforma a visualização tradicional de datasets em um painel operacional profissional.

**Acesso**: Navegue para a rota `/datasets-dataops` no frontend.

---

## 🔍 Funcionalidades Principais

### 1. **Grid Enterprise-Grade com AG Grid**

A grid utiliza AG Grid Community Edition com as seguintes características:

✅ **Header Fixo**: Cabeçalho permanece visível ao rolar  
✅ **Colunas Pinadas**: Dataset, Status e Source fixas à esquerda  
✅ **Filtros por Coluna**: Filtros estilo Excel em todas as colunas  
✅ **Ordenação**: Clique no header para ordenar  
✅ **Density Compacta**: 36px de altura por linha para máxima densidade  
✅ **Virtualização**: Suporta 1000+ datasets sem lag  

---

## 📋 Colunas Disponíveis

### **Colunas Fixas (Pinned Left)**
| Coluna | Descrição | Tipo |
|--------|-----------|------|
| **Dataset** | Nome do dataset | Texto |
| **Status** | Estado de execução (ACTIVE, PAUSED, etc.) | Badge colorido |
| **Source** | Origem dos dados (ORACLE, SUPABASE, etc.) | Badge colorido |

### **Contexto**
| Coluna | Descrição |
|--------|-----------|
| **Projeto** | Identificador do projeto |
| **Área** | Identificador da área |
| **Tipo Carga** | FULL, INCREMENTAL ou SNAPSHOT |

### **Operacional (Governança)** 🔥
| Coluna | Descrição | Uso |
|--------|-----------|-----|
| **Coluna Incremental** | Nome da coluna watermark (ex: `updated_at`) | Identificar datasets sem incremental |
| **Lookback Days** | Quantidade de dias retroativos | Validar janela de carga |
| **Criado em** | Data de criação do dataset | Identificar datasets antigos |
| **Última Execução** | Timestamp da última execução bem-sucedida | **SLA Monitoring** 🚨 |

### **Estrutural**
| Coluna | Descrição |
|--------|-----------|
| **Bronze** | Nome completo da tabela Bronze |
| **Silver** | Nome completo da tabela Silver |
| **Bronze Mode** | SNAPSHOT, CURRENT ou APPEND_LOG |
| **Strategy** | Estratégia incremental (WATERMARK, HASH_MERGE, etc.) |

---

## 🎨 Badges e Indicadores Visuais

### **Tipo de Carga**
- 📦 **FULL** (Cinza): Carga completa sempre
- 🔄 **INCREMENTAL** (Verde): Carga incremental com watermark
- 📸 **SNAPSHOT** (Roxo): Snapshot periódico

### **Última Execução (Health Status)**
- 🟢 **Verde**: Executado nas últimas 24h (saudável)
- 🟡 **Amarelo**: Executado há 1-3 dias (atenção)
- 🔴 **Vermelho**: Executado há mais de 3 dias (crítico)
- ⚪ **Branco**: Nunca executado

### **Bronze Mode**
- 📸 **SNAPSHOT**: INSERT OVERWRITE (reescreve tudo)
- 🔄 **CURRENT**: MERGE UPSERT (atualiza apenas mudanças)
- 📝 **APPEND_LOG**: INSERT (apenas adiciona)

---

## 🚀 Filtros Rápidos (Quick Filters)

Clique nos botões para aplicar filtros pré-configurados:

| Filtro | Descrição | Caso de Uso |
|--------|-----------|-------------|
| 🔴 **Sem Incremental** | Datasets sem `watermark_column` | Identificar candidatos para otimização |
| ⚠️ **Parados >3 dias** | Datasets sem execução há mais de 3 dias | Monitorar SLA |
| 📦 **Carga Full** | Datasets com `load_type=FULL` | Identificar cargas pesadas |
| 🔄 **Carga Incremental** | Datasets com `load_type=INCREMENTAL` | Validar incrementais ativos |
| 📸 **Snapshot** | Datasets com estratégia SNAPSHOT | Revisar snapshots periódicos |

**Dica**: Você pode combinar múltiplos filtros. O badge mostra quantos filtros estão ativos.

---

## 📊 Barra de Estatísticas

A barra azul no topo mostra métricas em tempo real:

- **Total de Datasets**: Quantidade total carregada
- 🔄 **Incrementais**: Datasets com carga incremental ativa
- 🔴 **Sem Watermark**: Datasets sem coluna incremental configurada
- ⚠️ **Parados >3 dias**: Datasets que não executam há mais de 3 dias

---

## 🔎 Filtros por Coluna (Excel-style)

Cada coluna possui um filtro individual:

### **Filtros de Texto**
- Digite para buscar (ex: "oracle")
- Operadores: contém, igual, começa com, termina com

### **Filtros Numéricos**
- Operadores: maior que, menor que, igual
- Exemplo: `lookback_days > 7`

### **Filtros de Data**
- Range de datas
- Últimos X dias
- Antes/depois de

### **Filtros Set (Dropdown)**
- Selecione múltiplos valores
- Status: ACTIVE, PAUSED, etc.
- Source: ORACLE, SUPABASE, etc.

---

## 🎯 Casos de Uso Práticos

### **1. Identificar Datasets sem Carga Incremental**
```
1. Clique no botão "🔴 Sem Incremental"
2. Resultado: 48 datasets sem watermark configurado
3. Ação: Priorizar configuração incremental
```

### **2. Monitorar SLA de Execuções**
```
1. Clique no botão "⚠️ Parados >3 dias"
2. Observe a coluna "Última Execução" com 🔴
3. Ação: Investigar datasets críticos
```

### **3. Auditar Cargas FULL (Pesadas)**
```
1. Clique no botão "📦 Carga Full"
2. Ordene por "Bronze Rows" (descendente)
3. Ação: Converter maiores para incremental
```

### **4. Validar Lookback Days**
```
1. Use filtro de coluna: lookback_days > 30
2. Resultado: Datasets com janela muito larga
3. Ação: Ajustar para lookback_days adequado (ex: 3-7 dias)
```

### **5. Encontrar Datasets Órfãos**
```
1. Clique no filtro de coluna "Última Execução"
2. Selecione "Nunca executado" (⚪)
3. Ação: Remover ou ativar datasets
```

---

## 🛠️ Atalhos e Dicas

### **Navegação**
- **Clique na linha**: Abre detalhes do dataset
- **Clique no header**: Ordena a coluna
- **Drag column border**: Redimensiona coluna
- **Drag header**: Reordena colunas

### **Filtros**
- **Limpar Filtros**: Clique no botão "🔄 Limpar Filtros"
- **Múltiplos filtros**: Combine quick filters + filtros de coluna
- **Reset individual**: Use botão "Reset" em cada filtro de coluna

### **Performance**
- A grid virtualiza linhas (apenas visíveis são renderizadas)
- Suporta até 1000 datasets simultaneamente
- Filtros são aplicados no **backend** (não client-side)

---

## 📈 Glossário de Termos

| Termo | Definição |
|-------|-----------|
| **Watermark Column** | Coluna usada para identificar registros novos/atualizados (ex: `updated_at`, `dt_updated`) |
| **Lookback Days** | Quantidade de dias retroativos para buscar na carga incremental |
| **Bronze Mode** | Estratégia de escrita na camada Bronze (SNAPSHOT, CURRENT, APPEND_LOG) |
| **Incremental Strategy** | Método de descoberta de dados incrementais (WATERMARK, HASH_MERGE, CDC) |
| **Load Type** | Tipo de carga derivado: FULL (sem incremental), INCREMENTAL (com watermark), SNAPSHOT (estratégia snapshot) |
| **Last Success At** | Timestamp da última execução bem-sucedida (usado para SLA monitoring) |

---

## 🚨 Troubleshooting

### **Problema: Grid não carrega**
**Solução**: 
1. Verifique console do navegador (F12)
2. Confirme que backend está rodando em `http://localhost:3000`
3. Teste endpoint: `http://localhost:3000/api/portal/datasets`

### **Problema: Filtros não funcionam**
**Solução**:
1. Limpe cache do navegador (Ctrl+Shift+R)
2. Verifique que backend retorna os campos: `enable_incremental`, `incremental_strategy`, `watermark_column`, `lookback_days`, `load_type`

### **Problema: Badges não aparecem**
**Solução**:
1. Recompile frontend: `npm run build` (no diretório `frontend/`)
2. Reinicie servidor backend
3. Recarregue página com cache limpo

---

## 📞 Suporte

Para dúvidas ou problemas:
1. Consulte os logs do backend (janela PowerShell do servidor)
2. Verifique a documentação técnica: `docs/IMPLEMENTACAO_MONITORAMENTO_INCREMENTAL.md`
3. Revise o plano de implementação: Ver plano `fdcc5371-b602-400f-8472-c7461b71f0aa`

---

**Última Atualização**: 2026-02-27  
**Versão**: 1.0.0
