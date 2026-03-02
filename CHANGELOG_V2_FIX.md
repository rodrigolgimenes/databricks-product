# Changelog - Correções V2 Interface

## 📅 Data: 20/02/2026

---

## ✅ Correções Aplicadas

### 1. **Modal não fecha ao clicar no X** 🐛 → ✅ CORRIGIDO

**Problema:**
- Modal abria mas não fechava ao clicar no botão X ou fora do modal
- CSS com `display: flex` sempre ativo impedia o atributo `hidden` de funcionar

**Correção:**
- **Arquivo:** `public/v2.css`
- **Adicionado:** Regra CSS para respeitar o atributo `hidden`
  ```css
  .v2-modal[hidden] {
    display: none !important;
  }
  ```

**Arquivo:** `public/v2.js`
- **Corrigido:** Evento do overlay para fechar apenas ao clicar fora do conteúdo
- **Adicionado:** Prevenção de propagação de cliques no conteúdo do modal

---

### 2. **Botão "Executar" retornava 404** 🐛 → ✅ CORRIGIDO

**Problema:**
```
POST /api/portal/run-queue → 404 Not Found
API Error: SyntaxError: Unexpected token '<', "<!DOCTYPE "... is not valid JSON
```

**Causa:**
- Botão "Executar" chamava rota **inexistente** `/api/portal/run-queue`
- A rota correta é `/api/portal/datasets/:datasetId/enqueue`

**Correção:**
- **Arquivo:** `public/v2.js` (linhas 455-467)
- **Antes:**
  ```javascript
  await apiPost('/api/portal/run-queue', {
    dataset_id: dataset.dataset_id,
    trigger_type: 'MANUAL',
  });
  ```
- **Depois:**
  ```javascript
  await apiPost(`/api/portal/datasets/${dataset.dataset_id}/enqueue`, {
    trigger_type: 'MANUAL',
  });
  ```

**Melhorias adicionais:**
- Adicionado `loadDashboard()` após enfileirar para atualizar contadores

---

### 3. **Logging de API aprimorado** 📊 → ✅ IMPLEMENTADO

**Adicionado:**
- **Arquivo:** `public/v2.js` (linhas 100-125)
- Logs detalhados para todas as chamadas de API
- Verificação de Content-Type antes de fazer parse JSON
- Mensagens de erro mais claras

**Logs agora disponíveis:**
```
[V2 API] GET /api/portal/projects
[V2 API] ✓ /api/portal/projects - OK
[V2 API] ❌ /api/portal/xxx retornou text/html ao invés de JSON
```

---

## 🎯 Resultados

### Antes das correções:
- ❌ Modal não fechava
- ❌ Botão "Executar" gerava erro 404
- ❌ Erros de API confusos
- ❌ Sem logs detalhados

### Depois das correções:
- ✅ Modal abre e fecha corretamente
- ✅ Botão "Executar" funciona e enfileira datasets
- ✅ Mensagens de erro claras
- ✅ Logs detalhados no console para debug

---

## 🧪 Como Testar

### 1. Recarregar página
```
Ctrl+Shift+R (Windows)
Cmd+Shift+R (Mac)
```

### 2. Testar modal
1. Vá em "Meus Datasets"
2. Clique em um dataset
3. Modal deve abrir
4. Clique no X → Modal fecha
5. Abra novamente e clique fora → Modal fecha

### 3. Testar execução
1. Abra modal de um dataset ACTIVE
2. Clique em "▶ Executar"
3. Deve aparecer toast de sucesso
4. Modal fecha automaticamente
5. Dashboard atualiza com novo item pendente

### 4. Verificar logs
1. Abra console (F12)
2. Procure por logs `[V2 API]`
3. Todos os endpoints devem retornar `✓ ... - OK`

---

## 📋 Arquivos Modificados

1. **`public/v2.css`**
   - Adicionado: `.v2-modal[hidden] { display: none !important; }`

2. **`public/v2.js`**
   - Linha ~109: Adicionado verificação de Content-Type
   - Linha ~459: Corrigido rota do botão Executar
   - Linha ~501: Corrigido evento do overlay do modal
   - Linha ~513: Adicionado prevenção de propagação no conteúdo

---

## 🔗 Documentação Relacionada

- **Troubleshooting completo:** `TROUBLESHOOTING_V2.md`
- **Guia do usuário:** `docs/UI_V2_GUIDE.md`
- **Fluxos visuais:** `docs/UI_V2_FLOW.md`
- **Quick Start:** `QUICK_START_V2.md`

---

## 🚀 Próximos Passos

### Para o dataset `CON_CENTRO_CUSTO`:

1. **Verificar se tabela Oracle existe:**
   ```bash
   # Execute no Databricks Notebook
   python diagnose-oracle-table.py
   ```

2. **Se tabela não existir:**
   - Contate o DBA para verificar nome correto
   - Ou crie a view no Oracle
   - Ou atualize o dataset_name no portal

3. **Se tabela existir, executar dataset:**
   - Abra o dataset no portal V2
   - Clique em "▶ Executar"
   - Aguarde finalização
   - Verifique tabelas bronze/silver no Databricks

4. **Verificar tabelas criadas:**
   ```sql
   SELECT COUNT(*) FROM cm_dbx_dev.bronze_mega.CON_CENTRO_CUSTO;
   SELECT COUNT(*) FROM cm_dbx_dev.silver_mega.CON_CENTRO_CUSTO;
   ```

### Para o orchestrator:

O orchestrator está rodando mas não está executando os 6 jobs pendentes. Possíveis ações:

1. **Verificar logs do job no Databricks**
2. **Verificar se está processando items da fila**
3. **Se necessário, reiniciar o job**

---

## ✨ Melhorias Futuras (Sugestões)

1. **Adicionar botão "Ver Run" no modal** - Link direto para o job no Databricks
2. **Polling automático** - Atualizar status do dataset a cada X segundos
3. **Confirmação antes de executar** - Modal de confirmação
4. **Histórico visual** - Timeline das execuções no modal
5. **Filtros avançados** - Filtrar datasets por projeto/área/status
6. **Bulk actions** - Executar múltiplos datasets de uma vez

---

## 📞 Suporte

Se encontrar problemas:

1. **Force refresh** (Ctrl+Shift+R)
2. **Veja o console** (F12)
3. **Copie os logs** `[V2 API]`
4. **Veja logs do servidor** (terminal onde rodou `node server.js`)
5. **Consulte** `TROUBLESHOOTING_V2.md`

---

**Versão:** V2.1.0 (20/02/2026)
**Status:** ✅ Todas as correções aplicadas e testadas
