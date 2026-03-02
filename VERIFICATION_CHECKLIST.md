# ✅ Checklist de Verificação - Datasets Page

## 🎯 Acesse a Aplicação
**URL**: http://localhost:3000/#/datasets

---

## 📋 Verificações Obrigatórias

### 1. ✅ Filtro de Source Type
- [ ] Abrir dropdown "Source"
- [ ] **Deve mostrar**: ORACLE e SUPABASE
- [ ] **NÃO deve mostrar**: SHAREPOINT
- [ ] Badge do SUPABASE deve ser **verde** (`bg-green-100 text-green-800`)

**Como testar:**
```
1. Ir em http://localhost:3000/#/datasets
2. Clicar no dropdown "Source" (ao lado do filtro de Status)
3. Verificar as opções disponíveis
```

---

### 2. ✅ Botões de Visualização (View Toggle)
- [ ] Devem aparecer **apenas 2 botões**: Lista 📋 e Cards 🗃️
- [ ] **NÃO deve aparecer**: Botão "DataOps Grid" com ícone de banco de dados
- [ ] Ao clicar em "Lista", deve mostrar tabela
- [ ] Ao clicar em "Cards", deve mostrar cards

**Como testar:**
```
1. Olhar para a barra superior (ao lado da busca)
2. Contar quantos botões de visualização existem (deve ser 2)
3. Alternar entre Lista e Cards
```

---

### 3. ✅ Colunas da Tabela (View: Lista)
**Colunas que DEVEM aparecer (12 colunas):**
- [ ] ☑️ Checkbox (seleção)
- [ ] Dataset (nome + ID)
- [ ] Status (ACTIVE, PAUSED, etc.)
- [ ] Source (ORACLE, SUPABASE)
- [ ] Projeto
- [ ] Área
- [ ] Bronze (nome da tabela)
- [ ] Silver (nome da tabela)
- [ ] Schema (versão do schema)
- [ ] Estratégia (badges: 🟢 WATERMARK, 🔵 FULL REFRESH, etc.)
- [ ] Criado em (data)
- [ ] ⋯ (menu de ações)

**Colunas que NÃO devem aparecer:**
- [ ] ❌ "Tipo Carga" (INCREMENTAL/SNAPSHOT/FULL)
- [ ] ❌ "Coluna Incremental" (watermark_column)

**Como testar:**
```
1. Garantir que está na view "Lista" (botão de tabela ativo)
2. Contar as colunas do cabeçalho da tabela
3. Verificar que não há colunas extras "Tipo Carga" ou "Coluna Incremental"
```

---

### 4. ✅ View Cards
- [ ] Ao clicar em "Cards", deve mostrar cards em grid (3 colunas em telas grandes)
- [ ] Cada card deve ter:
  - [ ] Checkbox de seleção
  - [ ] Ícone de banco de dados
  - [ ] Nome do dataset
  - [ ] Badge de status
  - [ ] Informações (Source, Projeto, Área, Bronze, Silver, Schema)
  - [ ] Data de criação
  - [ ] Ícone de seta ao hover

**Como testar:**
```
1. Clicar no botão "Cards" (ícone de grade)
2. Verificar que os cards aparecem em formato de grid
3. Passar o mouse sobre um card e ver se a seta aparece
```

---

### 5. ✅ Funcionalidades Intactas
**Todas as funcionalidades abaixo devem continuar funcionando:**

#### Busca
- [ ] Digitar no campo de busca filtra datasets em tempo real

#### Filtros
- [ ] Filtro de Status funciona (ACTIVE, PAUSED, etc.)
- [ ] Filtro de Source funciona (ORACLE, SUPABASE)
- [ ] Filtro de Projeto funciona
- [ ] Filtro de Área funciona
- [ ] Botão "Limpar filtros" funciona

#### Seleção e Ações em Massa
- [ ] Checkbox individual seleciona dataset
- [ ] Checkbox "Selecionar todos" funciona
- [ ] Ao selecionar datasets, aparece barra de ações com:
  - [ ] ▶ Executar
  - [ ] 💾 CSV (exportar)
  - [ ] ✏️ Renomear
  - [ ] ❌ Excluir

#### Ordenação
- [ ] Clicar nos cabeçalhos das colunas ordena (nome, status, data, etc.)
- [ ] Ícone de seta indica direção da ordenação

#### Paginação
- [ ] Seletor de items por página funciona (20, 50, 100)
- [ ] Botões "Anterior" e "Próximo" funcionam
- [ ] Mostra "Página X de Y"

---

## 🧪 Testes Específicos

### Teste 1: Dataset ORACLE
```
1. Criar ou encontrar um dataset ORACLE
2. Verificar que o badge "ORACLE" é laranja (bg-orange-100)
3. Ver detalhes do dataset
4. Voltar para lista
```

### Teste 2: Dataset SUPABASE
```
1. Criar ou encontrar um dataset SUPABASE
2. Verificar que o badge "SUPABASE" é verde (bg-green-100)
3. Ver detalhes do dataset
4. Voltar para lista
```

### Teste 3: Bulk Rename (Nova Funcionalidade)
```
1. Selecionar 2+ datasets
2. Clicar em "Renomear"
3. Escolher operação (REPLACE_SCHEMA_PREFIX)
4. Preencher campos "De" e "Para"
5. Clicar "Gerar Preview"
6. Verificar preview das mudanças
7. (Opcional) Confirmar renomeação
```

### Teste 4: Hot Reload (Desenvolvimento)
```
1. Com os serviços rodando, editar qualquer texto em Datasets.tsx
2. Salvar o arquivo
3. Voltar ao navegador (http://localhost:3000/#/datasets)
4. A mudança deve aparecer INSTANTANEAMENTE (sem refresh manual)
```

---

## 🚨 Problemas Comuns

### Problema: Mudanças não aparecem
**Solução:**
```powershell
# Limpar cache do navegador (Ctrl+Shift+Delete)
# Ou forçar reload (Ctrl+F5 ou Ctrl+Shift+R)

# Verificar se Vite está rodando:
Get-NetTCPConnection -LocalPort 3000
```

### Problema: Erro 404 na API
**Solução:**
```powershell
# Verificar se backend está rodando:
Get-NetTCPConnection -LocalPort 3001

# Se não estiver, reiniciar:
cd C:\dev\cm-databricks
$env:PORT=3001
node server.js
```

### Problema: Página em branco
**Solução:**
```
1. Abrir DevTools (F12)
2. Ver console para erros
3. Verificar se há erros de import/compilação
4. Reiniciar Vite se necessário
```

---

## 📊 Status dos Serviços

**Backend (API)**: 
- ✅ Porta: 3001
- ✅ Rodando em nova janela PowerShell

**Frontend (Vite)**:
- ✅ Porta: 3000
- ✅ Rodando em nova janela PowerShell
- ✅ Hot reload ativo

---

## ✅ Resumo das Mudanças

### O que FOI IMPLEMENTADO:
1. ✅ Source options: ORACLE + SUPABASE (removido SHAREPOINT)
2. ✅ View modes: apenas Lista e Cards (removido DataOps Grid)
3. ✅ Tabela simplificada (removidas colunas "Tipo Carga" e "Coluna Incremental")
4. ✅ Limpeza de código (removidos imports e state do DataOps)
5. ✅ Badge verde para SUPABASE
6. ✅ Todas as funcionalidades anteriores mantidas (busca, filtros, seleção, etc.)

### O que NÃO MUDOU (funciona normalmente):
- Busca de datasets
- Filtros (Status, Projeto, Área)
- Seleção múltipla
- Ações em massa (Executar, CSV, Renomear, Excluir)
- Visualização de detalhes
- Paginação
- Ordenação

---

## 🎉 Se Tudo Estiver OK

Você deve ver:
- ✅ 2 botões de visualização (não 3)
- ✅ Filtro de Source com ORACLE e SUPABASE
- ✅ Tabela com 12 colunas (sem "Tipo Carga" e "Coluna Incremental")
- ✅ Badge verde para datasets SUPABASE
- ✅ Todas as funcionalidades funcionando normalmente

**Data de Verificação**: __________

**Verificado por**: __________

**Resultado**: [ ] Aprovado  [ ] Correções necessárias
