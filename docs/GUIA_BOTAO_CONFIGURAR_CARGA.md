# Guia: Como Acessar o Botão "Configurar Carga"

## Localização do Botão

O botão **"Configurar Carga"** NÃO aparece na lista de datasets. Ele está localizado na **página de detalhes de um dataset específico**.

## Passos para Acessar:

### 1. Lista de Datasets
Na tela atual que você está vendo (lista de datasets), você precisa:
- **Clicar em qualquer linha/dataset** da tabela
- Isso abrirá a página de detalhes daquele dataset

### 2. Página de Detalhes do Dataset
Uma vez dentro da página de detalhes, você verá:
- **Header superior** com:
  - Botão de voltar (←)
  - Nome do dataset
  - Badge de status (ACTIVE, PAUSED, etc.)
  - **Área de ações à direita** com os botões:
    1. **⚙️ Configurar Carga** ← NOVO BOTÃO ADICIONADO
    2. **▶ Executar**
    3. **🗑️ Excluir** (ícone de lixeira)

### 3. Layout Visual

```
┌─────────────────────────────────────────────────────────────────────────┐
│ ←  CMASTER.GLO_GRUPO_USUARIO@CM...  [ACTIVE]                            │
│                                                                           │
│                                  [⚙️ Configurar Carga] [▶ Executar] [🗑️] │
└─────────────────────────────────────────────────────────────────────────┘
```

## Funcionalidade do Botão

Quando você clicar em **"⚙️ Configurar Carga"**, um diálogo será aberto com:

### Opções Disponíveis:

1. **Habilitar Carga Incremental** (Switch)
   - ON: Ativa carga incremental
   - OFF: Sempre fará carga FULL

2. **Modo de Escrita (Bronze)** (Dropdown)
   - SNAPSHOT: Sobrescreve tudo (OVERWRITE)
   - CURRENT: Merge incremental (UPSERT) ← Recomendado para incremental
   - APPEND_LOG: Apenas append

3. **Lookback Days** (Input numérico)
   - Padrão: 3 dias
   - Define quantos dias retroativos buscar da data delta
   - Para reprocessamento histórico: usar 30, 60, 90 dias

4. **Override Watermark** (Input texto - OPCIONAL)
   - Para reprocessamento manual
   - Formato: `2024-01-01 00:00:00`
   - Deixe vazio para usar watermark normal

## Exemplo de Uso: Carga Incremental dos Últimos 30 Dias

1. Clique no dataset desejado na lista
2. Na página de detalhes, clique em **"⚙️ Configurar Carga"**
3. No diálogo:
   - ✅ **Habilitar Carga Incremental**: ON
   - **Modo de Escrita**: CURRENT (merge/upsert)
   - **Lookback Days**: 30
   - **Override Watermark**: (deixe vazio, a menos que queira forçar uma data específica)
4. Clique em **"Salvar Configurações"**
5. Clique em **"▶ Executar"** para iniciar a carga

## Verificação na Aba "Execuções"

Após executar, você pode verificar na aba **"Execuções"** do dataset:
- **Tipo Carga**: Badge azul (FULL) ou verde (INCREMENTAL)
- **Δ Incremental**: Quantidade de linhas incrementais lidas
- **Detalhes expandidos**: Watermark start/end da execução

## Troubleshooting

### "Não vejo o botão"
- ✅ Certifique-se de estar na **página de detalhes** (não na lista)
- ✅ Verifique se o frontend foi recompilado: `npm run build`
- ✅ Recarregue a página (Ctrl+F5)

### "O diálogo não abre"
- Verifique o console do navegador (F12) para erros
- Confirme que o componente `IncrementalConfigDialog.tsx` existe

### "Erro ao salvar"
- Verifique se o backend está rodando
- Confirme que o endpoint `/api/portal/datasets/:id/incremental-config` está disponível
- Valores válidos para `bronze_mode`: SNAPSHOT, CURRENT, APPEND_LOG
