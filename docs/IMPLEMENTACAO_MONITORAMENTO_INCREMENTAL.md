# Implementação: Monitoramento e Controle de Cargas Incrementais

## 📋 Visão Geral

Este documento descreve as melhorias implementadas no sistema de monitoramento para:

1. **Exibir informações sobre tipo de carga** (FULL vs INCREMENTAL) e quantidade de linhas incrementais nos logs
2. **Permitir que o usuário configure e execute cargas incrementais** com parâmetros customizados (lookback days, watermark override, etc.)

---

## 🗄️ Alterações no Banco de Dados

### 1. Adicionar campos na tabela `batch_process`

Execute a seguinte migration no Databricks:

```sql
-- Migration: Tracking de informações de carga incremental
ALTER TABLE cm_dbx_dev.ingestion_sys_ops.batch_process ADD COLUMNS (
  load_type STRING COMMENT 'Tipo de carga executada: FULL | INCREMENTAL | SNAPSHOT',
  incremental_rows_read INT COMMENT 'Número de linhas lidas na carga incremental (apenas novos/atualizados)',
  watermark_start STRING COMMENT 'Valor inicial do watermark usado (para cargas incrementais)',
  watermark_end STRING COMMENT 'Valor final do watermark após a execução'
);
```

**Campos adicionados:**
- `load_type`: Indica se foi uma carga FULL ou INCREMENTAL
- `incremental_rows_read`: Quantidade específica de linhas incrementais processadas
- `watermark_start` / `watermark_end`: Range do watermark usado na execução

---

## 🔧 Alterações no Backend

### 1. Atualizar endpoint de monitoramento (portalRoutes.js)

**Localização:** `src/portalRoutes.js` (linha ~940)

**Modificação:**
Adicionar os novos campos na query do endpoint `/api/portal/monitor/batch-processes/recent`:

```javascript
sqlQueryObjects(
  `SELECT bp.run_id, bp.dataset_id, bp.status, bp.started_at, bp.finished_at,\n` +
    `       bp.bronze_row_count, bp.silver_row_count, bp.error_class, bp.error_message,\n` +
    `       bp.load_type, bp.incremental_rows_read, bp.watermark_start, bp.watermark_end,\n` +
    `       CAST(TIMESTAMPDIFF(SECOND, bp.started_at, bp.finished_at) AS BIGINT) AS duration_seconds,\n` +
    `       dc.dataset_name, dc.incremental_strategy, dc.enable_incremental\n` +
    `FROM ${portalCfg.opsSchema}.batch_process bp\n` +
    `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON bp.dataset_id = dc.dataset_id\n` +
    `${whereClause}\n` +
    `ORDER BY bp.started_at DESC\n` +
    `LIMIT ${pageSize} OFFSET ${offset}`
)
```

**Também atualizar:**
- `/api/portal/runs/:runId` (linha ~650)
- `/api/portal/datasets/:datasetId/runs` (linha ~615)

### 2. Novo endpoint para atualizar configurações incrementais

**✅ Implementado em:** `src/portalRoutes.js` (linha 2466)

**Endpoint:** `PATCH /api/portal/datasets/:datasetId/incremental-config`

**Funcionalidades:**
- Habilitar/desabilitar carga incremental (`enable_incremental`)
- Alterar modo de escrita bronze (`bronze_mode`: SNAPSHOT | CURRENT | APPEND_LOG)
- Definir lookback days via `incremental_metadata`
- Override manual de watermark para reprocessamento histórico

**Exemplo de uso:**
```bash
curl -X PATCH http://localhost:3000/api/portal/datasets/DATASET_ID/incremental-config \
  -H "Content-Type: application/json" \
  -d '{
    "enable_incremental": true,
    "bronze_mode": "CURRENT",
    "incremental_metadata": {
      "lookback_days": 30
    },
    "override_watermark_value": "2024-01-01 00:00:00"
  }'
```

---

## 💻 Alterações no Frontend

### 1. Exibir tipo de carga e linhas incrementais no ExecutionsTab

**✅ Implementado em:** `frontend/src/components/monitor/ExecutionsTab.tsx`

**Mudanças:**
- Nova coluna **"Tipo Carga"** com badge colorido (azul=FULL, verde=INCREMENTAL)
- Nova coluna **"Δ Incremental"** mostrando quantidade de linhas incrementais

**Visual:**
```
| Dataset | Status | Tipo Carga | Bronze | Δ Incremental | Silver |
|---------|--------|------------|--------|---------------|--------|
| CLIENTES| OK     | INCREMENTAL| 100k   | 2.5k          | 99k    |
| PRODUTOS| OK     | FULL       | 50k    | —             | 49k    |
```

### 2. Detalhes de watermark no RunDetailPanel

**✅ Implementado em:** `frontend/src/components/RunDetailPanel.tsx`

**Mudanças:**
- Seção destacada para cargas incrementais (fundo verde)
- Exibe: linhas incrementais, watermark início/fim

**Visual:**
```
┌─────────────────────────────────────────┐
│ → Carga Incremental                     │
│ Linhas Incrementais: 2,500              │
│ Watermark Início: 2024-02-20 00:00:00  │
│ Watermark Fim: 2024-02-26 23:59:59     │
└─────────────────────────────────────────┘
```

### 3. Novo componente: IncrementalConfigDialog

**✅ Criado em:** `frontend/src/components/IncrementalConfigDialog.tsx`

**Funcionalidades:**
- Switch para habilitar/desabilitar incremental
- Dropdown para selecionar modo de escrita (SNAPSHOT/CURRENT/APPEND_LOG)
- Input para configurar lookback days (padrão: 3 dias)
- Input avançado para override de watermark

**Como usar:**
Você pode adicionar um botão no detalhe do dataset ou na lista de execuções:

```tsx
import { IncrementalConfigDialog } from "@/components/IncrementalConfigDialog";

const [configDialogOpen, setConfigDialogOpen] = useState(false);
const [selectedDataset, setSelectedDataset] = useState(null);

// Botão para abrir configurações
<Button onClick={() => {
  setSelectedDataset(dataset);
  setConfigDialogOpen(true);
}}>
  ⚙️ Configurar Carga
</Button>

// Dialog
<IncrementalConfigDialog
  open={configDialogOpen}
  onOpenChange={setConfigDialogOpen}
  datasetId={selectedDataset?.dataset_id}
  datasetName={selectedDataset?.dataset_name}
  currentConfig={selectedDataset}
  onConfigUpdated={() => {
    // Recarregar dados do dataset
    fetchDataset();
  }}
/>
```

---

## 📝 Alterações no Notebook Python (Databricks)

### Atualizar logging no batch_process

Ao registrar execuções na tabela `batch_process`, adicionar os novos campos:

```python
# Determinar tipo de carga
load_type = "INCREMENTAL" if enable_incremental and incremental_strategy != "SNAPSHOT" else "FULL"

# Contar linhas incrementais (exemplo com watermark)
incremental_rows = df_bronze.count() if load_type == "INCREMENTAL" else None

# Capturar watermark range
watermark_start = None
watermark_end = None
if load_type == "INCREMENTAL" and watermark_col:
    watermark_start = str(min_watermark)  # valor inicial usado
    watermark_end = str(max_watermark)    # valor final processado

# INSERT na batch_process
spark.sql(f"""
INSERT INTO {ops_schema}.batch_process
  (run_id, dataset_id, queue_id, execution_mode, status, 
   started_at, finished_at, bronze_row_count, silver_row_count,
   load_type, incremental_rows_read, watermark_start, watermark_end,
   orchestrator_job_id, orchestrator_run_id, orchestrator_task)
VALUES (
  '{run_id}', '{dataset_id}', '{queue_id}', '{execution_mode}', '{status}',
  TIMESTAMP '{started_at}', TIMESTAMP '{finished_at}', {bronze_count}, {silver_count},
  '{load_type}', {incremental_rows or 'NULL'}, {f"'{watermark_start}'" if watermark_start else 'NULL'}, {f"'{watermark_end}'" if watermark_end else 'NULL'},
  '{orch_job_id}', '{orch_run_id}', '{orch_task}'
)
""")
```

---

## 🔄 Fluxo Completo de Uso

### Cenário 1: Visualizar tipo de carga nos logs

1. Usuário acessa o **Monitoramento > Execuções**
2. Na tabela, vê coluna **"Tipo Carga"** com badge FULL/INCREMENTAL
3. Se for INCREMENTAL, vê também a coluna **"Δ Incremental"** com quantidade de linhas
4. Ao clicar em **"Logs"**, vê detalhes completos incluindo range do watermark

### Cenário 2: Configurar carga incremental personalizada

1. Usuário acessa o **Dataset** desejado
2. Clica em botão **"Configurar Carga"** ou **"⚙️ Parâmetros"**
3. No dialog que abre:
   - **Habilita** carga incremental
   - Seleciona modo de escrita: **CURRENT** (merge)
   - Define **lookback days = 30** (ao invés dos 3 dias padrão)
   - Opcionalmente define **override watermark** para carga histórica específica
4. Clica em **"Salvar Configurações"**
5. Clica em **"Executar Dataset"**
6. Sistema usa os novos parâmetros configurados
7. Após execução, pode ver nos logs que foi processado 30 dias de dados incrementais

---

## 📊 Campos de Configuração Disponíveis

### Tabela `dataset_control` (já existente desde migration 004)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `enable_incremental` | BOOLEAN | Se TRUE, usa carga incremental; se FALSE, sempre FULL |
| `incremental_strategy` | STRING | WATERMARK \| HASH_MERGE \| SNAPSHOT \| APPEND_LOG |
| `bronze_mode` | STRING | SNAPSHOT \| CURRENT \| APPEND_LOG |
| `incremental_metadata` | STRING | JSON com configurações: `{"lookback_days": 30, "watermark_col": "UPDATED_AT"}` |
| `override_watermark_value` | STRING | Valor manual de watermark (ex: "2024-01-01 00:00:00"). NULL = usar watermark normal |

### Tabela `batch_process` (novos campos)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `load_type` | STRING | FULL \| INCREMENTAL \| SNAPSHOT |
| `incremental_rows_read` | INT | Quantidade de linhas incrementais processadas |
| `watermark_start` | STRING | Valor inicial do watermark |
| `watermark_end` | STRING | Valor final do watermark |

---

## ✅ Checklist de Implementação

### Backend
- [x] Adicionar campos na tabela `batch_process` (SQL migration)
- [x] Atualizar endpoint `/api/portal/monitor/batch-processes/recent` para incluir novos campos
- [x] Criar endpoint `PATCH /api/portal/datasets/:datasetId/incremental-config`
- [ ] Atualizar notebook Python para preencher novos campos no `batch_process`

### Frontend
- [x] Adicionar coluna "Tipo Carga" no `ExecutionsTab.tsx`
- [x] Adicionar coluna "Δ Incremental" no `ExecutionsTab.tsx`
- [x] Adicionar seção de detalhes incrementais no `RunDetailPanel.tsx`
- [x] Criar componente `IncrementalConfigDialog.tsx`
- [ ] Integrar botão de configuração na página de detalhes do dataset
- [ ] Adicionar menu "Configurar" na lista de datasets

### Documentação
- [x] Criar este documento de implementação
- [ ] Atualizar documentação de usuário com novo fluxo
- [ ] Criar tutorial de uso para equipe

---

## 🎯 Exemplos de Uso

### 1. Carga incremental padrão (3 dias)
```json
{
  "enable_incremental": true,
  "bronze_mode": "CURRENT",
  "incremental_metadata": {
    "lookback_days": 3
  }
}
```

### 2. Reprocessamento histórico (últimos 30 dias)
```json
{
  "enable_incremental": true,
  "bronze_mode": "CURRENT",
  "incremental_metadata": {
    "lookback_days": 30
  }
}
```

### 3. Carga incremental a partir de data específica
```json
{
  "enable_incremental": true,
  "bronze_mode": "CURRENT",
  "override_watermark_value": "2024-01-01 00:00:00"
}
```

### 4. Voltar para carga full
```json
{
  "enable_incremental": false,
  "bronze_mode": "SNAPSHOT"
}
```

---

## 🔍 Queries Úteis

### Ver configurações de carga de um dataset
```sql
SELECT 
  dataset_name,
  enable_incremental,
  incremental_strategy,
  bronze_mode,
  incremental_metadata,
  override_watermark_value
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = 'SEU_DATASET_ID';
```

### Ver histórico de execuções com tipo de carga
```sql
SELECT 
  bp.run_id,
  dc.dataset_name,
  bp.load_type,
  bp.incremental_rows_read,
  bp.bronze_row_count,
  bp.watermark_start,
  bp.watermark_end,
  bp.started_at,
  bp.status
FROM cm_dbx_dev.ingestion_sys_ops.batch_process bp
JOIN cm_dbx_dev.ingestion_sys_ctrl.dataset_control dc 
  ON bp.dataset_id = dc.dataset_id
WHERE dc.dataset_name = 'SEU_DATASET'
ORDER BY bp.started_at DESC
LIMIT 20;
```

### Estatísticas de cargas incrementais
```sql
SELECT 
  load_type,
  COUNT(*) as total_execucoes,
  AVG(incremental_rows_read) as media_linhas_incrementais,
  AVG(TIMESTAMPDIFF(SECOND, started_at, finished_at)) as media_duracao_seg
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE started_at >= current_timestamp() - INTERVAL 7 DAYS
  AND status = 'SUCCEEDED'
GROUP BY load_type;
```

---

## 📞 Suporte

Para dúvidas ou problemas com a implementação, entre em contato com o time de Engenharia de Dados.
