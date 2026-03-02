# Troubleshooting: Dados Incrementais Não Aparecem no Monitoramento

## 🔍 Diagnóstico

O frontend **JÁ ESTÁ PREPARADO** para mostrar as informações incrementais, mas os dados não aparecem porque:

### ✅ O que JÁ EXISTE no Frontend:

1. **ExecutionsTab.tsx** (linhas 138-162):
   - Coluna "Tipo Carga" com badges coloridos
   - Coluna "Δ Incremental" para mostrar linhas incrementais

2. **RunDetailPanel.tsx** (linhas 160-186):
   - Seção verde destacada para cargas incrementais
   - Mostra: linhas incrementais, watermark_start, watermark_end

### ❌ O que está faltando:

**Os dados não estão chegando da API!**

---

## 🛠️ Checklist de Verificação

### 1. Backend - Migration 005 Executada?

```sql
-- Verificar se colunas existem
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'batch_process' 
  AND column_name IN ('load_type', 'incremental_rows_read', 'watermark_start', 'watermark_end');
```

**Esperado**: 4 linhas retornadas

**Se retornar vazio**:
```sql
-- Executar migration 005
ALTER TABLE batch_process 
ADD COLUMN load_type STRING,
ADD COLUMN incremental_rows_read BIGINT,
ADD COLUMN watermark_start STRING,
ADD COLUMN watermark_end STRING;
```

---

### 2. Backend - Endpoint Retornando Dados?

**Endpoint**: `GET /api/portal/monitor/batch-processes/recent`

**Testar**:
```bash
curl http://localhost:3000/api/portal/monitor/batch-processes/recent
```

**Verificar resposta**:
```json
{
  "items": [
    {
      "run_id": "...",
      "load_type": "INCREMENTAL",           ← Deve estar presente
      "incremental_rows_read": 12345,      ← Deve estar presente
      "watermark_start": "2024-02-20...",  ← Deve estar presente
      "watermark_end": "2024-02-26..."     ← Deve estar presente
    }
  ]
}
```

**Se load_type = null**:
- Verificar se endpoint está fazendo SELECT dos novos campos
- Verificar linha 939-951 em `src/portalRoutes.js`

---

### 3. Notebook Python - Populando Dados?

**Arquivo**: `databricks_notebooks/governed_ingestion_orchestrator.py`

**Verificar**:
1. Linha 1143-1150: Variáveis inicializadas?
2. Linha 1245-1299: Lógica de captura implementada?
3. Linha 1402-1405: Chamada `_finish_batch_process` com parâmetros?

**Se não estiver**:
- Revisar `docs/NOTEBOOK_UPDATES_INCREMENTAL_TRACKING.md`
- Aplicar as mudanças no notebook

---

### 4. Dados Existentes - Retroativo

**Problema**: Execuções antigas não têm os novos campos (eram NULL antes)

**Solução**: Apenas novas execuções terão os dados

**Para testar**:
1. Configure um dataset com carga incremental
2. Execute uma nova carga
3. Verifique se aparece no monitoramento

---

## 🐛 Debug Passo a Passo

### Passo 1: Verificar se o Notebook Populou os Dados

```sql
-- No Databricks SQL ou via MCP
SELECT 
  run_id,
  dataset_id,
  status,
  load_type,
  incremental_rows_read,
  watermark_start,
  watermark_end,
  started_at
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE started_at >= CURRENT_DATE - INTERVAL 1 DAY
ORDER BY started_at DESC
LIMIT 10;
```

**Se load_type = NULL**:
- Notebook não está populando
- Verificar se versão nova do notebook foi deployada no Databricks

**Se load_type tem valor**:
- ✅ Notebook OK
- Problema está no backend/frontend

---

### Passo 2: Verificar Query do Backend

**Arquivo**: `src/portalRoutes.js`

**Linha 939**:
```javascript
SELECT 
  bp.run_id,
  bp.dataset_id,
  bp.status,
  bp.started_at,
  bp.finished_at,
  bp.bronze_row_count,
  bp.silver_row_count,
  bp.load_type,                    ← DEVE ESTAR AQUI
  bp.incremental_rows_read,        ← DEVE ESTAR AQUI
  bp.watermark_start,              ← DEVE ESTAR AQUI
  bp.watermark_end,                ← DEVE ESTAR AQUI
  ...
FROM ${portalCfg.opsSchema}.batch_process bp
```

**Se não estiver**:
```javascript
// Adicionar após bronze_row_count, silver_row_count
bp.load_type,
bp.incremental_rows_read,
bp.watermark_start,
bp.watermark_end,
```

---

### Passo 3: Verificar Response da API (Browser)

1. Abrir DevTools (F12)
2. Aba "Network"
3. Filtrar por "batch-processes"
4. Clicar em uma requisição
5. Aba "Response"

**Verificar se JSON contém**:
```json
{
  "items": [
    {
      "load_type": "INCREMENTAL",
      "incremental_rows_read": 5000,
      ...
    }
  ]
}
```

**Se não contém**:
- Backend não está retornando os campos
- Verificar query SQL no backend

**Se contém mas não aparece no frontend**:
- Cache do navegador
- Ctrl + Shift + R para hard reload

---

### Passo 4: Console do Navegador

```javascript
// No Console do DevTools
// Verificar se dados estão chegando
console.log(data); // Na ExecutionsTab linha 75
```

---

## 🎯 Solução Rápida

### Se NADA aparecer:

1. **Backend**: Verificar se query inclui os campos (portalRoutes.js:939)
2. **Banco**: Verificar se migration foi executada
3. **Notebook**: Verificar se versão nova está no Databricks
4. **Testar**: Executar um dataset manualmente e verificar

### Comando Rápido de Teste:

```sql
-- Inserir dados de teste manualmente
UPDATE cm_dbx_dev.ingestion_sys_ops.batch_process
SET 
  load_type = 'INCREMENTAL',
  incremental_rows_read = 1234,
  watermark_start = '2024-02-20 10:00:00',
  watermark_end = '2024-02-26 14:00:00'
WHERE run_id = '<ultimo_run_id>'
AND load_type IS NULL;
```

Recarregue o monitoramento. Se aparecer, significa que:
- ✅ Frontend OK
- ✅ Backend OK
- ❌ Notebook não está populando

---

## 📊 Onde os Dados Devem Aparecer

### 1. Página de Monitoramento

**URL**: `/monitor`

**Aba "Execuções"**:

```
┌─────────────┬────────┬─────────┬────────────┬────────────┬─────────┬────────────┐
│ Dataset     │ Status │ Início  │ Tipo Carga │ Bronze     │ Δ Incr. │ Silver     │
├─────────────┼────────┼─────────┼────────────┼────────────┼─────────┼────────────┤
│ CIVIL_10465 │ ✓      │ 14:43   │ INCREMEN.. │ 122.004    │ 5.000   │ 122.004    │
│ GLO_GRUPO   │ ✓      │ 14:20   │ FULL       │ 609        │ —       │ 609        │
└─────────────┴────────┴─────────┴────────────┴────────────┴─────────┴────────────┘
```

**Ao expandir a linha**:
- Box verde com detalhes incrementais
- Watermark Start e End

---

### 2. Página de Dataset

**URL**: `/datasets/<id>`

**Aba "Execuções"**:
- Mesmas informações da página de monitoramento
- Ao expandir: box verde com watermarks

---

## 🔧 Fix Manual se Necessário

### Se o problema persistir após verificações:

1. **Limpar cache do navegador completamente**
2. **Rebuild do frontend**:
   ```bash
   cd frontend
   npm run build
   ```
3. **Restart do backend**:
   ```bash
   cd ..
   npm run dev  # ou pm2 restart portal
   ```
4. **Executar um novo dataset**
5. **Verificar logs do backend**:
   ```bash
   # Procurar por:
   [COLUMNS_PREVIEW]
   [UPDATE_INCREMENTAL_CONFIG]
   ```

---

## 📞 Suporte

Se após todas as verificações o problema persistir:

1. Verificar logs do Databricks Job
2. Confirmar que notebook tem versão atualizada
3. Confirmar que migration 005 foi executada
4. Testar insert manual conforme comando acima

---

**Versão**: 1.0.0  
**Data**: 26/02/2026  
**Autor**: Oz (AI Agent)
