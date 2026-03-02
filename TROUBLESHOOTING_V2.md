# Troubleshooting - Interface V2

## 🔍 Problemas Identificados

### 1. Dataset no Portal mas não no Databricks Catalog

**Sintoma:** Dataset `CON_CENTRO_CUSTO` aparece no portal mas as tabelas não existem no Databricks.

**Causa:** O dataset foi criado mas nunca foi executado com sucesso, ou a execução falhou.

**Solução:**

1. **Verifique o histórico de execuções:**
   ```sql
   SELECT * 
   FROM cm_dbx_dev.ingestion_sys_ops.run_queue
   WHERE dataset_id = '1f59c444-9ddd-4acf-88ee-08d6894d40c8'
   ORDER BY requested_at DESC
   LIMIT 10;
   ```

2. **Execute o dataset manualmente:**
   - No portal V2, clique no dataset
   - Clique em "▶ Executar"
   - Aguarde a execução completar

3. **Se a execução falhar, verifique:**
   - Conexão Oracle está funcionando?
   - Tabela/view `CMASTER.CON_CENTRO_CUSTO` existe no Oracle?
   - Use o script `diagnose-oracle-table.py` para diagnosticar

4. **Após sucesso, verifique:**
   ```sql
   -- Bronze criada?
   SELECT COUNT(*) FROM cm_dbx_dev.bronze_mega.CON_CENTRO_CUSTO;
   
   -- Silver criada?
   SELECT COUNT(*) FROM cm_dbx_dev.silver_mega.CON_CENTRO_CUSTO;
   ```

---

### 2. Job em Execução Não Aparece no Dashboard

**Sintoma:** Orchestrator rodando mas dashboard mostra "running: 0"

**Causa:** O job que está rodando é o **orchestrator em si**, não um job de dataset.

**Explicação:**
- O dashboard monitora apenas **jobs de datasets** (bronze/silver)
- O job `governed_ingestion_orchestrator` é o controlador que executa os datasets
- O orchestrator pega itens da fila e executa cada dataset como um job separado

**Como funciona:**
```
1. Orchestrator roda continuamente (job que você vê no Databricks)
2. Ele verifica a fila (run_queue) a cada X segundos
3. Para cada item PENDING na fila:
   - Cria um job para executar o dataset
   - Marca como RUNNING
   - Executa bronze → silver
4. Dashboard conta apenas os jobs de datasets, não o orchestrator
```

**O que você vê:**
- ✅ Orchestrator rodando no Databricks (correto)
- ❌ Dashboard mostra 0 running (porque não há datasets sendo executados neste momento)
- ⚠️ 6 pendentes (datasets aguardando execução)

**Status "INATIVO":**
O orchestrator está marcado como inativo porque não processou nenhum dataset nos últimos 5 minutos. Possíveis causas:

1. **Fila vazia** - Não há datasets para processar
2. **Orchestrator pausado** - Verifique se está realmente executando itens da fila
3. **Erro no orchestrator** - Veja os logs do job no Databricks

**Solução:**
```bash
# Verifique se há itens pendentes
curl http://localhost:3000/api/portal/orchestrator/status

# Se há pendentes mas não estão executando:
# 1. Verifique logs do orchestrator no Databricks
# 2. Veja se o job está realmente pegando itens da fila
```

---

### 3. Erro API: "Unexpected token '<'"

**Sintoma:**
```
API Error: SyntaxError: Unexpected token '<', "<!doctype "... is not valid JSON
```

**Causa:** Algum endpoint da API está retornando **HTML** ao invés de **JSON**.

**Como descobrir qual endpoint:**

1. **Force refresh:** Ctrl+Shift+R
2. **Abra o console:** F12 → Console
3. **Procure por logs:** `[V2 API] ❌ ... retornou ... ao invés de JSON`

**Endpoints possíveis:**
- `/api/portal/projects`
- `/api/portal/connections/oracle`
- `/api/portal/datasets`
- `/api/portal/dashboard/summary`
- `/api/portal/orchestrator/status`

**Teste manualmente:**
```bash
# PowerShell
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/projects" | Select-Object StatusCode, ContentType

# Se ContentType = text/html, o endpoint está com problema
```

**Soluções comuns:**

1. **Servidor não está rodando:**
   ```bash
   node server.js
   ```

2. **Rota não existe:**
   - Verifique `src/portalRoutes.js`
   - Adicione a rota faltando

3. **Erro no código da rota:**
   - Veja os logs do servidor
   - A rota pode estar lançando exceção

---

## 🧪 Testes Rápidos

### Teste 1: API está OK?
```bash
# PowerShell
Invoke-WebRequest -Uri "http://localhost:3000/api/portal/dashboard/summary" | ConvertFrom-Json
```

### Teste 2: Databricks conecta?
```bash
node test-connection.js
```

### Teste 3: Datasets estão cadastrados?
```sql
SELECT 
  dataset_id,
  dataset_name,
  execution_state,
  bronze_table,
  silver_table
FROM cm_dbx_dev.ingestion_sys_ctrl.datasets
ORDER BY created_at DESC;
```

### Teste 4: Tabelas bronze/silver existem?
```sql
-- Listar todas as tabelas bronze
SHOW TABLES IN cm_dbx_dev.bronze_mega;

-- Listar todas as tabelas silver
SHOW TABLES IN cm_dbx_dev.silver_mega;
```

---

## 📋 Checklist de Verificação

### Quando um dataset não aparece no Databricks:

- [ ] Dataset foi executado pelo menos uma vez com sucesso?
- [ ] Verifique histórico na tabela `run_queue`
- [ ] Última execução tem status `SUCCESS`?
- [ ] Tabela fonte existe no Oracle?
- [ ] Conexão Oracle está configurada corretamente?

### Quando orchestrator está "inativo":

- [ ] Job `governed_ingestion_orchestrator` está rodando?
- [ ] Há itens `PENDING` na fila?
- [ ] Orchestrator processou algo nos últimos 5 minutos?
- [ ] Veja logs do job no Databricks para erros

### Quando API retorna HTML:

- [ ] Servidor Node.js está rodando?
- [ ] Porta 3000 está livre?
- [ ] Rota está definida em `portalRoutes.js`?
- [ ] Veja logs do servidor para erros

---

## 🚑 Ações Emergenciais

### Se nada funciona:

1. **Reinicie o servidor:**
   ```bash
   # Ctrl+C para parar
   node server.js
   ```

2. **Limpe o cache do navegador:**
   - Ctrl+Shift+R

3. **Verifique se há jobs travados:**
   ```sql
   SELECT * 
   FROM cm_dbx_dev.ingestion_sys_ops.run_queue
   WHERE status = 'RUNNING'
   AND requested_at < CURRENT_TIMESTAMP - INTERVAL 1 HOUR;
   ```

4. **Reprocesse um dataset:**
   ```bash
   curl -X POST http://localhost:3000/api/portal/run-queue \
     -H "Content-Type: application/json" \
     -d '{
       "dataset_id": "1f59c444-9ddd-4acf-88ee-08d6894d40c8",
       "trigger_type": "MANUAL"
     }'
   ```

---

## 📞 Próximos Passos

1. **Force refresh** (Ctrl+Shift+R)
2. **Abra o console** (F12)
3. **Veja qual endpoint está falhando**
4. **Me envie:**
   - Print do console com os logs `[V2 API]`
   - Status HTTP do endpoint que falhou
   - Logs do servidor Node.js

---

## 🔗 Links Úteis

- Databricks Jobs: https://dbc-c9eab3b3-1f5f.cloud.databricks.com/jobs
- Orchestrator: https://dbc-c9eab3b3-1f5f.cloud.databricks.com/jobs/690887429046802
- Portal V2: http://localhost:3000/v2.html
- API Docs: http://localhost:3000/api-docs (se existir)
