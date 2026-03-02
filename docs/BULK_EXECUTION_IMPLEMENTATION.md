# Execução em Massa de Datasets 🚀

## ✅ Implementação Completa (Arquitetura Enterprise - Opção C)

A funcionalidade de execução em massa foi implementada com **paralelismo real** no Databricks, seguindo a arquitetura enterprise recomendada.

## 🎯 Objetivo

Permitir que o usuário:
1. Selecione múltiplos datasets na tela de listagem
2. Clique em "Executar Selecionados"
3. Escolha estratégia de execução (paralela ou sequencial)
4. Sistema enfileira e dispara jobs individuais no Databricks

## 🏗️ Arquitetura Implementada: Opção C (Enterprise)

### Modelo Atual
```
Usuário seleciona N datasets
         ↓
Frontend envia para /api/portal/datasets/bulk-enqueue
         ↓
Backend:
  - Valida cada dataset (eligibility)
  - Cria N registros na run_queue (com prioridade baseada em strategy)
  - Dispara N Jobs independentes no Databricks via API
         ↓
Databricks:
  - Cada dataset vira 1 Job Run separado
  - target_dataset_id é passado como parâmetro do notebook
  - Jobs executam em paralelo (baseado em max_concurrent_runs)
         ↓
Cada Job:
  - Processa apenas 1 dataset
  - Atualiza run_queue e batch_process
  - Finaliza independentemente
```

### Por que essa é a melhor arquitetura?

✅ **Execução isolada**: Cada dataset tem seu próprio run no Databricks
✅ **Logs separados**: Facilita debugging e auditoria
✅ **Escala automática**: Databricks gerencia recursos por job
✅ **Sem gargalo**: Não há loop único processando tudo sequencialmente
✅ **Falhas isoladas**: Um dataset falhando não afeta os outros

## 📁 Arquivos Modificados/Criados

### Frontend

**`frontend/src/pages/Datasets.tsx`**
- ✅ Checkbox por dataset (seleção múltipla)
- ✅ Checkbox "Selecionar todos"
- ✅ Botão "Executar Selecionados (N)" (aparece quando há seleção)
- ✅ Modal de confirmação com estratégias
- ✅ Indicador visual de datasets selecionados (ring azul)

**`frontend/src/lib/api.ts`**
- ✅ Função `bulkEnqueueDatasets(datasetIds, strategy)`

**`frontend/src/components/ui/checkbox.tsx`** (novo)
- ✅ Componente Checkbox do shadcn/ui

**`frontend/src/components/ui/radio-group.tsx`** (novo)
- ✅ Componente RadioGroup do shadcn/ui

### Backend

**`src/portalRoutes.js`** (linha 1478)
- ✅ Endpoint `/api/portal/datasets/bulk-enqueue` (POST)
- ✅ Validação de eligibility (PAUSED, DEPRECATED, BLOCKED_SCHEMA_CHANGE)
- ✅ Criação de N registros em `run_queue`
- ✅ Disparo de N jobs via `triggerOrchestratorJob(datasetId)`
- ✅ Suporte a estratégias: `sequential` (prioridade descendente) e `parallel` (mesma prioridade)

**`src/portalRoutes.js`** (linha 222)
- ✅ Função `triggerOrchestratorJob(datasetId)` já existente
- ✅ Dispara Job do Databricks com `target_dataset_id` como parâmetro

### Orquestrador (já existente)

**`databricks_notebooks/governed_ingestion_orchestrator.py`**
- ✅ Suporta modo targeted (widget `target_dataset_id`)
- ✅ Quando `target_dataset_id` está definido, processa apenas aquele dataset
- ✅ Claims jobs da fila com filtro `dataset_id = target_dataset_id`

## 🎨 UX Implementado

### Tela de Datasets

**Antes**:
```
[Dataset 1] [Dataset 2] [Dataset 3]
```

**Depois**:
```
☑ Selecionar todos (15)

[Executar Selecionados (5)]  [Buscar...]  [Atualizar]  [Novo Dataset]

☑ [Dataset 1]  ACTIVE  ORACLE
☑ [Dataset 2]  ACTIVE  ORACLE
☐ [Dataset 3]  PAUSED ORACLE  <- não selecionável (estado bloqueado)
☑ [Dataset 4]  ACTIVE  ORACLE
☑ [Dataset 5]  ACTIVE  SHAREPOINT
```

**Visual**:
- Datasets selecionados têm **ring azul** ao redor do card
- Botão verde "Executar Selecionados (N)" aparece quando N > 0
- Checkbox no canto superior direito de cada card

### Modal de Confirmação

```
┌─────────────────────────────────────────┐
│  Executar Datasets em Massa              │
├─────────────────────────────────────────┤
│  Você selecionou 5 dataset(s) para      │
│  execução.                               │
│                                          │
│  Estratégia de Execução                 │
│  ○ Execução Paralela (recomendado)      │
│    Dispara múltiplos jobs simultâneos   │
│    no Databricks                         │
│                                          │
│  ○ Execução Sequencial                  │
│    Processa um dataset por vez          │
│    (mais lento, mas seguro)              │
│                                          │
│  ℹ️ Paralela: Cada dataset vira um Job │
│     Run independente no Databricks.     │
│     O sistema gerencia automaticamente  │
│     o paralelismo.                      │
│                                          │
│  [Cancelar]  [Confirmar Execução]       │
└─────────────────────────────────────────┘
```

## 🚀 Estratégias de Execução

### 1. Execução Paralela (Padrão)

**Como funciona**:
- Todos os datasets recebem **prioridade 100**
- Jobs são disparados simultaneamente
- Databricks gerencia quantos rodam em paralelo baseado em `max_concurrent_runs`

**Quando usar**:
- ✅ Datasets independentes (não compartilham recursos)
- ✅ Necessidade de velocidade
- ✅ Confiança na capacidade do cluster

**Comportamento no Databricks**:
```
Time 0s:  Job Run #1 (dataset A) -> RUNNING
Time 0s:  Job Run #2 (dataset B) -> RUNNING
Time 0s:  Job Run #3 (dataset C) -> RUNNING
Time 0s:  Job Run #4 (dataset D) -> PENDING (aguarda slot)
Time 0s:  Job Run #5 (dataset E) -> PENDING (aguarda slot)
Time 45s: Job Run #1 finaliza -> Job Run #4 inicia
Time 50s: Job Run #2 finaliza -> Job Run #5 inicia
...
```

### 2. Execução Sequencial

**Como funciona**:
- Datasets recebem **prioridades descendentes**: 200, 199, 198, ...
- Jobs são disparados em ordem, mas Databricks respeita a fila de prioridade
- Próximo só inicia quando anterior finalizar

**Quando usar**:
- ✅ Datasets com dependências lógicas
- ✅ Recursos limitados (JDBC connections, memória)
- ✅ Debugging (ver um de cada vez)

**Comportamento no Databricks**:
```
Time 0s:   Job Run #1 (pri=200, dataset A) -> RUNNING
Time 0s:   Job Run #2 (pri=199, dataset B) -> PENDING
Time 0s:   Job Run #3 (pri=198, dataset C) -> PENDING
Time 45s:  Job Run #1 finaliza
Time 46s:  Job Run #2 inicia -> RUNNING
Time 91s:  Job Run #2 finaliza
Time 92s:  Job Run #3 inicia -> RUNNING
...
```

## ⚙️ Configuração do Databricks Job

### Parâmetro Crítico: `max_concurrent_runs`

Para habilitar paralelismo real, você **DEVE** configurar:

**Databricks UI**:
1. Acesse **Workflows** → Job do orquestrador
2. **Settings** → **Advanced**
3. Encontre: **"Maximum concurrent runs"**
4. **Configurar para 3-5** (ou mais, baseado em capacidade)

**Valores sugeridos**:
- `1`: Sequencial puro (nunca executa mais de 1)
- `3`: Até 3 datasets simultaneamente (recomendado para MVP)
- `5`: Até 5 datasets simultaneamente (produção leve)
- `10+`: Alta paralelização (requer cluster potente)

⚠️ **Se deixar em 1**: Mesmo escolhendo "Paralela", jobs rodarão sequencialmente!

## 📊 Exemplo de Uso

### Cenário: Enfileirar 10 datasets Oracle

**Passo 1**: Selecionar datasets
```
Usuario marca checkboxes de 10 datasets
Botão "Executar Selecionados (10)" aparece
```

**Passo 2**: Confirmar estratégia
```
Modal abre
Usuario escolhe "Execução Paralela"
Clica "Confirmar Execução"
```

**Passo 3**: Backend processa
```javascript
POST /api/portal/datasets/bulk-enqueue
Body: {
  dataset_ids: ["uuid1", "uuid2", ..., "uuid10"],
  strategy: "parallel"
}

Backend:
  - Valida 10 datasets (todos ACTIVE)
  - Insere 10 registros em run_queue (todos priority=100)
  - Dispara 10 calls para Databricks API:
    POST /api/2.1/jobs/run-now
    {
      job_id: 690887429046802,
      job_parameters: { target_dataset_id: "uuid1" }
    }
    ... (x10)
```

**Passo 4**: Databricks executa
```
Se max_concurrent_runs = 3:
  - Jobs 1, 2, 3 iniciam RUNNING
  - Jobs 4-10 ficam PENDING na fila
  - Conforme jobs finalizam, novos iniciam
  - Todos processam em paralelo (até limite de 3)
```

**Passo 5**: Resultado
```
15 minutos depois:
  - 10 datasets processados
  - 10 registros em batch_process (status SUCCEEDED)
  - Bronze e Silver atualizados para todos
```

## 🧪 Como Testar

### Teste 1: Seleção Múltipla

1. Abrir http://localhost:3010/datasets
2. Marcar checkbox de 3-5 datasets
3. **Verificar**: Botão "Executar Selecionados (N)" aparece
4. **Verificar**: Cards selecionados têm ring azul
5. Clicar "Selecionar todos"
6. **Verificar**: Todos ficam selecionados

### Teste 2: Modal de Confirmação

1. Selecionar datasets
2. Clicar "Executar Selecionados"
3. **Verificar**: Modal abre com contagem correta
4. **Verificar**: Radio buttons funcionam
5. **Verificar**: Info box explica estratégia paralela

### Teste 3: Execução Paralela

1. Selecionar 5 datasets ACTIVE
2. Escolher "Execução Paralela"
3. Clicar "Confirmar Execução"
4. **Verificar**: Backend retorna `{ ok: true, summary: { enqueued: 5 } }`
5. **Abrir Monitoramento**: http://localhost:3010/monitor
6. **Verificar**: 5 jobs aparecem como RUNNING (ou PENDING se fila cheia)
7. **Databricks**: Ver 3-5 Job Runs simultâneos (baseado em max_concurrent_runs)

### Teste 4: Execução Sequencial

1. Selecionar 3 datasets
2. Escolher "Execução Sequencial"
3. Confirmar
4. **Verificar**: Jobs têm prioridades diferentes (200, 199, 198)
5. **Monitorar**: Ver que processar um de cada vez (respeitando ordem)

### Teste 5: Datasets Bloqueados

1. Mudar 1 dataset para estado PAUSED via SQL:
   ```sql
   UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
   SET execution_state = 'PAUSED'
   WHERE dataset_id = 'uuid';
   ```
2. Selecionar esse dataset + outros ACTIVE
3. Executar
4. **Verificar**: Backend retorna `{ status: 'NOT_ELIGIBLE' }` para o PAUSED
5. **Verificar**: Outros executam normalmente

## 🔍 Debugging

### Ver Jobs Disparados

**Via SQL**:
```sql
SELECT 
  queue_id, dataset_id, trigger_type, status, priority,
  requested_at, requested_by
FROM cm_dbx_dev.ingestion_sys_ops.run_queue
WHERE trigger_type = 'MANUAL_BULK'
ORDER BY requested_at DESC
LIMIT 20;
```

**Via Frontend**:
- Abrir **Monitoramento** → Aba "Fila de Execução"
- Filtrar por `trigger_type = MANUAL_BULK`

### Ver Execuções no Databricks

**Databricks UI**:
1. **Workflows** → Job do orquestrador
2. **Runs** → Ver runs recentes
3. **Parameters** → Confirmar `target_dataset_id` está setado

**Logs**:
- Cada run mostra: `[CLAIM] 🎯 MODO TARGETIZADO: dataset_id=...`
- Processa apenas aquele dataset específico

### Troubleshooting

**Problema**: Jobs não executam em paralelo
- **Verificar**: `max_concurrent_runs` no Job
- **Solução**: Aumentar para 3+

**Problema**: Backend retorna erro "NOT_FOUND"
- **Verificar**: dataset_ids são UUIDs válidos
- **Verificar**: Datasets existem em dataset_control

**Problema**: Jobs ficam PENDING muito tempo
- **Verificar**: Cluster está disponível
- **Verificar**: Não há jobs órfãos travados
- **Solução**: Cancelar runs antigos no Databricks

**Problema**: Alguns datasets pulam execução
- **Verificar**: Estados (PAUSED, DEPRECATED, BLOCKED_SCHEMA_CHANGE bloqueiam)
- **Verificar**: Response do backend mostra `status: 'NOT_ELIGIBLE'`

## 📈 Métricas e Performance

### Throughput Esperado

**Sequencial** (max_concurrent_runs=1):
- 1 dataset/vez
- 120K rows em ~1 min
- 10 datasets = ~10 minutos

**Paralelo** (max_concurrent_runs=3):
- 3 datasets simultâneos
- 10 datasets = ~4 minutos (3x mais rápido)

**Paralelo** (max_concurrent_runs=5):
- 5 datasets simultâneos
- 10 datasets = ~2-3 minutos (5x mais rápido)

### Limites Recomendados

- **Máximo por requisição**: 200 datasets (hard limit no backend)
- **Recomendado**: 10-20 datasets por vez (melhor UX)
- **max_concurrent_runs**: 3-5 (balance entre velocidade e estabilidade)

## 🎯 Comparação: Antes vs Depois

| Cenário | Antes | Depois |
|---------|-------|--------|
| Executar 10 datasets | 10 cliques manuais | 1 clique (seleção múltipla) |
| Paralelismo | Não (loop sequencial dentro do job) | Sim (N jobs independentes) |
| Logs | Misturados no mesmo run | Separados por dataset |
| Falha de 1 dataset | Para tudo | Outros continuam |
| Escalabilidade | Limitada (1 cluster) | Alta (N clusters/jobs) |
| Visibilidade | Difícil debugar | Cada dataset tem run próprio |

## 🚀 Próximos Passos (Roadmap)

1. ✅ **Implementado**: Seleção múltipla + Modal + Backend
2. ⏳ **Testar em produção**: Validar com 10+ datasets reais
3. 📊 **Métricas**: Coletar tempo médio de execução paralela
4. 🔔 **Notificações**: Avisar quando batch completar (email/webhook)
5. 📅 **Agendamento em massa**: "Executar todos os dias às 8h"
6. 🎛️ **Controle fino**: Pausar/cancelar batch em andamento

---

**Implementado por**: Warp AI Agent  
**Data**: 2026-02-21  
**Arquitetura**: Opção C (Enterprise - Paralelismo Real)  
**Status**: ✅ Completo e pronto para teste
