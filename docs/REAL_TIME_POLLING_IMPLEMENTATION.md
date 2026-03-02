# Real-Time Polling Implementation 🚀

## Problema Resolvido

O dashboard de monitoramento não atualizava automaticamente quando jobs mudavam de estado no Databricks (RUNNING → SUCCESS/FAILED). O usuário precisava recarregar manualmente ou expandir cards para ver atualizações.

## Solução Implementada: Polling Inteligente (Opção 1)

Escolhemos a **Opção 1** como MVP por ser:
- ✅ Rápida de implementar
- ✅ Resolve 90% do problema
- ✅ Não requer mudanças arquiteturais grandes
- ✅ Base sólida para evoluir para WebSocket depois

### Arquitetura Atual

```
Databricks Job (executa)
         ↓
   Atualiza tabelas
   (batch_process, run_queue)
         ↓
   Backend API (Node.js)
   (leitura sob demanda)
         ↓
   Frontend React
   (polling inteligente)
```

## Funcionalidades Implementadas

### 1. **Polling Dinâmico Adaptativo**

O intervalo de polling muda automaticamente baseado no estado das execuções:

```typescript
// Detecta se há jobs RUNNING
const hasRunning = [recent, queue].some(item => 
  ["RUNNING", "CLAIMED"].includes(item.status)
);

// Ajusta intervalo
if (hasRunning) {
  setPollingInterval(5000);  // 5s quando RUNNING
  setIsAutoRefreshing(true);
} else {
  setPollingInterval(30000); // 30s quando idle
  setIsAutoRefreshing(false);
}
```

**Comportamento**:
- **Idle (sem jobs RUNNING)**: Atualiza a cada 30 segundos
- **Ativo (jobs RUNNING)**: Atualiza a cada 5 segundos
- **Transição automática** entre os modos

### 2. **Indicador Visual de Auto-Refresh**

```tsx
{isAutoRefreshing && (
  <span className="ml-2 inline-flex items-center gap-1 text-blue-600">
    <span className="h-2 w-2 bg-blue-600 rounded-full animate-pulse"></span>
    Auto-refresh ativo (5s)
  </span>
)}
```

**Mostra ao usuário**:
- 🔵 Ponto azul pulsando quando polling está acelerado
- Indica que atualizações estão acontecendo automaticamente
- Aparece apenas quando há jobs RUNNING

### 3. **Contador de Duração em Tempo Real**

```typescript
// Clock tick a cada 1 segundo
useEffect(() => {
  const timer = setInterval(() => setNow(Date.now()), 1000);
  return () => clearInterval(timer);
}, []);

// Calcula duração dinâmica
const getLiveDuration = (runId: string, staticDuration: any): string => {
  if (!runId || !liveTimestamps[runId]) return formatDuration(staticDuration);
  const startTime = liveTimestamps[runId];
  const elapsed = Math.floor((now - startTime) / 1000);
  return formatDuration(elapsed);
};
```

**Resultado**:
- Jobs RUNNING mostram duração crescendo em tempo real
- Formato: `1m 23s` → `1m 24s` → `1m 25s`...
- Visual pulsante azul para chamar atenção
- Não requer refresh para atualizar

### 4. **Mapeamento de Timestamps para Execuções Ativas**

```typescript
// Constrói mapa de timestamps na hora do fetch
const timestamps: Record<string, number> = {};
[...recent, ...queue].forEach(item => {
  if (["RUNNING", "CLAIMED"].includes(item.status)) {
    const id = item.run_id || item.queue_id;
    const startTime = item.started_at || item.claimed_at;
    if (id && startTime) {
      timestamps[id] = new Date(startTime).getTime();
    }
  }
});
setLiveTimestamps(timestamps);
```

**Benefício**:
- Clock local não depende de server
- Precisão de 1 segundo
- Funciona mesmo se backend ficar lento

## Arquivos Modificados

### `frontend/src/pages/Monitor.tsx`

**Linhas adicionadas**:
- **47-51**: Estados para polling inteligente e timestamps
- **66-79**: Construção do mapa de timestamps e detecção de RUNNING
- **81-92**: Ajuste dinâmico do intervalo de polling
- **107-117**: Separação dos useEffect para polling dinâmico
- **123-137**: Lógica de contador em tempo real
- **129-134**: Indicador visual de auto-refresh
- **260-265**: Aplicação do contador na UI

**Total**: ~60 linhas adicionadas

## Comparação: Antes vs Depois

### Antes ❌
```
User abre dashboard → Vê snapshot de 30s atrás
Job muda RUNNING → SUCCESS
Dashboard continua mostrando RUNNING
User precisa clicar em "Atualizar" manualmente
Duração estática não cresce
```

### Depois ✅
```
User abre dashboard → Vê dados atuais
Job está RUNNING → Polling muda para 5s automaticamente
Dashboard mostra "Auto-refresh ativo" 🔵
Duração cresce em tempo real: 1m 23s → 1m 24s → 1m 25s
Job termina → Dashboard atualiza em até 5s
Polling volta para 30s (idle mode)
```

## Performance e Otimizações

### Network Requests
- **Idle**: 2 requests/min (30s interval)
- **Active**: 12 requests/min (5s interval)
- **Economia**: Polling só acelera quando necessário

### Browser Performance
- **Clock tick**: `setInterval` leve (1000ms)
- **Cálculo de duração**: O(1) lookup em mapa
- **Re-renders**: Apenas componentes afetados

### Databricks Load
- Sem impacto adicional
- Queries já existentes, apenas frequência muda
- Backend cache pode ser adicionado depois

## Evolução Futura: WebSocket (Opção 2)

### Preparação Atual
O código está estruturado para fácil migração:

```typescript
// HOJE (Polling)
const fetchAll = useCallback(async () => {
  const data = await api.getRecentBatchProcesses();
  setRecent(data.items);
}, []);

// AMANHÃ (WebSocket)
useEffect(() => {
  const ws = new WebSocket("wss://api/monitor");
  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    setRecent(data.items);
  };
  return () => ws.close();
}, []);
```

### Backend WebSocket (Roadmap)

**Stack sugerida**:
- **Socket.IO** (Node.js) ou **ws** library
- Endpoint: `ws://localhost:3010/ws/monitor`
- Events: `executions:update`, `queue:update`

**Implementação backend** (exemplo):
```javascript
// server.js
const { Server } = require("socket.io");
const io = new Server(server, { cors: { origin: "*" } });

io.on("connection", (socket) => {
  console.log("Client connected to monitor");
  
  // Poll Databricks and emit updates
  const interval = setInterval(async () => {
    const data = await fetchMonitorData();
    socket.emit("executions:update", data);
  }, 3000);
  
  socket.on("disconnect", () => {
    clearInterval(interval);
  });
});
```

**Vantagens WebSocket**:
- 🚀 Latência < 1s (vs 5s polling)
- 💰 Menos requests (1 conexão persistente)
- 📊 Push real em vez de pull
- 🔄 Bidirecional (frontend pode enviar comandos)

## Opção 3: Webhook + Persistência (Enterprise)

Para arquitetura enterprise, configurar Databricks para chamar webhook:

```python
# No final do notebook do orquestrador
dbutils.webhook.post(
  url="https://your-api.com/webhooks/orchestrator",
  body={
    "run_id": run_id,
    "dataset_id": dataset_id,
    "status": "SUCCEEDED",
    "bronze_rows": bronze_count,
    "silver_rows": silver_count
  }
)
```

**Backend recebe webhook**:
```javascript
app.post("/webhooks/orchestrator", (req, res) => {
  const { run_id, status } = req.body;
  
  // Atualiza banco
  db.updateRunStatus(run_id, status);
  
  // Notifica todos os clientes conectados via WebSocket
  io.emit("execution:completed", req.body);
  
  res.status(200).send("OK");
});
```

## Monitoramento e Debugging

### Console Logs (Desenvolvimento)
```typescript
console.log(`[Monitor] Polling interval: ${pollingInterval}ms`);
console.log(`[Monitor] Has running jobs: ${isAutoRefreshing}`);
console.log(`[Monitor] Live timestamps:`, liveTimestamps);
```

### Métricas Sugeridas (Produção)
- Tempo médio de detecção de mudança de status
- Latência entre finalização no Databricks e atualização no frontend
- Taxa de requests/min por usuário
- Cache hit rate (futuro)

## Configuração

Não requer configuração adicional! ✨

As mudanças são **retrocompatíveis** e funcionam automaticamente ao:
1. Reconstruir frontend: `npm run build` (na pasta `frontend/`)
2. Reiniciar servidor backend: `node server.js`
3. Acessar dashboard: http://localhost:3010

## Testes

### Cenário 1: Job RUNNING
1. Enfileirar job via portal
2. Abrir dashboard de Monitoramento
3. **Verificar**: Ponto azul "Auto-refresh ativo (5s)" aparece
4. **Verificar**: Duração cresce em tempo real
5. **Verificar**: Status atualiza automaticamente quando job termina

### Cenário 2: Sem Jobs RUNNING
1. Aguardar todos jobs finalizarem
2. **Verificar**: Ponto azul desaparece
3. **Verificar**: Polling volta para 30s
4. **Verificar**: Dashboard continua funcional

### Cenário 3: Múltiplos Jobs RUNNING
1. Enfileirar 3+ jobs simultaneamente
2. **Verificar**: Todos mostram duração crescente
3. **Verificar**: Quando todos terminam, polling desacelera

## Troubleshooting

### Dashboard não atualiza automaticamente
- **Verificar**: Console do navegador por erros de API
- **Verificar**: Backend está rodando (`http://localhost:3010/health`)
- **Verificar**: Databricks tabelas estão acessíveis

### Duração não cresce em tempo real
- **Verificar**: Item tem `run_id` ou `queue_id`
- **Verificar**: Item tem `started_at` ou `claimed_at`
- **Verificar**: Status é "RUNNING" ou "CLAIMED"

### Polling não acelera com jobs RUNNING
- **Verificar**: API `/api/executions/recent` retorna status correto
- **Verificar**: Status vem em uppercase
- **Console**: Checar `liveTimestamps` no React DevTools

## Conclusão

✅ **Implementado com sucesso!**

A solução de Polling Inteligente:
- Resolve o problema de atualização manual
- Melhora UX com feedback visual em tempo real
- Otimiza performance com polling adaptativo
- Cria base sólida para evoluir para WebSocket

**Próximos passos sugeridos**:
1. ✅ Testar em produção por 1-2 semanas
2. 📊 Coletar métricas de uso
3. 🚀 Evoluir para WebSocket quando necessário
4. 🔔 Adicionar notificações browser (opcional)

---

**Implementado por**: Warp AI Agent  
**Data**: 2026-02-21  
**Arquivos modificados**: `frontend/src/pages/Monitor.tsx`  
**Linhas adicionadas**: ~60 linhas  
**Breaking changes**: Nenhum (retrocompatível)
