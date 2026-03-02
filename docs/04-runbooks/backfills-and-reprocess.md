# docs/04-runbooks/backfills-and-reprocess.md
## Runbook — Backfills e Reprocessamentos Governados

---

## 1) Objetivo
Este runbook descreve **como executar backfills e reprocessamentos** de forma **segura, auditável e governada**, garantindo:
- determinismo dos resultados;
- respeito aos contratos da Silver;
- rastreabilidade completa;
- ausência de execuções “fora do sistema”.

---

## 2) Princípios
- Todo reprocessamento **passa pela run_queue**
- Nunca executar notebooks manualmente
- Silver sempre respeita **contrato ativo**
- Backfill **não ignora** bloqueios de schema
- Resultados devem ser **reproduzíveis**

---

## 3) Tipos de Reprocessamento

### 3.1 Reprocessamento simples (última janela)
Uso:
- erro temporário
- falha de infraestrutura
- correção de código sem mudança de schema

Ação:
- enqueue MANUAL

```sql
INSERT INTO ingestion_sys.ops.run_queue
(queue_id, dataset_id, trigger_type, requested_by, requested_at, status)
VALUES
(uuid(), '<dataset_id>', 'MANUAL', 'ops', current_timestamp(), 'PENDING');
```

---

### 3.2 Backfill histórico
Uso:
- carga retroativa
- correção de dados históricos
- mudança de lógica de negócio **sem mudança estrutural**

Ação:
- enqueue BACKFILL com prioridade

```sql
INSERT INTO ingestion_sys.ops.run_queue
(queue_id, dataset_id, trigger_type, requested_by, requested_at, status, priority)
VALUES
(uuid(), '<dataset_id>', 'BACKFILL', 'ops', current_timestamp(), 'PENDING', 10);
```

---

## 4) Watermark e Backfill

### 4.1 Regra de ouro
> **Watermark só pode ser alterado via processo administrativo.**

Nunca:
- atualizar `dataset_watermark` manualmente
- sobrescrever valor sem auditoria

---

### 4.2 Reset controlado de watermark
Fluxo recomendado:
1. Pausar dataset
2. Ajustar watermark via Admin Service
3. Registrar auditoria
4. Retomar dataset
5. Enfileirar BACKFILL

---

## 5) Impacto na Silver
- MERGE + LWW garante:
  - idempotência prática
  - atualização correta do estado final
- Backfills não geram duplicidade
- Métricas refletem volume processado

---

## 6) Bloqueios e Exceções

### 6.1 Dataset bloqueado por schema
- Backfill **não executa Silver**
- Bronze continua
- Aprovação é obrigatória antes do reprocessamento efetivo

---

### 6.2 Dataset PAUSED ou DEPRECATED
- PAUSED: não executa
- DEPRECATED: não deve ser reprocessado

---

## 7) Checklist de Segurança
Antes de executar:
- [ ] contrato Silver ativo
- [ ] dataset em ACTIVE
- [ ] watermark validado
- [ ] impacto avaliado
- [ ] stakeholders informados

---

## 8) Monitoramento do Backfill
```sql
SELECT *
FROM ingestion_sys.ops.run_queue
WHERE trigger_type = 'BACKFILL'
ORDER BY requested_at DESC;
```

```sql
SELECT dataset_id, SUM(row_count)
FROM ingestion_sys.ops.batch_process_table_details
WHERE started_at >= current_date()
GROUP BY dataset_id;
```

---

## 9) Anti-patterns (proibido)
- ❌ apagar Silver e reprocessar “do zero”
- ❌ executar backfill direto no notebook
- ❌ ignorar bloqueio de schema
- ❌ alterar watermark sem registro

---

## 10) Referências
- operations.md
- debugging.md
- ADR-0002 — Run Queue
- ADR-0004 — LWW + MERGE
