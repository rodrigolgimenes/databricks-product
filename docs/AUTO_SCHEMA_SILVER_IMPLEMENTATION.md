# Auto-Criação de Schema Silver - Implementação

## Problema Resolvido

Anteriormente, o orquestrador exigia que o schema ACTIVE existisse **antes** de promover dados para Silver, o que requeria criação manual do schema via frontend/SQL. Isso violava o princípio de automação do sistema governado de ingestão.

## Solução Implementada

O orquestrador agora **detecta automaticamente** quando não há schema ACTIVE e cria um schema baseado na estrutura da tabela Bronze na primeira execução.

### Modificações no Código

#### 1. Nova Função: `_auto_create_schema_from_bronze` (linhas 409-494)

```python
def _auto_create_schema_from_bronze(dataset_id: str, bronze_table: str) -> Dict[str, Any]:
    """
    Auto-generates and persists a schema from Bronze table structure.
    Used for DRAFT datasets on first execution.
    
    Returns the created schema dict.
    """
```

**Funcionalidades:**
- Lê o schema da tabela Bronze usando `spark.table(bronze_table).schema`
- Converte schema Spark para formato de contrato JSON do sistema
- Preserva precisão/escala de campos DECIMAL
- Define valores padrão seguros:
  - `primary_key: []` (sem PK por padrão)
  - `order_column: null`
  - `watermark: null`
- Insere registro em `schema_versions` com `version=1`, `status='ACTIVE'`
- Atualiza `dataset_control.current_schema_ver = 1`
- Retorna o schema criado para uso imediato

#### 2. Modificação na Lógica Silver (linhas 1199-1217)

**Antes:**
```python
schema = _get_active_schema(dataset_id)
if not schema:
    raise SchemaError("NO_ACTIVE_SCHEMA")
```

**Depois:**
```python
schema = _get_active_schema(dataset_id)

# Auto-create schema from Bronze if missing (DRAFT datasets on first execution)
if not schema:
    print(f"[RUN:SILVER] ⚠️ Nenhum schema ACTIVE encontrado")
    print(f"[RUN:SILVER] ✨ PRIMEIRA EXECUÇÃO: Auto-gerando schema da Bronze...")
    
    try:
        schema = _auto_create_schema_from_bronze(
            dataset_id=dataset_id,
            bronze_table=bronze_table
        )
        print(f"[RUN:SILVER] ✓ Schema ACTIVE criado automaticamente!")
    except Exception as e:
        print(f"[RUN:SILVER] ✗ ERRO ao auto-gerar schema: {e}")
        raise SchemaError(f"AUTO_SCHEMA_FAILED: {e}")
else:
    print(f"[RUN:SILVER] ✓ Schema ACTIVE encontrado")
```

### Fluxo de Execução

#### Primeira Execução (DRAFT → ACTIVE)
1. **Bronze Load**: Carrega dados do Oracle → Bronze (com otimizações de performance)
2. **Schema Detection**: Verifica se existe schema ACTIVE
3. **Auto-Schema Creation** (NOVO):
   - Detecta ausência de schema
   - Infere estrutura da tabela Bronze
   - Cria schema_versions (version=1, ACTIVE)
   - Atualiza dataset_control.current_schema_ver=1
4. **Silver Promotion**: Usa schema criado para cast, dedupe e merge
5. **Watermark Update**: Atualiza watermark (se configurado)
6. **Status Change**: Dataset permanece ou muda para ACTIVE

#### Execuções Subsequentes
1. **Bronze Load**: Carrega dados atualizados
2. **Schema Detection**: Encontra schema ACTIVE existente
3. **Silver Promotion**: Aplica schema conhecido
4. **Watermark Update**: Atualiza incrementalmente

### Vantagens

✅ **Zero Intervenção Manual**: Schema criado automaticamente na primeira execução
✅ **Preserva Tipos de Dados**: Decimal precision/scale, nullable, etc.
✅ **Consistente com Bronze**: Schema sempre reflete estrutura real da Bronze
✅ **Evolutivo**: Usuário pode posteriormente adicionar PK, watermark, order_column via frontend
✅ **Seguro**: Usa valores padrão conservadores (sem PK = append-only)

### Limitações Conhecidas

⚠️ **Schemas Auto-Gerados São Básicos**:
- Sem primary key (dados são APPEND-only até configuração manual)
- Sem order column (deduplicação LWW não funciona sem PK)
- Sem watermark (ingestão incremental requer configuração manual)

👉 **Recomendação**: Após primeira execução bem-sucedida, usuário deve acessar frontend e configurar:
- Primary Key (para UPSERT/MERGE em vez de APPEND)
- Order Column (para deduplicação Last-Write-Wins)
- Watermark (para ingestão incremental)

### Casos de Uso

#### 1. Novo Dataset Oracle (DBLink ou Local)
```
Usuário cria dataset → Enfileira execução → Orquestrador executa:
  → Bronze: 120K rows em 32s (com fetchsize=10000)
  → Silver: Auto-cria schema + promote (primeira vez)
  → SUCESSO sem intervenção manual
```

#### 2. Dataset com Schema Previamente Configurado
```
Orquestrador detecta schema ACTIVE existente → Usa configuração salva
  → Bronze: Load com otimizações
  → Silver: Aplica cast/dedupe/merge conforme contrato
  → Atualiza watermark incrementalmente
```

#### 3. Mudança de Schema na Origem (Fonte Oracle)
```
Se colunas mudam na origem, Bronze reflete mudança automaticamente.
Silver falha ao aplicar contrato antigo → Erro BLOCKED_SCHEMA_CHANGE
  → Admin deve revisar schema_versions e criar novo schema (version=2)
  → Orquestrador usa novo schema após aprovação
```

### Monitoramento e Logs

Logs relevantes durante auto-criação:

```
[RUN:SILVER] Buscando schema ACTIVE para dataset_id=...
[RUN:SILVER] ⚠️ Nenhum schema ACTIVE encontrado
[RUN:SILVER] ✨ PRIMEIRA EXECUÇÃO: Auto-gerando schema da Bronze...
[AUTO_SCHEMA] Auto-gerando schema a partir da tabela Bronze...
[AUTO_SCHEMA] Bronze table: cm_dbx_dev.bronze_mega.cmaluinterno
[AUTO_SCHEMA] ✓ Schema inferido: 9 colunas
[AUTO_SCHEMA]   - DTA_MEDICAO: timestamp
[AUTO_SCHEMA]   - ID_PROJETO: decimal
[AUTO_SCHEMA]   - ID_ITEM: decimal
[AUTO_SCHEMA]   - ...
[AUTO_SCHEMA] Persistindo schema na tabela schema_versions...
[AUTO_SCHEMA] Atualizando dataset_control.current_schema_ver=1...
[AUTO_SCHEMA] ✓ Schema criado com sucesso! (version=1, status=ACTIVE)
[RUN:SILVER] ✓ Schema ACTIVE criado automaticamente!
[RUN:SILVER] Aplicando transformações (cast + dedupe + merge)...
```

### Teste da Implementação

#### Preparação do Teste
```sql
-- 1. Limpar schema existente do dataset de teste
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.schema_versions 
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';

-- 2. Resetar current_schema_ver
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET current_schema_ver = NULL
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';

-- 3. Enfileirar job para teste
INSERT INTO cm_dbx_dev.ingestion_sys_ops.run_queue 
(queue_id, dataset_id, trigger_type, status, priority, requested_at, requested_by)
VALUES 
('test-auto-schema-002', '92fb0589-07b1-48b5-98a2-c3deadad19c1', 
 'MANUAL', 'PENDING', 1, CURRENT_TIMESTAMP(), 'test_auto_schema');
```

#### Executar Orquestrador (Modo Targetizado)
```python
# No Databricks Notebook: governed_ingestion_orchestrator
# Configurar widget:
dbutils.widgets.text("target_dataset_id", "92fb0589-07b1-48b5-98a2-c3deadad19c1")

# Executar notebook
# OU via workflow/job do Databricks com parâmetro target_dataset_id
```

#### Validação Pós-Execução
```sql
-- 1. Verificar schema criado
SELECT schema_version, status, created_by, change_description, expect_schema_json
FROM cm_dbx_dev.ingestion_sys_ctrl.schema_versions
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
-- Esperado: version=1, status=ACTIVE, created_by='orchestrator'

-- 2. Verificar dataset_control atualizado
SELECT current_schema_ver 
FROM cm_dbx_dev.ingestion_sys_ctrl.dataset_control
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
-- Esperado: current_schema_ver=1

-- 3. Verificar execução bem-sucedida
SELECT run_id, status, bronze_row_count, silver_row_count, finished_at
FROM cm_dbx_dev.ingestion_sys_ops.batch_process
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1'
ORDER BY started_at DESC LIMIT 1;
-- Esperado: status='SUCCEEDED', bronze_row_count=120200, silver_row_count=120200

-- 4. Verificar tabela Silver criada e populada
SELECT COUNT(*) FROM cm_dbx_dev.silver_mega.cmaluinterno;
-- Esperado: 120200 rows
```

### Rollback (Se Necessário)

Se precisar reverter a auto-criação e testar novamente:

```sql
-- Limpar schema auto-gerado
DELETE FROM cm_dbx_dev.ingestion_sys_ctrl.schema_versions 
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1' 
  AND created_by = 'orchestrator';

-- Limpar dados Silver (opcional)
TRUNCATE TABLE cm_dbx_dev.silver_mega.cmaluinterno;

-- Resetar versão no dataset
UPDATE cm_dbx_dev.ingestion_sys_ctrl.dataset_control
SET current_schema_ver = NULL
WHERE dataset_id = '92fb0589-07b1-48b5-98a2-c3deadad19c1';
```

---

## Referências

- **Arquivo modificado**: `databricks_notebooks/governed_ingestion_orchestrator.py`
- **Linhas adicionadas**: 409-494 (função nova)
- **Linhas modificadas**: 1199-1217 (detecção + auto-criação)
- **Tabelas afetadas**: 
  - `cm_dbx_dev.ingestion_sys_ctrl.schema_versions` (INSERT)
  - `cm_dbx_dev.ingestion_sys_ctrl.dataset_control` (UPDATE current_schema_ver)

## Próximos Passos

1. ✅ Código implementado
2. ⏳ **Testar em Databricks** (executar orquestrador com dataset sem schema)
3. ⏳ Validar logs e tabelas resultantes
4. ⏳ Documentar no frontend que schema é auto-gerado mas pode ser editado
5. ⏳ Implementar UI para edição de PK/watermark/order_column pós-criação
