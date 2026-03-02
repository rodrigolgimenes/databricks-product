# Sistema de Configuração Flexível de Nomenclatura de Tabelas

## 📋 Visão Geral

Este sistema permite controle total sobre a nomenclatura das tabelas Bronze e Silver na plataforma de governança Databricks, oferecendo três níveis de flexibilidade:

1. **Preview & Customização Individual** (Fase 1) - Durante a criação de datasets
2. **Gestão de Convenções** (Fase 2) - Interface administrativa para padrões globais
3. **Renomeação em Massa** (Fase 3) - Operações bulk com preview e auditoria

---

## 🎯 Fase 1: Preview & Customização Individual

### Funcionalidades
- Preview automático da nomenclatura antes de criar dataset
- Toggle "Customizar nomenclatura" para edição manual
- Campos separados para catálogo, schema e tabela (Bronze e Silver)
- Preview dinâmico das mudanças em tempo real

### Como Usar
1. Acesse **Criar Dataset** → Preencha dados básicos
2. Na etapa 3 (Revisão):
   - Visualize os nomes auto-gerados
   - Marque "Customizar nomenclatura" para editar
   - Ajuste catálogo, schema ou nome da tabela
   - Confirme no preview visual

### Endpoint API
```typescript
POST /api/portal/datasets/naming-preview
Body: {
  "area_id": "mega",
  "dataset_name": "CMASTER.GLO_AGENTES@CMASTERPRD"
}

Response: {
  "ok": true,
  "bronze_catalog": "cm_dbx_dev",
  "bronze_schema": "bronze_mega",
  "bronze_table": "CMASTER_GLO_AGENTES",
  "silver_catalog": "cm_dbx_dev",
  "silver_schema": "silver_mega",
  "silver_table": "CMASTER_GLO_AGENTES",
  "dataset_sanitized": "CMASTER_GLO_AGENTES"
}
```

---

## ⚙️ Fase 2: Gestão de Convenções de Nomenclatura

### Funcionalidades
- Interface administrativa em `/settings`
- Criação de múltiplas convenções versionadas
- Ativação/desativação com controle de versão
- Preview de patterns com exemplos
- Validação de placeholders `{area}` e `{dataset}`

### Estrutura da Tabela
```sql
cm_dbx_dev.ingestion_sys_ctrl.naming_conventions
├── naming_version (INT, PRIMARY KEY)
├── is_active (BOOLEAN)
├── bronze_pattern (STRING) -- Ex: "bronze_{area}.{dataset}"
├── silver_pattern (STRING) -- Ex: "silver_{area}.{dataset}"
├── created_at (TIMESTAMP)
├── created_by (STRING)
└── notes (STRING)
```

### Como Usar
1. Acesse **Configurações** no menu lateral
2. Clique em **Nova Convenção**
3. Defina patterns Bronze e Silver:
   - Use `{area}` para substituição da área
   - Use `{dataset}` para substituição do dataset
4. Adicione notas descritivas (opcional)
5. Após criar, clique em **Ativar** para aplicar globalmente

### Exemplos de Patterns
```
# Padrão atual
bronze_pattern: "bronze_{area}.{dataset}"
silver_pattern: "silver_{area}.{dataset}"

# Com prefixo de ambiente
bronze_pattern: "dev_bronze_{area}.{dataset}"
silver_pattern: "dev_silver_{area}.{dataset}"

# Com catálogo diferente
bronze_pattern: "landing.raw_{area}.{dataset}"
silver_pattern: "curated.clean_{area}.{dataset}"
```

### Endpoints API
```typescript
// Listar todas as convenções
GET /api/portal/admin/naming-conventions

// Criar nova convenção
POST /api/portal/admin/naming-conventions
Body: {
  "bronze_pattern": "bronze_{area}.{dataset}",
  "silver_pattern": "silver_{area}.{dataset}",
  "notes": "Padrão de desenvolvimento"
}

// Ativar convenção
POST /api/portal/admin/naming-conventions/:version/activate

// Editar convenção (apenas inativas)
PATCH /api/portal/admin/naming-conventions/:version
Body: {
  "bronze_pattern": "new_bronze_{area}.{dataset}",
  "notes": "Pattern atualizado"
}
```

---

## 🔄 Fase 3: Renomeação em Massa (Bulk Rename)

### Funcionalidades
- Seleção múltipla de datasets via checkbox
- 3 tipos de operações:
  - **REPLACE_SCHEMA_PREFIX**: Substitui prefixo do schema
  - **REPLACE_CATALOG**: Substitui catálogo
  - **REPLACE_FULL**: Substituição completa
- Preview obrigatório antes de executar
- Criação automática de schemas (opcional)
- Detecção de conflitos (nomes duplicados)
- Auditoria completa de mudanças

### Como Usar
1. Em **Datasets**, selecione datasets via checkbox
2. Clique no botão **Renomear** na barra de ações
3. Escolha a operação:
   - **Substituir Prefixo do Schema**: `bronze_old.table` → `bronze_new.table`
   - **Substituir Catálogo**: `catalog_old.schema.table` → `catalog_new.schema.table`
   - **Substituição Completa**: Define novo nome completo `catalog.schema.table`
4. Preencha campos "De" e "Para" conforme a operação
5. Clique em **Gerar Preview**
6. Revise mudanças:
   - ✅ Verde: Renomeação OK
   - ⚠️ Amarelo: Conflito detectado (nome já existe)
   - ❌ Vermelho: Erro de validação
7. Clique em **Renomear** para executar

### Operações Disponíveis

#### 1. REPLACE_SCHEMA_PREFIX
Substitui o prefixo do schema mantendo o resto do nome.
```
Bronze: bronze_mega.CMASTER_GLO_AGENTES → bronze_prod.CMASTER_GLO_AGENTES
Silver: silver_mega.CMASTER_GLO_AGENTES → silver_prod.CMASTER_GLO_AGENTES
```

#### 2. REPLACE_CATALOG
Substitui apenas o catálogo, mantendo schema e tabela.
```
Bronze: cm_dbx_dev.bronze_mega.TABLE → cm_dbx_prod.bronze_mega.TABLE
Silver: cm_dbx_dev.silver_mega.TABLE → cm_dbx_prod.silver_mega.TABLE
```

#### 3. REPLACE_FULL
Substituição completa do nome da tabela.
```
Bronze: cm_dbx_dev.bronze_mega.OLD → cm_dbx_prod.landing.NEW
Silver: cm_dbx_dev.silver_mega.OLD → cm_dbx_prod.curated.NEW
```

### Endpoint API
```typescript
// Preview (não executa)
POST /api/portal/datasets/bulk-rename
Body: {
  "dataset_ids": ["uuid1", "uuid2", "uuid3"],
  "operation": "REPLACE_SCHEMA_PREFIX",
  "bronze_from": "bronze_dev",
  "bronze_to": "bronze_prod",
  "silver_from": "silver_dev",
  "silver_to": "silver_prod",
  "create_schemas": true,
  "confirm": false  // Preview mode
}

Response: {
  "ok": true,
  "preview": true,
  "results": [
    {
      "dataset_id": "uuid1",
      "dataset_name": "SCHEMA.TABLE",
      "status": "PREVIEW",
      "old_bronze": "cm_dbx_dev.bronze_dev.TABLE",
      "new_bronze": "cm_dbx_dev.bronze_prod.TABLE",
      "old_silver": "cm_dbx_dev.silver_dev.TABLE",
      "new_silver": "cm_dbx_dev.silver_prod.TABLE"
    }
  ],
  "schemas_to_create": ["cm_dbx_dev.bronze_prod", "cm_dbx_dev.silver_prod"]
}

// Executar (após preview)
POST /api/portal/datasets/bulk-rename
Body: { ...same as above, "confirm": true }

Response: {
  "ok": true,
  "renamed": 3,
  "results": [...],
  "created_schemas": ["cm_dbx_dev.bronze_prod"]
}
```

---

## 📊 Auditoria de Mudanças

### Tabela de Audit Log
```sql
cm_dbx_dev.ingestion_sys_ctrl.naming_audit_log
├── audit_id (STRING, NOT NULL)
├── dataset_id (STRING)
├── operation_type (STRING, NOT NULL) -- BULK_RENAME, INDIVIDUAL_RENAME, CONVENTION_CHANGE
├── old_bronze_table (STRING)
├── new_bronze_table (STRING)
├── old_silver_table (STRING)
├── new_silver_table (STRING)
├── old_naming_version (INT)
├── new_naming_version (INT)
├── performed_by (STRING, NOT NULL)
├── performed_at (TIMESTAMP, NOT NULL)
├── change_reason (STRING)
└── metadata (STRING) -- JSON with operation details
```

### Consultas Úteis

```sql
-- Histórico de mudanças de um dataset
SELECT 
  performed_at,
  operation_type,
  old_bronze_table,
  new_bronze_table,
  performed_by
FROM cm_dbx_dev.ingestion_sys_ctrl.naming_audit_log
WHERE dataset_id = 'dataset-uuid'
ORDER BY performed_at DESC;

-- Mudanças recentes (últimas 24h)
SELECT 
  dataset_id,
  operation_type,
  old_bronze_table,
  new_bronze_table,
  performed_by,
  performed_at
FROM cm_dbx_dev.ingestion_sys_ctrl.naming_audit_log
WHERE performed_at >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY performed_at DESC;

-- Mudanças por usuário
SELECT 
  performed_by,
  COUNT(*) as total_changes,
  COUNT(DISTINCT dataset_id) as datasets_affected
FROM cm_dbx_dev.ingestion_sys_ctrl.naming_audit_log
GROUP BY performed_by
ORDER BY total_changes DESC;
```

---

## 🚀 Status de Implementação

### ✅ Fase 1 - Preview & Customização Individual (100%)
- [x] Backend: Endpoint `/datasets/naming-preview`
- [x] Backend: Suporte a `custom_bronze_table` e `custom_silver_table` no POST `/datasets`
- [x] Frontend: Preview automático na etapa 3
- [x] Frontend: Toggle "Customizar nomenclatura"
- [x] Frontend: Formulário de edição com preview dinâmico
- [x] Validação de formato `catalog.schema.table`

### ✅ Fase 2 - Gestão de Convenções (100%)
- [x] Backend: Endpoints admin CRUD completos
- [x] Backend: Sistema de versionamento
- [x] Backend: Ativação/desativação de convenções
- [x] Frontend: Página `/settings`
- [x] Frontend: Lista com badges de status
- [x] Frontend: Dialog criar/editar com preview
- [x] Frontend: Validação de placeholders
- [x] Menu: Item "Configurações" no sidebar

### ✅ Fase 3 - Renomeação em Massa (100%)
- [x] Backend: Endpoint `/datasets/bulk-rename` com 3 operações
- [x] Backend: Preview mode com detecção de conflitos
- [x] Backend: Criação automática de schemas
- [x] Backend: Integração com audit log
- [x] Frontend: Botão "Renomear" na barra de ações
- [x] Frontend: Modal completo com seleção de operação
- [x] Frontend: Preview visual com status por dataset
- [x] Frontend: Feedback de execução
- [x] Database: Tabela `naming_audit_log` criada
- [x] Database: Migration SQL executada via MCP

---

## 🔧 Configuração e Deploy

### 1. Verificar Estrutura no Databricks
```sql
-- Verificar tabelas de controle
SHOW TABLES IN cm_dbx_dev.ingestion_sys_ctrl;

-- Deve listar:
-- - naming_conventions
-- - naming_audit_log
-- - dataset_control
-- - (outras tabelas do sistema)
```

### 2. Reiniciar Backend
```bash
cd C:\dev\cm-databricks
node server.js
```

### 3. Acessar Frontend
```
http://localhost:3000/#/settings  # Gestão de convenções
http://localhost:3000/#/datasets  # Renomeação em massa
http://localhost:3000/#/create    # Preview customizado
```

---

## 📝 Notas Técnicas

### Validações
- Nomes de tabela devem seguir formato `catalog.schema.table` ou `schema.table`
- Apenas caracteres alfanuméricos e underscores são permitidos
- Patterns devem conter placeholders `{area}` e `{dataset}`
- Convenções ativas não podem ser editadas (desative primeiro)

### Segurança
- Detecção automática de conflitos (nomes duplicados)
- Validação de nomes seguros (SQL injection prevention)
- Audit log completo de todas mudanças
- Identificação de usuário via header `x-user` ou `x-portal-user`

### Performance
- Preview de renomeação não executa queries no banco
- Criação de schemas é opcional e assíncrona
- Audit log não bloqueia operação principal (try-catch)

---

## 🐛 Troubleshooting

### Erro: "NO_NAMING - Sem naming_conventions ativa"
**Solução**: Acesse `/settings` e ative uma convenção de nomenclatura.

### Erro: "Nome de tabela inválido"
**Solução**: Verifique se o formato está correto: `catalog.schema.table`

### Conflito ao renomear
**Solução**: O nome novo já existe em outro dataset. Escolha outro nome ou delete o dataset conflitante.

### Audit log não registra
**Solução**: Verifique se a tabela `naming_audit_log` existe e tem permissões de INSERT.

---

## 📚 Referências

- **Backend**: `src/portalRoutes.js` (linhas 1282-2930)
- **Frontend Settings**: `frontend/src/pages/Settings.tsx`
- **Frontend Datasets**: `frontend/src/pages/Datasets.tsx` (linhas 152-1176)
- **API Types**: `frontend/src/lib/api.ts` (linhas 62-170)
- **Migration**: `migrations/005_create_naming_audit_log.sql`

---

## 🎉 Conclusão

O sistema está **100% implementado e operacional**. Todos os endpoints, interfaces e migrations foram criados, testados e aplicados no Databricks via MCP. 

Para dúvidas ou suporte, consulte a documentação do código ou logs do sistema.
