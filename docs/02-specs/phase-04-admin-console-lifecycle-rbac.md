# docs/02-specs/phase-04-admin-console-lifecycle-rbac.md
## Fase 04 — Admin Console, Lifecycle do Dataset e RBAC (Unity Catalog)

---

## 1) Objetivo da fase
Implementar a **camada administrativa do produto** (sem UI no MVP) responsável por:
- setup governado (projetos, áreas, conexões);
- lifecycle explícito de datasets;
- versionamento de naming conventions;
- **RBAC centralizado via Unity Catalog** (por Projeto/Área);
- aplicação idempotente de permissões.

Esta fase consolida **governança operacional** e prepara o terreno para o User Portal (Fase 05).

---

## 2) Escopo
### Inclui
- Serviços/rotinas administrativas (scripts/Jobs/APIs internas)
- Regras de lifecycle do dataset (transições válidas)
- Versionamento e ativação de naming conventions
- Aplicação de GRANTs no Unity Catalog por Projeto/Área
- Auditoria de mudanças administrativas

### Não inclui
- UI gráfica (Admin Console visual)
- Aprovação de schema via UI (hook já existe)
- Políticas de acesso por dataset (proibido por design)

---

## 3) Artefatos gerados
- **Serviços / Jobs**
  - `ProjectService`
  - `AreaService`
  - `ConnectionApprovalService`
  - `NamingConventionService`
  - `DatasetLifecycleService`
  - `UCRbacApplier`
- **Tabelas utilizadas**
  - `ingestion_sys.ctrl.projects`
  - `ingestion_sys.ctrl.areas`
  - `ingestion_sys.ctrl.connections_*`
  - `ingestion_sys.ctrl.naming_conventions`
  - `ingestion_sys.ctrl.dataset_control`
  - `ingestion_sys.ctrl.dataset_state_changes`
  - `ingestion_sys.ctrl.rbac_defaults`
- **Plataforma**
  - Unity Catalog (schemas, tables, grants)

---

## 4) Lifecycle do Dataset (regras formais)

### 4.1 Estados permitidos
- `DRAFT`
- `ACTIVE`
- `PAUSED`
- `DEPRECATED`
- `BLOCKED_SCHEMA_CHANGE`

---

### 4.2 Transições válidas
| De | Para | Condição |
|---|---|---|
| DRAFT | ACTIVE | Config válida + naming resolvido |
| ACTIVE | PAUSED | Ação administrativa |
| PAUSED | ACTIVE | Ação administrativa |
| ACTIVE | DEPRECATED | Ação administrativa |
| BLOCKED_SCHEMA_CHANGE | ACTIVE | Schema aprovado |
| * | BLOCKED_SCHEMA_CHANGE | Detecção automática |

Transições inválidas **devem falhar**.

---

### 4.3 Auditoria
Toda mudança de estado:
- gera registro em `dataset_state_changes`;
- registra `old_state`, `new_state`, `reason`, `actor`.

---

## 5) Admin Console (sem UI) — Operações

### 5.1 Projetos e Áreas
```python
create_project(project_id, name)
create_area(area_id, project_id, name)
```

Regras:
- Projeto precisa existir para criar área
- Desativar projeto **não apaga dados**

---

### 5.2 Conexões (Oracle / SharePoint)
```python
approve_connection(connection_id)
revoke_connection(connection_id)
```

Regras:
- Apenas conexões **APPROVED** podem ser usadas
- Revogação não apaga histórico

---

### 5.3 Naming Conventions (versionadas)
```python
create_naming_convention(version, patterns)
activate_naming_convention(version)
```

Regras:
- Apenas **1 versão ativa**
- Nova ativação **não renomeia retroativamente**
- Naming é resolvido no momento do publish

---

## 6) RBAC — Unity Catalog (regra de ouro)

### 6.1 Princípios
- RBAC **por Projeto/Área**
- Nunca por dataset
- Silver é mais restrita que Bronze
- Admin separado de Data Engineer

---

### 6.2 Aplicação de permissões (exemplo)
```sql
GRANT SELECT ON SCHEMA bronze_sales TO ROLE de_read_sales;
GRANT MODIFY ON SCHEMA silver_sales TO ROLE de_write_sales;
```

Fonte da verdade:
- `ingestion_sys.ctrl.rbac_defaults`

---

### 6.3 Processo idempotente
```python
apply_uc_grants(project_id, area_id)
```

Regras:
- Pode rodar N vezes
- Sempre converge para o estado desejado
- Nunca remove permissões fora do escopo do projeto

---

## 7) Integração com Orchestrator
- Orchestrator **respeita execution_state**
- Datasets `PAUSED` ou `DEPRECATED` não executam
- `BLOCKED_SCHEMA_CHANGE` interrompe Silver
- RBAC não é decidido no runtime

---

## 8) Critérios de aceite (checklist técnico)
- [ ] Transições inválidas falham
- [ ] Auditoria registrada em todas as mudanças
- [ ] Apenas conexões aprovadas são usadas
- [ ] Naming ativo único
- [ ] GRANTs aplicados por Projeto/Área
- [ ] Scripts idempotentes
- [ ] Orchestrator respeita estados

---

## 9) Riscos e guardrails
### Riscos
- Erros manuais em grants
- Naming inconsistente

### Guardrails
- RBAC centralizado
- Naming versionado
- Execução idempotente

---

## 10) Pré-requisitos para próxima fase
Para iniciar a **Fase 05**:
- Lifecycle estável
- RBAC aplicado e validado
- Admin operations funcionais

Próximo documento:
👉 `docs/02-specs/phase-05-user-portal-errors.md`
