# docs/02-specs/phase-03-schema-versioning-and-blocking.md
## Fase 03 — Versionamento de Schema e Bloqueio Automático

---

## 1) Objetivo da fase
Implementar **versionamento formal de schema da Silver** com **bloqueio automático** diante de qualquer mudança estrutural, garantindo:
- zero schema drift silencioso;
- proteção de consumidores downstream;
- rastreabilidade e auditoria completas;
- continuidade de ingestão Bronze.

Esta fase **materializa o ADR-0005** e integra-se diretamente às Fases 01 e 02.

---

## 2) Escopo
### Inclui
- Cálculo de **fingerprint determinístico** do schema efetivo
- Comparação com o contrato Silver ativo
- Criação automática de **nova versão de schema (PENDING)**
- Bloqueio do dataset (`BLOCKED_SCHEMA_CHANGE`)
- Interrupção da escrita Silver
- Registro para aprovação/rejeição
- Mensagens de erro acionáveis

### Não inclui
- UI de aprovação (entra na Fase 05)
- RBAC e permissões UC (Fase 04)
- Evoluções avançadas de diff semântico (MVP usa diff estrutural)

---

## 3) Artefatos gerados
- **Código**
  - `SchemaFingerprintCalculator`
  - `SchemaDiffEngine`
  - `SchemaVersionService`
  - `SchemaBlockingService`
- **Tabelas**
  - `ingestion_sys.ctrl.schema_versions`
  - `ingestion_sys.ctrl.schema_approvals`
  - `ingestion_sys.ctrl.dataset_control`
- **Estados**
  - `ACTIVE`
  - `PENDING`
  - `REJECTED`
  - `BLOCKED_SCHEMA_CHANGE`

---

## 4) Implementação (fluxos e pseudocódigo)

### 4.1 Extração do schema efetivo (Bronze)
```python
effective_schema = extract_schema(bronze_df)
```

Normalização obrigatória:
- nomes em lowercase
- tipos Spark normalizados
- ordenação alfabética das colunas

---

### 4.2 Cálculo do fingerprint
```python
fingerprint = hash(
    canonical_json(effective_schema)
)
```

Regras:
- independente da ordem física
- sensível a:
  - nome
  - tipo
  - nulabilidade
  - chaves

---

### 4.3 Comparação com schema ativo
```python
active_schema = load_active_schema(dataset_id)

if fingerprint != active_schema.fingerprint:
    handle_schema_change()
```

---

### 4.4 Criação de nova versão (PENDING)
```python
new_version = active_schema.version + 1

create_schema_version(
    dataset_id=dataset_id,
    version=new_version,
    fingerprint=fingerprint,
    status="PENDING"
)
```

---

### 4.5 Bloqueio do dataset
```python
update_dataset_state(
    dataset_id,
    state="BLOCKED_SCHEMA_CHANGE",
    reason="SCHEMA_CHANGE_DETECTED"
)
```

Comportamento esperado:
- Bronze: ✅ continua
- Silver: ❌ interrompida
- Retry: ❌ não aplicável

---

### 4.6 Aprovação / rejeição (hook)
```python
if approval.decision == "APPROVED":
    activate_schema_version()
    unblock_dataset()

elif approval.decision == "REJECTED":
    keep_blocked()
```

---

## 5) Critérios de aceite (checklist técnico)
- [ ] Fingerprint é determinístico
- [ ] Ordem de colunas não afeta fingerprint
- [ ] Nova versão criada automaticamente
- [ ] Dataset entra em `BLOCKED_SCHEMA_CHANGE`
- [ ] Silver não escreve após bloqueio
- [ ] Bronze continua executando
- [ ] Histórico de versões preservado
- [ ] Erro é explícito (`SCHEMA_CHANGE_DETECTED`)

---

## 6) Riscos e guardrails
### Riscos
- Falsos positivos por normalização incorreta
- Bloqueios frequentes por origem instável

### Guardrails
- Normalização rígida
- Aprovação explícita obrigatória
- Métricas de bloqueio monitoradas

---

## 7) Pré-requisitos para próxima fase
Para iniciar a **Fase 04**:
- Bloqueio automático funcionando
- Versionamento auditável
- Estados refletidos no `dataset_control`

Próximo documento:
👉 `docs/02-specs/phase-04-admin-console-lifecycle-rbac.md`
