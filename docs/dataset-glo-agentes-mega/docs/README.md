# Dataset Real — glo_agentes (MEGA ERP / Oracle)

Este projeto contém um **dataset governado real** baseado na tabela **`CMASTER.GLO_AGENTES`**,
usado como **primeira tabela de teste** no Orchestrator.

## Objetivo
- Validar conectividade Oracle → Bronze → Silver
- Validar governança do contrato (ExpectSchemaJSON)
- Validar merge determinístico (LWW) usando `AGN_DT_ULTIMAATUCAD`

## Fonte Oracle
- Owner: `CMASTER`
- Tabela: `GLO_AGENTES`
- DBLINK (quando aplicável): `CMASTERPRD`

> ⚠️ **Não versionar credenciais.** Use variáveis de ambiente / secret scope.

## Contrato Silver
- Arquivo: `contracts/glo_agentes.expectschema.json`
- PK: `AGN_TAB_IN_CODIGO`, `AGN_PAD_IN_CODIGO`, `AGN_IN_CODIGO`
- Watermark / Order: `AGN_DT_ULTIMAATUCAD`

## Como registrar no sistema (DEV)
1. Ajuste `sql/register_dataset.sql` com seus `project_id`, `area_id` e `connection_id`
2. Execute o SQL no Databricks SQL (DEV)
3. Registre o schema v1 em `schema_versions` (via Admin Service ou seed)
4. Enfileire uma execução via `run_queue`

## Gerado em
2025-12-12T21:20:48.483632Z
