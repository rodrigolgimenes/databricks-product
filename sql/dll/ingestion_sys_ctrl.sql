-- sql/ddl/ingestion_sys.ctrl.sql
-- =========================================================
-- Plataforma de Ingestão Governada
-- Schema: ingestion_sys.ctrl
-- Objetivo:
--   Tabelas de CONTROLE e SETUP do produto:
--   - Admin Console (projetos, áreas, conexões, naming, RBAC)
--   - Dataset lifecycle e estados
--   - Versionamento e aprovação de schema
--
-- Requisitos:
--   - Databricks + Unity Catalog
--   - Delta Lake
--   - Scripts idempotentes
-- =========================================================

CREATE SCHEMA IF NOT EXISTS ingestion_sys.ctrl;

-- =========================================================
-- 1) PROJETOS
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.projects (
    project_id          STRING      NOT NULL,
    project_name        STRING      NOT NULL,
    description         STRING,
    is_active           BOOLEAN     NOT NULL DEFAULT true,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,
    updated_at          TIMESTAMP,
    updated_by          STRING,

    CONSTRAINT pk_projects PRIMARY KEY (project_id)
)
USING DELTA;

-- =========================================================
-- 2) ÁREAS (vinculadas a projeto)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.areas (
    area_id             STRING      NOT NULL,
    project_id          STRING      NOT NULL,
    area_name           STRING      NOT NULL,
    description         STRING,
    is_active           BOOLEAN     NOT NULL DEFAULT true,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,
    updated_at          TIMESTAMP,
    updated_by          STRING,

    CONSTRAINT pk_areas PRIMARY KEY (area_id)
)
USING DELTA;

-- =========================================================
-- 3) CONEXÕES ORACLE (aprovadas pelo Admin Console)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.connections_oracle (
    connection_id       STRING      NOT NULL,
    project_id          STRING      NOT NULL,
    area_id             STRING      NOT NULL,

    jdbc_url            STRING      NOT NULL,
    secret_scope        STRING      NOT NULL,
    secret_user_key     STRING      NOT NULL,
    secret_pwd_key      STRING      NOT NULL,

    approval_status     STRING      NOT NULL, -- APPROVED | REVOKED | PENDING
    approved_by         STRING,
    approved_at         TIMESTAMP,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,
    updated_at          TIMESTAMP,
    updated_by          STRING,

    CONSTRAINT pk_connections_oracle PRIMARY KEY (connection_id)
)
USING DELTA;

-- =========================================================
-- 4) CONEXÕES SHAREPOINT
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.connections_sharepoint (
    connection_id       STRING      NOT NULL,
    project_id          STRING      NOT NULL,
    area_id             STRING      NOT NULL,

    tenant_id           STRING      NOT NULL,
    site_url            STRING      NOT NULL,
    drive_name          STRING      NOT NULL,
    secret_scope        STRING      NOT NULL,
    secret_client_id    STRING      NOT NULL,
    secret_client_key   STRING      NOT NULL,

    approval_status     STRING      NOT NULL,
    approved_by         STRING,
    approved_at         TIMESTAMP,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,
    updated_at          TIMESTAMP,
    updated_by          STRING,

    CONSTRAINT pk_connections_sharepoint PRIMARY KEY (connection_id)
)
USING DELTA;

-- =========================================================
-- 5) NAMING CONVENTIONS (versionadas)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.naming_conventions (
    naming_version      INT         NOT NULL,
    is_active           BOOLEAN     NOT NULL,

    bronze_pattern      STRING      NOT NULL,
    silver_pattern      STRING      NOT NULL,
    gold_pattern        STRING,

    example_bronze      STRING,
    example_silver      STRING,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,

    CONSTRAINT pk_naming_conventions PRIMARY KEY (naming_version)
)
USING DELTA;

-- =========================================================
-- 6) RBAC DEFAULTS (por projeto / área)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.rbac_defaults (
    rbac_id             STRING      NOT NULL,
    project_id          STRING      NOT NULL,
    area_id             STRING      NOT NULL,

    role_name           STRING      NOT NULL, -- ex: DE_READ, DE_WRITE, ADMIN
    layer               STRING      NOT NULL, -- BRONZE | SILVER | GOLD
    privilege           STRING      NOT NULL, -- SELECT | MODIFY | OWN

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,

    CONSTRAINT pk_rbac_defaults PRIMARY KEY (rbac_id)
)
USING DELTA;

-- =========================================================
-- 7) DATASET CONTROL (núcleo do produto)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.dataset_control (
    dataset_id          STRING      NOT NULL,
    project_id          STRING      NOT NULL,
    area_id             STRING      NOT NULL,

    dataset_name        STRING      NOT NULL,
    source_type         STRING      NOT NULL, -- ORACLE | SHAREPOINT
    connection_id       STRING      NOT NULL,

    execution_state     STRING      NOT NULL, 
    -- DRAFT | ACTIVE | PAUSED | DEPRECATED | BLOCKED_SCHEMA_CHANGE

    bronze_table        STRING      NOT NULL,
    silver_table        STRING      NOT NULL,

    current_schema_ver  INT,
    last_success_run_id STRING,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,
    updated_at          TIMESTAMP,
    updated_by          STRING,

    CONSTRAINT pk_dataset_control PRIMARY KEY (dataset_id)
)
USING DELTA;

-- =========================================================
-- 8) DATASET STATE CHANGES (auditoria)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.dataset_state_changes (
    change_id           STRING      NOT NULL,
    dataset_id          STRING      NOT NULL,

    old_state           STRING      NOT NULL,
    new_state           STRING      NOT NULL,
    reason              STRING,

    changed_at          TIMESTAMP   NOT NULL,
    changed_by          STRING      NOT NULL,

    CONSTRAINT pk_dataset_state_changes PRIMARY KEY (change_id)
)
USING DELTA;

-- =========================================================
-- 9) SCHEMA VERSIONS (Silver)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.schema_versions (
    dataset_id          STRING      NOT NULL,
    schema_version      INT         NOT NULL,

    schema_fingerprint  STRING      NOT NULL,
    expect_schema_json  STRING      NOT NULL,

    status              STRING      NOT NULL, 
    -- ACTIVE | PENDING | REJECTED | DEPRECATED

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,

    CONSTRAINT pk_schema_versions PRIMARY KEY (dataset_id, schema_version)
)
USING DELTA;

-- =========================================================
-- 10) SCHEMA APPROVALS
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ctrl.schema_approvals (
    approval_id         STRING      NOT NULL,
    dataset_id          STRING      NOT NULL,
    schema_version      INT         NOT NULL,

    decision            STRING      NOT NULL, -- APPROVED | REJECTED
    decision_by         STRING      NOT NULL,
    decision_at         TIMESTAMP   NOT NULL,
    comments            STRING,

    CONSTRAINT pk_schema_approvals PRIMARY KEY (approval_id)
)
USING DELTA;

-- =========================================================
-- FIM DO DDL ingestion_sys.ctrl
-- =========================================================
