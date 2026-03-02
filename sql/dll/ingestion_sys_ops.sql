-- sql/ddl/ingestion_sys.ops.sql
-- =========================================================
-- Plataforma de Ingestão Governada
-- Schema: ingestion_sys.ops
-- Objetivo:
--   Tabelas OPERACIONAIS (runtime) do produto:
--   - run_queue (fila de execução governada)
--   - batch_process (execução por dataset/run)
--   - batch_process_table_details (detalhes por tabela/camada)
--   - dataset_watermark (controle incremental/watermark por dataset)
--
-- Requisitos:
--   - Databricks + Unity Catalog
--   - Delta Lake
--   - Scripts idempotentes
-- =========================================================

CREATE SCHEMA IF NOT EXISTS ingestion_sys.ops;

-- =========================================================
-- 1) RUN QUEUE (fila governada)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ops.run_queue (
    queue_id            STRING      NOT NULL,
    dataset_id          STRING      NOT NULL,

    trigger_type        STRING      NOT NULL, 
    -- SCHEDULE | MANUAL | BACKFILL

    requested_by        STRING      NOT NULL,
    requested_at        TIMESTAMP   NOT NULL,

    priority            INT         NOT NULL DEFAULT 100, -- menor = mais prioritário

    status              STRING      NOT NULL,
    -- PENDING | CLAIMED | RUNNING | SUCCEEDED | FAILED | CANCELLED

    claim_owner         STRING,
    claimed_at          TIMESTAMP,

    attempt             INT         NOT NULL DEFAULT 0,
    max_retries         INT         NOT NULL DEFAULT 3,
    next_retry_at       TIMESTAMP,

    last_error_class    STRING,     -- SCHEMA_ERROR | SOURCE_ERROR | RUNTIME_ERROR | UNKNOWN
    last_error_message  STRING,

    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,

    correlation_id      STRING,     -- para rastrear cadeia (portal/admin)
    run_id              STRING,     -- preenche quando batch_process é criado

    CONSTRAINT pk_run_queue PRIMARY KEY (queue_id)
)
USING DELTA;

-- =========================================================
-- 2) BATCH PROCESS (1 execução por dataset)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ops.batch_process (
    run_id              STRING      NOT NULL,
    dataset_id          STRING      NOT NULL,

    queue_id            STRING,     -- se originado do run_queue

    execution_mode      STRING      NOT NULL,
    -- ORCHESTRATED | ADHOC

    status              STRING      NOT NULL,
    -- RUNNING | SUCCEEDED | FAILED | SKIPPED

    started_at          TIMESTAMP   NOT NULL,
    finished_at         TIMESTAMP,

    orchestrator_job_id  STRING,     -- id do workflow/job
    orchestrator_run_id  STRING,     -- id da execução do workflow
    orchestrator_task    STRING,     -- opcional

    bronze_row_count     BIGINT,
    silver_row_count     BIGINT,

    error_class          STRING,     -- SCHEMA_ERROR | SOURCE_ERROR | RUNTIME_ERROR | UNKNOWN
    error_message        STRING,
    error_stacktrace     STRING,

    created_at          TIMESTAMP   NOT NULL,
    created_by          STRING      NOT NULL,

    CONSTRAINT pk_batch_process PRIMARY KEY (run_id)
)
USING DELTA;

-- =========================================================
-- 3) BATCH PROCESS TABLE DETAILS (detalhes por camada/tabela)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ops.batch_process_table_details (
    detail_id           STRING      NOT NULL,
    run_id              STRING      NOT NULL,
    dataset_id          STRING      NOT NULL,

    layer               STRING      NOT NULL, -- BRONZE | SILVER | GOLD
    table_name          STRING      NOT NULL,

    operation           STRING      NOT NULL, -- APPEND | MERGE | OVERWRITE
    started_at          TIMESTAMP   NOT NULL,
    finished_at         TIMESTAMP,

    row_count           BIGINT,
    inserted_count      BIGINT,
    updated_count       BIGINT,
    deleted_count       BIGINT,

    status              STRING      NOT NULL, -- SUCCEEDED | FAILED | SKIPPED
    error_message       STRING,

    CONSTRAINT pk_batch_process_table_details PRIMARY KEY (detail_id)
)
USING DELTA;

-- =========================================================
-- 4) DATASET WATERMARK (controle incremental)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ops.dataset_watermark (
    dataset_id          STRING      NOT NULL,

    watermark_type      STRING      NOT NULL, -- TIMESTAMP | NUMERIC | STRING
    watermark_column    STRING      NOT NULL, -- nome do campo de origem
    watermark_value     STRING,               -- armazenar como string para flexibilidade

    last_run_id         STRING,
    last_updated_at     TIMESTAMP,

    CONSTRAINT pk_dataset_watermark PRIMARY KEY (dataset_id)
)
USING DELTA;

-- =========================================================
-- 5) BATCH PROCESS STEPS (timeline/progresso por fase)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingestion_sys.ops.batch_process_steps (
    step_id            STRING      NOT NULL,
    run_id             STRING      NOT NULL,
    dataset_id         STRING      NOT NULL,

    phase              STRING      NOT NULL, -- ORCHESTRATOR | BRONZE | SILVER | GOLD
    step_key           STRING      NOT NULL, -- ex: BRONZE_READ | BRONZE_WRITE | SILVER_CAST | SILVER_MERGE
    status             STRING      NOT NULL, -- RUNNING | SUCCEEDED | FAILED | SKIPPED

    message            STRING,
    progress_current   BIGINT,
    progress_total     BIGINT,
    details_json       STRING,

    started_at         TIMESTAMP   NOT NULL,
    updated_at         TIMESTAMP,
    finished_at        TIMESTAMP,

    CONSTRAINT pk_batch_process_steps PRIMARY KEY (step_id)
)
USING DELTA;

-- =========================================================
-- FIM DO DDL ingestion_sys.ops
-- =========================================================
