# Databricks notebook source
%run /Workspace/Shared/incremental_loading/incremental_loading_functions

# COMMAND ----------

# VERSION: 2026-02-27-16:35 - FIX: UnboundLocalError json - removed redundant import json inside run_one
print("[IMPORT] ✓ Incremental loading functions imported successfully")
print(f"[IMPORT] Available: _load_oracle_bronze_incremental = {'YES' if '_load_oracle_bronze_incremental' in dir() else 'NOT FOUND'}")

# COMMAND ----------

# Governed Ingestion Orchestrator (MVP)
# - Consome cm_dbx_dev.ingestion_sys_ops.run_queue
# - Executa Bronze (Oracle via JDBC) e tenta promover Silver (quando há contrato ACTIVE)
# - Registra batch_process e batch_process_table_details

# COMMAND ----------

from __future__ import annotations

import json
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# -----------------------------
# Params
# -----------------------------

def _get_widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.get(name)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        try:
            dbutils.widgets.text(name, default)  # type: ignore[name-defined]
            return default
        except Exception:
            return default


CATALOG = _get_widget("catalog", "cm_dbx_dev")
CTRL = f"{CATALOG}.ingestion_sys_ctrl"
OPS = f"{CATALOG}.ingestion_sys_ops"

MAX_ITEMS = int(_get_widget("max_items", "200"))
MAX_PARALLELISM = int(_get_widget("max_parallelism", "5"))  # Batch size: datasets processed per round
TARGET_DATASET_ID = _get_widget("target_dataset_id", "").strip()  # Targeted execution
JOB_ID = _get_widget("job_id", "").strip()  # Scheduled job ID (for job-based execution)

CLAIM_OWNER = _get_widget("claim_owner", f"orchestrator-{uuid.uuid4()}")
JOB_INSTANCE_ID = CLAIM_OWNER

SRE_METRICS: Dict[str, int] = {
    "claim_attempt_count": 0,
    "claim_conflict_count": 0,
    "claim_lost_count": 0,
    "stale_claim_resets": 0,
    "state_transition_invalid_attempt": 0,
}


# -----------------------------
# Errors
# -----------------------------


class SchemaError(Exception):
    pass


class SourceError(Exception):
    pass


# -----------------------------
# Helpers
# -----------------------------


def now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _sql_string_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _metric_inc(name: str, amount: int = 1) -> None:
    if name not in SRE_METRICS:
        SRE_METRICS[name] = 0
    SRE_METRICS[name] += int(amount)


def _retry_delta_sql(
    sql_str: str,
    max_retries: int = 5,
    context: str = "SQL",
    on_conflict_metric: Optional[str] = None,
) -> None:
    """Executa SQL com retry automático para ConcurrentAppendException do Delta Lake.
    
    Necessário quando múltiplos jobs concorrentes escrevem na mesma tabela Delta
    (ex: run_queue, batch_process).
    """
    import time as _time
    for _attempt in range(max_retries):
        try:
            spark.sql(sql_str)  # type: ignore[name-defined]
            return
        except Exception as e:
            if "ConcurrentAppendException" in str(e) and _attempt < max_retries - 1:
                if on_conflict_metric:
                    _metric_inc(on_conflict_metric)
                _wait = 2 ** _attempt  # 1s, 2s, 4s, 8s, 16s
                print(f"[{context}] ⚠️ Conflito Delta (tentativa {_attempt+1}/{max_retries}), retry em {_wait}s...")
                _time.sleep(_wait)
            else:
                raise


def _safe_identifier(name: str) -> bool:
    import re

    # Oracle identifiers commonly include $, #.
    return re.fullmatch(r"[A-Za-z0-9_$#]+", name or "") is not None


def _parse_oracle_table(dataset_name: str, default_owner: str = "CMASTER") -> str:
    # Support:
    # - "OWNER.TABLE"
    # - "TABLE" (defaults OWNER)
    # - "OWNER.TABLE@DBLINK" (Oracle database link)
    # - "TABLE@DBLINK" (defaults OWNER)

    raw = str(dataset_name or "").strip()
    if not raw:
        raise SourceError("Empty oracle table name")

    base = raw
    dblink = None
    if "@" in raw:
        base, dblink = raw.split("@", 1)
        base = base.strip()
        dblink = (dblink or "").strip()

    parts = [p for p in base.split(".") if p]
    if len(parts) == 2:
        owner, table = parts
    else:
        owner, table = default_owner, base

    owner = owner.strip()
    table = table.strip()

    if not (_safe_identifier(owner) and _safe_identifier(table)):
        raise SourceError(f"Invalid oracle table identifier: {raw}")

    if dblink is not None:
        if not dblink or not _safe_identifier(dblink):
            raise SourceError(f"Invalid oracle DB link identifier: {raw}")
        return f"{owner}.{table}@{dblink}"

    return f"{owner}.{table}"


def _table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name)  # type: ignore[name-defined]
        return True
    except Exception:
        return False


def _write_table_details(
    *,
    run_id: str,
    dataset_id: str,
    layer: str,
    table_name: str,
    operation: str,
    status: str,
    row_count: Optional[int] = None,
    inserted_count: Optional[int] = None,
    updated_count: Optional[int] = None,
    deleted_count: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    detail_id = str(uuid.uuid4())

    rc = "NULL" if row_count is None else str(int(row_count))
    ic = "NULL" if inserted_count is None else str(int(inserted_count))
    uc = "NULL" if updated_count is None else str(int(updated_count))
    dc = "NULL" if deleted_count is None else str(int(deleted_count))
    em = "NULL" if not error_message else _sql_string_literal(error_message)

    spark.sql(  # type: ignore[name-defined]
        f"""
        INSERT INTO {OPS}.batch_process_table_details (
          detail_id, run_id, dataset_id,
          layer, table_name,
          operation, started_at, finished_at,
          row_count,
          inserted_count, updated_count, deleted_count,
          status, error_message
        ) VALUES (
          {_sql_string_literal(detail_id)},
          {_sql_string_literal(run_id)},
          {_sql_string_literal(dataset_id)},
          {_sql_string_literal(layer)},
          {_sql_string_literal(table_name)},
          {_sql_string_literal(operation)},
          current_timestamp(),
          current_timestamp(),
          {rc},
          {ic}, {uc}, {dc},
          {_sql_string_literal(status)},
          {em}
        )
        """
    )


def _ensure_steps_table() -> None:
    # Best-effort: do not fail orchestration if UI telemetry table cannot be created.
    try:
        spark.sql(  # type: ignore[name-defined]
            f"""
            CREATE TABLE IF NOT EXISTS {OPS}.batch_process_steps (
              step_id            STRING      NOT NULL,
              run_id             STRING      NOT NULL,
              dataset_id         STRING      NOT NULL,
              phase              STRING      NOT NULL,
              step_key           STRING      NOT NULL,
              status             STRING      NOT NULL,
              message            STRING,
              progress_current   BIGINT,
              progress_total     BIGINT,
              details_json       STRING,
              started_at         TIMESTAMP   NOT NULL,
              updated_at         TIMESTAMP,
              finished_at        TIMESTAMP,
              CONSTRAINT pk_batch_process_steps PRIMARY KEY (step_id)
            )
            USING DELTA
            """
        )
    except Exception:
        pass


def _ensure_run_queue_transitions_table() -> None:
    try:
        spark.sql(  # type: ignore[name-defined]
            f"""
            CREATE TABLE IF NOT EXISTS {OPS}.run_queue_transitions (
              transition_id      STRING    NOT NULL,
              queue_id           STRING    NOT NULL,
              old_status         STRING,
              new_status         STRING    NOT NULL,
              actor              STRING    NOT NULL,
              claim_token        STRING,
              reason             STRING,
              state_version      BIGINT,
              created_at         TIMESTAMP NOT NULL
            )
            USING DELTA
            """
        )
    except Exception:
        pass


def _record_queue_transition(
    *,
    queue_id: str,
    old_status: Optional[str],
    new_status: str,
    actor: str,
    claim_token: Optional[str],
    reason: Optional[str],
    state_version: Optional[int],
) -> None:
    try:
        spark.sql(  # type: ignore[name-defined]
            f"""
            INSERT INTO {OPS}.run_queue_transitions (
              transition_id, queue_id, old_status, new_status, actor, claim_token, reason, state_version, created_at
            ) VALUES (
              {_sql_string_literal(str(uuid.uuid4()))},
              {_sql_string_literal(queue_id)},
              {"NULL" if old_status is None else _sql_string_literal(old_status)},
              {_sql_string_literal(new_status)},
              {_sql_string_literal(actor)},
              {"NULL" if not claim_token else _sql_string_literal(claim_token)},
              {"NULL" if not reason else _sql_string_literal(reason)},
              {"NULL" if state_version is None else str(int(state_version))},
              current_timestamp()
            )
            """
        )
    except Exception:
        pass


def _insert_step(
    *,
    run_id: str,
    dataset_id: str,
    phase: str,
    step_key: str,
    status: str,
    message: Optional[str] = None,
    progress_current: Optional[int] = None,
    progress_total: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    try:
        step_id = str(uuid.uuid4())
        msg = "NULL" if not message else _sql_string_literal(message)
        pc = "NULL" if progress_current is None else str(int(progress_current))
        pt = "NULL" if progress_total is None else str(int(progress_total))
        dj = "NULL" if not details else _sql_string_literal(json.dumps(details, ensure_ascii=False, default=str))

        spark.sql(  # type: ignore[name-defined]
            f"""
            INSERT INTO {OPS}.batch_process_steps (
              step_id, run_id, dataset_id,
              phase, step_key, status,
              message, progress_current, progress_total, details_json,
              started_at, updated_at, finished_at
            ) VALUES (
              {_sql_string_literal(step_id)},
              {_sql_string_literal(run_id)},
              {_sql_string_literal(dataset_id)},
              {_sql_string_literal(phase)},
              {_sql_string_literal(step_key)},
              {_sql_string_literal(status)},
              {msg},
              {pc},
              {pt},
              {dj},
              current_timestamp(),
              current_timestamp(),
              NULL
            )
            """
        )
        return step_id
    except Exception:
        return None


def _update_step(
    *,
    step_id: Optional[str],
    status: Optional[str] = None,
    message: Optional[str] = None,
    progress_current: Optional[int] = None,
    progress_total: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
    finished: bool = False,
) -> None:
    if not step_id:
        return

    try:
        sets = ["updated_at = current_timestamp()"]
        if status is not None:
            sets.append(f"status = {_sql_string_literal(status)}")
        if message is not None:
            sets.append(f"message = {_sql_string_literal(message)}")
        if progress_current is not None:
            sets.append(f"progress_current = {int(progress_current)}")
        if progress_total is not None:
            sets.append(f"progress_total = {int(progress_total)}")
        if details is not None:
            sets.append(f"details_json = {_sql_string_literal(json.dumps(details, ensure_ascii=False, default=str))}")
        if finished:
            sets.append("finished_at = current_timestamp()")

        spark.sql(  # type: ignore[name-defined]
            f"UPDATE {OPS}.batch_process_steps SET {', '.join(sets)} WHERE step_id = {_sql_string_literal(step_id)}"
        )
    except Exception:
        pass


def _set_run_queue_status(
    *,
    queue_id: str,
    status: str,
    run_id: Optional[str] = None,
    error_class: Optional[str] = None,
    error_message: Optional[str] = None,
    started: bool = False,
    finished: bool = False,
    next_retry_at_minutes: Optional[int] = None,
    attempt_inc: bool = False,
    claim_token: Optional[str] = None,
) -> None:
    sets = [f"status = {_sql_string_literal(status)}"]

    # When a new attempt starts, clear previous error fields to avoid confusing diagnostics.
    if status == "RUNNING":
        sets.append("last_error_class = NULL")
        sets.append("last_error_message = NULL")

    if run_id is not None:
        sets.append(f"run_id = {_sql_string_literal(run_id)}")

    if started:
        sets.append("started_at = current_timestamp()")

    if finished:
        sets.append("finished_at = current_timestamp()")

    if error_class is not None:
        sets.append(f"last_error_class = {_sql_string_literal(error_class)}")

    if error_message is not None:
        sets.append(f"last_error_message = {_sql_string_literal(error_message)}")

    if attempt_inc:
        sets.append("attempt = attempt + 1")
        sets.append("attempt_number = COALESCE(attempt_number, attempt, 0) + 1")

    if next_retry_at_minutes is not None:
        sets.append(f"next_retry_at = current_timestamp() + INTERVAL {int(next_retry_at_minutes)} MINUTES")
    else:
        # clear next_retry_at when finalizing
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            sets.append("next_retry_at = NULL")

    where_clause = f"queue_id = {_sql_string_literal(queue_id)}"
    ownership_guard_statuses = ("RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "PENDING")
    if claim_token and status in ownership_guard_statuses:
        where_clause += f" AND claim_token = {_sql_string_literal(claim_token)}"

    current_rows = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT status, state_version
        FROM {OPS}.run_queue
        WHERE {where_clause}
        LIMIT 1
        """
    ).collect()
    if not current_rows:
        _metric_inc("state_transition_invalid_attempt")
        print(f"[QUEUE_STATUS] ⚠️ Transição inválida ignorada: queue_id={queue_id}, status={status}")
        return

    old_status = str(current_rows[0]["status"]) if current_rows[0]["status"] is not None else None

    _retry_delta_sql(
        f"UPDATE {OPS}.run_queue SET {', '.join(sets)}, state_version = COALESCE(state_version, 0) + 1 WHERE {where_clause}",
        context="QUEUE_STATUS"
    )

    new_rows = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT status, state_version
        FROM {OPS}.run_queue
        WHERE queue_id = {_sql_string_literal(queue_id)}
        LIMIT 1
        """
    ).collect()
    new_state_version = int(new_rows[0]["state_version"]) if new_rows and new_rows[0]["state_version"] is not None else None
    _record_queue_transition(
        queue_id=queue_id,
        old_status=old_status,
        new_status=status,
        actor=CLAIM_OWNER,
        claim_token=claim_token,
        reason=error_class or None,
        state_version=new_state_version,
    )


def _create_batch_process(*, run_id: str, dataset_id: str, queue_id: str) -> None:
    spark.sql(  # type: ignore[name-defined]
        f"""
        INSERT INTO {OPS}.batch_process (
          run_id, dataset_id, queue_id,
          execution_mode, status,
          started_at, finished_at,
          orchestrator_job_id, orchestrator_run_id, orchestrator_task,
          bronze_row_count, silver_row_count,
          error_class, error_message, error_stacktrace,
          load_type, incremental_rows_read, watermark_start, watermark_end,
          created_at, created_by
        ) VALUES (
          {_sql_string_literal(run_id)},
          {_sql_string_literal(dataset_id)},
          {_sql_string_literal(queue_id)},
          'ORCHESTRATED',
          'RUNNING',
          current_timestamp(),
          NULL,
          NULL, NULL, NULL,
          NULL, NULL,
          NULL, NULL, NULL,
          NULL, NULL, NULL, NULL,
          current_timestamp(),
          {_sql_string_literal('orchestrator')}
        )
        """
    )


def _finish_batch_process(
    *,
    run_id: str,
    status: str,
    bronze_row_count: Optional[int] = None,
    silver_row_count: Optional[int] = None,
    error_class: Optional[str] = None,
    error_message: Optional[str] = None,
    error_stacktrace: Optional[str] = None,
    load_type: Optional[str] = None,
    incremental_rows_read: Optional[int] = None,
    watermark_start: Optional[str] = None,
    watermark_end: Optional[str] = None,
) -> None:
    sets = [f"status = {_sql_string_literal(status)}", "finished_at = current_timestamp()"]

    if bronze_row_count is not None:
        sets.append(f"bronze_row_count = {int(bronze_row_count)}")

    if silver_row_count is not None:
        sets.append(f"silver_row_count = {int(silver_row_count)}")

    if error_class is not None:
        sets.append(f"error_class = {_sql_string_literal(error_class)}")

    if error_message is not None:
        sets.append(f"error_message = {_sql_string_literal(error_message)}")

    if error_stacktrace is not None:
        sets.append(f"error_stacktrace = {_sql_string_literal(error_stacktrace)}")

    # Novos campos para tracking de carga incremental
    if load_type is not None:
        sets.append(f"load_type = {_sql_string_literal(load_type)}")

    if incremental_rows_read is not None:
        sets.append(f"incremental_rows_read = {int(incremental_rows_read)}")

    if watermark_start is not None:
        sets.append(f"watermark_start = {_sql_string_literal(watermark_start)}")

    if watermark_end is not None:
        sets.append(f"watermark_end = {_sql_string_literal(watermark_end)}")

    spark.sql(  # type: ignore[name-defined]
        f"UPDATE {OPS}.batch_process SET {', '.join(sets)} WHERE run_id = {_sql_string_literal(run_id)}"
    )


def _get_active_schema(dataset_id: str) -> Optional[Dict[str, Any]]:
    rows = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT schema_version, expect_schema_json
        FROM {CTRL}.schema_versions
        WHERE dataset_id = {_sql_string_literal(dataset_id)} AND status = 'ACTIVE'
        ORDER BY schema_version DESC
        LIMIT 1
        """
    ).collect()

    if not rows:
        return None

    js = rows[0]["expect_schema_json"]
    return json.loads(js) if js else None


def _auto_create_schema_from_bronze(dataset_id: str, bronze_table: str) -> Dict[str, Any]:
    """
    Auto-generates and persists a schema from Bronze table structure.
    Used for DRAFT datasets on first execution.
    
    Returns the created schema dict.
    """
    print(f"[AUTO_SCHEMA] Auto-gerando schema a partir da tabela Bronze...")
    print(f"[AUTO_SCHEMA] Bronze table: {bronze_table}")
    
    # Get Bronze table schema
    bronze_df = spark.table(bronze_table)  # type: ignore[name-defined]
    spark_schema = bronze_df.schema
    
    # Convert Spark schema to our contract format
    # IMPORTANT: Skip technical metadata columns (_ingestion_ts, _batch_id, _source_table,
    # _op, _watermark_col, _watermark_value, _row_hash, _is_deleted) — these are Bronze-only
    # and must NOT be in the Silver schema contract.
    columns = []
    for field in spark_schema.fields:
        if field.name.startswith("_"):
            continue
        col_def = {
            "name": field.name,
            "type": str(field.dataType.simpleString()).lower(),
            "nullable": field.nullable,
        }
        
        # Handle decimal type (preserve precision/scale)
        if "decimal" in col_def["type"]:
            col_def["type"] = "decimal"
            # Extract precision/scale from "decimal(p,s)"
            import re
            match = re.search(r"decimal\((\d+),(\d+)\)", field.dataType.simpleString().lower())
            if match:
                col_def["decimal"] = {
                    "precision": int(match.group(1)),
                    "scale": int(match.group(2))
                }
            else:
                col_def["decimal"] = {"precision": 38, "scale": 10}
        
        columns.append(col_def)
    
    print(f"[AUTO_SCHEMA] ✓ Schema inferido: {len(columns)} colunas")
    for col in columns:
        print(f"[AUTO_SCHEMA]   - {col['name']}: {col['type']}")
    
    # Build schema contract — enrich with PK/watermark from incremental_metadata when available
    auto_pk: List[str] = []
    auto_watermark: Optional[Dict[str, str]] = None
    
    try:
        ds_inc_rows = spark.sql(  # type: ignore[name-defined]
            f"SELECT enable_incremental, incremental_metadata FROM {CTRL}.dataset_control WHERE dataset_id = {_sql_string_literal(dataset_id)} LIMIT 1"
        ).collect()
        if ds_inc_rows:
            _ds_inc = ds_inc_rows[0].asDict()
            if _ds_inc.get("enable_incremental"):
                _inc_meta_json = _ds_inc.get("incremental_metadata")
                if _inc_meta_json:
                    _inc_meta = json.loads(_inc_meta_json) if isinstance(_inc_meta_json, str) else _inc_meta_json
                    _inc_wm_col = _inc_meta.get("watermark_column") or _inc_meta.get("watermark_col")
                    if _inc_wm_col:
                        # Detect type from bronze columns
                        _wm_type = "string"
                        for _c in columns:
                            if _c["name"] == _inc_wm_col:
                                _wm_type = "timestamp" if "timestamp" in _c["type"] else "string"
                                break
                        auto_watermark = {"column": _inc_wm_col, "type": _wm_type}
                        print(f"[AUTO_SCHEMA] ✓ Watermark configurado: {_inc_wm_col} ({_wm_type})")
                    _inc_pk = _inc_meta.get("pk", [])
                    if _inc_pk:
                        auto_pk = _inc_pk
                        print(f"[AUTO_SCHEMA] ✓ PK configurada: {auto_pk}")
    except Exception as _inc_err:
        print(f"[AUTO_SCHEMA] ⚠️ Não foi possível ler incremental_metadata: {_inc_err}")
    
    schema_json = {
        "columns": columns,
        "primary_key": auto_pk,
        "order_column": None,
        "watermark": auto_watermark,
    }
    
    # Insert into schema_versions with version=1, status=ACTIVE
    schema_json_str = json.dumps(schema_json, ensure_ascii=False)
    
    # Generate fingerprint for schema (simple hash of JSON string)
    import hashlib
    schema_fingerprint = hashlib.md5(schema_json_str.encode('utf-8')).hexdigest()
    
    print(f"[AUTO_SCHEMA] Persistindo schema na tabela schema_versions...")
    spark.sql(  # type: ignore[name-defined]
        f"""
        INSERT INTO {CTRL}.schema_versions (
            dataset_id, schema_version, schema_fingerprint, status, expect_schema_json,
            created_at, created_by
        ) VALUES (
            {_sql_string_literal(dataset_id)},
            1,
            {_sql_string_literal(schema_fingerprint)},
            'ACTIVE',
            {_sql_string_literal(schema_json_str)},
            current_timestamp(),
            'orchestrator'
        )
        """
    )
    
    # Update dataset_control.current_schema_ver = 1
    print(f"[AUTO_SCHEMA] Atualizando dataset_control.current_schema_ver=1...")
    spark.sql(  # type: ignore[name-defined]
        f"""
        UPDATE {CTRL}.dataset_control
        SET current_schema_ver = 1,
            updated_at = current_timestamp(),
            updated_by = 'orchestrator'
        WHERE dataset_id = {_sql_string_literal(dataset_id)}
        """
    )
    
    print(f"[AUTO_SCHEMA] ✓ Schema criado com sucesso! (version=1, status=ACTIVE)")
    return schema_json


def _sync_schema_with_incremental(dataset_id: str, schema: Dict[str, Any], ds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sincroniza schema contract com incremental_metadata quando enable_incremental=True.
    
    Resolve o cenário onde:
    - Schema foi auto-gerado ANTES de enable_incremental ser ativado
    - Usuário habilitou incremental depois, mas schema ficou com watermark=null e pk=[]
    
    Se detectar divergência, atualiza in-memory E persiste na schema_versions.
    """
    if not ds.get("enable_incremental"):
        return schema
    
    metadata_json = ds.get("incremental_metadata")
    if not metadata_json:
        return schema
    
    try:
        meta = json.loads(metadata_json) if isinstance(metadata_json, str) else metadata_json
    except Exception:
        return schema
    
    wm_col = meta.get("watermark_column") or meta.get("watermark_col")
    pk = meta.get("pk", []) or []
    
    schema_wm = schema.get("watermark")
    schema_pk = schema.get("primary_key") or []
    
    needs_update = False
    
    # Enrich watermark if missing in schema but present in incremental_metadata
    if wm_col and not schema_wm:
        wm_type = "string"
        for c in (schema.get("columns") or []):
            if c["name"] == wm_col:
                wm_type = "timestamp" if "timestamp" in str(c.get("type", "")).lower() else "string"
                break
        schema["watermark"] = {"column": wm_col, "type": wm_type}
        needs_update = True
        print(f"[SYNC_SCHEMA] ✓ Watermark adicionado ao schema: {wm_col} ({wm_type})")
    
    # Enrich PK if missing in schema but present in incremental_metadata
    if pk and not schema_pk:
        schema["primary_key"] = pk
        needs_update = True
        print(f"[SYNC_SCHEMA] ✓ Primary Key adicionada ao schema: {pk}")
    
    # order_column is required by _dedupe_lww when PK is set
    if (schema.get("primary_key") or []) and not schema.get("order_column"):
        _order_col = wm_col or (schema.get("watermark") or {}).get("column")
        if _order_col:
            schema["order_column"] = _order_col
            needs_update = True
            print(f"[SYNC_SCHEMA] ✓ order_column definido: {_order_col}")
    
    if needs_update:
        # Persist updated schema to schema_versions (UPDATE existing ACTIVE row)
        try:
            schema_json_str = json.dumps(schema, ensure_ascii=False)
            import hashlib
            schema_fingerprint = hashlib.md5(schema_json_str.encode('utf-8')).hexdigest()
            
            spark.sql(  # type: ignore[name-defined]
                f"""
                UPDATE {CTRL}.schema_versions
                SET expect_schema_json = {_sql_string_literal(schema_json_str)},
                    schema_fingerprint = {_sql_string_literal(schema_fingerprint)},
                    created_at = current_timestamp(),
                    created_by = 'sync_incremental'
                WHERE dataset_id = {_sql_string_literal(dataset_id)} AND status = 'ACTIVE'
                """
            )
            print(f"[SYNC_SCHEMA] ✓ Schema persistido na schema_versions")
        except Exception as persist_err:
            print(f"[SYNC_SCHEMA] ⚠️ Erro ao persistir schema (continuando com in-memory): {persist_err}")
    
    return schema


def _oracle_table_exists(*, jdbc_url: str, user: str, pwd: str, oracle_table: str) -> Dict[str, Any]:
    """
    Check if Oracle table exists and return metadata.
    Returns dict with: exists (bool), owner (str), table_name (str), error_message (str)
    """
    result = {"exists": False, "owner": None, "table_name": None, "error_message": None}
    
    try:
        base = oracle_table
        dblink = None
        if "@" in oracle_table:
            base, dblink = oracle_table.split("@", 1)

        base = base.strip()
        parts = [p for p in base.split(".") if p]
        if len(parts) != 2:
            result["error_message"] = f"Invalid table format: {oracle_table}"
            return result

        owner, table = parts[0].upper(), parts[1].upper()
        
        # CRITICAL FIX: Cannot query ALL_TABLES@DBLINK remotely (Oracle restriction)
        # For DBLink tables, skip validation and assume table exists (will fail at read time if not)
        if dblink:
            result["exists"] = True
            result["owner"] = owner
            result["table_name"] = table
            result["error_message"] = None
            return result
        
        # For local tables, validate via ALL_TABLES
        q = f"(SELECT owner, table_name FROM all_tables WHERE owner = '{owner}' AND table_name = '{table}') t"

        rows = (
            spark.read.format("jdbc")  # type: ignore[name-defined]
            .option("url", jdbc_url)
            .option("dbtable", q)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
            .limit(1)
            .collect()
        )

        if rows and len(rows) > 0:
            result["exists"] = True
            result["owner"] = str(rows[0]["OWNER"])
            result["table_name"] = str(rows[0]["TABLE_NAME"])
        else:
            result["error_message"] = f"Table {owner}.{table} not found in ALL_TABLES or no SELECT privilege"
            
        return result
    except Exception as e:
        result["error_message"] = f"Error checking table existence: {e}"
        return result


def _oracle_estimate_num_rows(*, jdbc_url: str, user: str, pwd: str, oracle_table: str) -> Optional[int]:
    # Best-effort estimate (uses ALL_TABLES.NUM_ROWS). May return None if privileges/stats are missing.
    try:
        base = oracle_table
        dblink = None
        if "@" in oracle_table:
            base, dblink = oracle_table.split("@", 1)

        base = base.strip()
        parts = [p for p in base.split(".") if p]
        if len(parts) != 2:
            return None

        owner, table = parts[0].upper(), parts[1].upper()
        
        # CRITICAL FIX: Cannot query ALL_TABLES@DBLINK remotely (Oracle restriction)
        # For DBLink tables, skip row count estimation
        if dblink:
            return None
        
        q = f"(SELECT num_rows FROM all_tables WHERE owner = '{owner}' AND table_name = '{table}') t"

        rows = (
            spark.read.format("jdbc")  # type: ignore[name-defined]
            .option("url", jdbc_url)
            .option("dbtable", q)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
            .limit(1)
            .collect()
        )

        if not rows:
            return None

        v = rows[0][0]
        return int(v) if v is not None else None
    except Exception:
        return None


def _load_oracle_bronze(*, dataset_id: str, dataset_name: str, connection_id: str, bronze_table: str, run_id: str = "") -> Dict[str, Any]:
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ========== Iniciando carga Oracle ==========")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] dataset_id={dataset_id}")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] dataset_name={dataset_name}")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] connection_id={connection_id}")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] bronze_table={bronze_table}")
    
    # load performance configuration from dataset_control
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Carregando configurações de performance...")
    perf_cfg = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT 
            COALESCE(oracle_fetchsize, 10000) as fetchsize,
            spark_num_partitions,
            jdbc_partition_column,
            jdbc_lower_bound,
            jdbc_upper_bound,
            jdbc_num_partitions
        FROM {CTRL}.dataset_control
        WHERE dataset_id = {_sql_string_literal(dataset_id)}
        LIMIT 1
        """
    ).collect()
    
    if perf_cfg:
        fetchsize = int(perf_cfg[0]["fetchsize"] or 10000)
        spark_partitions = perf_cfg[0]["spark_num_partitions"]
        jdbc_part_col = perf_cfg[0]["jdbc_partition_column"]
        jdbc_lower = perf_cfg[0]["jdbc_lower_bound"]
        jdbc_upper = perf_cfg[0]["jdbc_upper_bound"]
        jdbc_num_parts = perf_cfg[0]["jdbc_num_partitions"]
        
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Configurações carregadas:")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   - Fetchsize: {fetchsize}")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   - Spark partitions: {spark_partitions or 'auto'}")
        if jdbc_part_col:
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   - JDBC Partitioning: ENABLED")
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE]     • Coluna: {jdbc_part_col}")
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE]     • Range: {jdbc_lower} → {jdbc_upper}")
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE]     • Partições: {jdbc_num_parts}")
        else:
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   - JDBC Partitioning: DISABLED (leitura sequencial)")
    else:
        # Fallback to defaults if not configured
        fetchsize = 10000
        spark_partitions = None
        jdbc_part_col = None
        jdbc_lower = None
        jdbc_upper = None
        jdbc_num_parts = None
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ⚠ Usando configurações padrão (fetchsize=10000)")
    
    # load connection metadata
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Carregando metadados da conexão...")
    c = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT jdbc_url, secret_scope, secret_user_key, secret_pwd_key, approval_status
        FROM {CTRL}.connections_oracle
        WHERE connection_id = {_sql_string_literal(connection_id)}
        LIMIT 1
        """
    ).collect()

    if not c:
        msg = f"Oracle connection not found: {connection_id}"
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ ERRO: {msg}")
        raise SourceError(msg)

    approval_status = str(c[0]["approval_status"] or "")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] approval_status={approval_status}")
    if approval_status != "APPROVED":
        msg = f"Oracle connection not approved: {connection_id} (status={approval_status})"
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ ERRO: {msg}")
        raise SourceError(msg)

    jdbc_url = str(c[0]["jdbc_url"])
    secret_scope = str(c[0]["secret_scope"])
    secret_user_key = str(c[0]["secret_user_key"])
    secret_pwd_key = str(c[0]["secret_pwd_key"])
    
    # Mascarar JDBC URL sensível para log seguro
    jdbc_url_safe = jdbc_url[:50] + "..." if len(jdbc_url) > 50 else jdbc_url
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] JDBC URL={jdbc_url_safe}")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] secret_scope={secret_scope}")

    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Recuperando credenciais do Databricks Secrets...")
    try:
        user = dbutils.secrets.get(secret_scope, secret_user_key)  # type: ignore[name-defined]
        pwd = dbutils.secrets.get(secret_scope, secret_pwd_key)  # type: ignore[name-defined]
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Credenciais recuperadas (user length={len(user)}, pwd length={len(pwd)})")
    except Exception as e:
        msg = f"Failed to read secrets for connection {connection_id}: {e}"
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ ERRO ao ler secrets: {msg}")
        raise SourceError(msg)

    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Fazendo parse da tabela Oracle: '{dataset_name}'")
    oracle_table = _parse_oracle_table(dataset_name)
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Tabela parseada: '{oracle_table}'")

    # Validate table exists BEFORE attempting JDBC read
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Validando existência da tabela Oracle...")
    table_check = _oracle_table_exists(jdbc_url=jdbc_url, user=user, pwd=pwd, oracle_table=oracle_table)
    if not table_check["exists"]:
        error_msg = table_check.get("error_message") or "Table not found or no access"
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ ERRO: Tabela não encontrada ou sem permissão!")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ Detalhes: {error_msg}")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ Sugestões:")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   1. Verifique se a tabela existe: SELECT * FROM all_tables WHERE owner='CMASTER' AND table_name='{oracle_table.split('.')[-1].upper()}'")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   2. Verifique permissões: GRANT SELECT ON {oracle_table} TO <usuario>")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE]   3. Verifique se o dataset_name está correto na tabela dataset_control")
        raise SourceError(f"Oracle table validation failed: {error_msg}")
    
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Tabela encontrada: {table_check['owner']}.{table_check['table_name']}")

    # If using DBLINK, do a fast preflight query so failures are immediate and explicit.
    if "@" in oracle_table:
        dblink = oracle_table.split("@", 1)[1]
        ping_sql = f"(SELECT 1 AS ping FROM dual@{dblink}) ping"
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] DBLink detectado: '{dblink}'")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Executando preflight check (testando conectividade)...")
        try:
            (
                spark.read.format("jdbc")  # type: ignore[name-defined]
                .option("url", jdbc_url)
                .option("dbtable", ping_sql)
                .option("user", user)
                .option("password", pwd)
                .option("driver", "oracle.jdbc.OracleDriver")
                .load()
                .limit(1)
                .collect()
            )
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Preflight check passou - DBLink acessível")
        except Exception as e:
            msg = f"DBLINK_PRECHECK_FAILED ({dblink}): {e}"
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ ERRO no preflight check: {msg}")
            raise SourceError(msg)

    # Spark JDBC sometimes fails schema resolution for OWNER.TABLE@DBLINK when passed as plain dbtable.
    # Wrapping as a subquery forces the driver to treat it as a query source.
    dbtable = oracle_table
    if "@" in oracle_table:
        dbtable = f"(SELECT * FROM {oracle_table}) src"
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Usando subquery wrapper para DBLink")
    
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Iniciando leitura JDBC...")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] dbtable='{dbtable}'")

    try:
        # Base JDBC reader com fetchsize otimizado
        reader = (
            spark.read.format("jdbc")  # type: ignore[name-defined]
            .option("url", jdbc_url)
            .option("dbtable", dbtable)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("fetchsize", str(fetchsize))
        )
        
        # Add JDBC partitioning if configured
        if jdbc_part_col and jdbc_lower is not None and jdbc_upper is not None and jdbc_num_parts:
            print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ⚡ Aplicando particionamento JDBC paralelo...")
            reader = (
                reader
                .option("partitionColumn", jdbc_part_col)
                .option("lowerBound", str(jdbc_lower))
                .option("upperBound", str(jdbc_upper))
                .option("numPartitions", str(jdbc_num_parts))
            )
        
        df = reader.load()
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ DataFrame JDBC criado com sucesso")
    except Exception as e:
        msg = (
            "Failed to read from Oracle via JDBC. If the driver is missing, install Oracle JDBC driver on the job cluster. "
            f"Details: {e}"
        )
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✗ ERRO ao criar DataFrame JDBC:")
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] {msg}")
        raise SourceError(msg)

    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Estimando número de linhas na origem...")
    source_estimate = _oracle_estimate_num_rows(jdbc_url=jdbc_url, user=user, pwd=pwd, oracle_table=oracle_table)
    if source_estimate is not None:
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Estimativa da origem: {source_estimate:,} linhas")
    else:
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Não foi possível estimar (stats ausentes ou sem permissão)")
    
    # Clean column names (remove spaces and special characters)
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Limpando nomes de colunas...")
    old_columns = df.schema.names
    cleaned_count = 0
    for col_name in old_columns:
        clean_name = col_name.replace(" ", "").replace("-", "_").replace(".", "_")
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
            cleaned_count += 1
    if cleaned_count > 0:
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] 🧹 {cleaned_count} colunas renomeadas (espaços/caracteres especiais removidos)")
    else:
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Nomes de colunas já estão limpos")
    
    # -------------------------------------------------------
    # Add 8 technical metadata columns (consistent with incremental)
    # -------------------------------------------------------
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Adicionando colunas técnicas de metadados...")
    df = df.withColumn("_ingestion_ts", F.current_timestamp())
    df = df.withColumn("_batch_id", F.lit(run_id) if run_id else F.lit(None).cast("string"))
    df = df.withColumn("_source_table", F.lit(oracle_table))
    df = df.withColumn("_op", F.lit("FULL_REFRESH"))
    df = df.withColumn("_watermark_col", F.lit(None).cast("string"))
    df = df.withColumn("_watermark_value", F.lit(None).cast("string"))
    df = df.withColumn("_row_hash", F.lit(None).cast("string"))
    df = df.withColumn("_is_deleted", F.lit(False))
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ 8 colunas técnicas adicionadas (FULL_REFRESH)")

    # Dynamic repartitioning before write
    if spark_partitions:
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Reparticionando para escrita: {spark_partitions} partições (configurado)")
        df = df.repartition(spark_partitions)
    elif source_estimate:
        # Auto-calculate optimal partitions based on row count
        if source_estimate < 1_000_000:
            optimal_parts = 200
        elif source_estimate < 10_000_000:
            optimal_parts = 400
        elif source_estimate < 50_000_000:
            optimal_parts = 800
        else:
            optimal_parts = 1600
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Reparticionando para escrita: {optimal_parts} partições (auto-calculado)")
        df = df.repartition(optimal_parts)
    else:
        # Default for unknown size
        print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Reparticionando para escrita: 800 partições (padrão)")
        df = df.repartition(800)

    # Bronze MVP: overwrite full snapshot.
    # Note: avoid df.count() here (it can trigger a second full Oracle scan). Count from Delta after write instead.
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Escrevendo dados na tabela Delta: '{bronze_table}'")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Mode: OVERWRITE (snapshot completo)")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Schema: overwriteSchema=true (reset completo)")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Escrita Delta concluída")
    
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] Contando linhas gravadas...")
    loaded = int(spark.table(bronze_table).count())  # type: ignore[name-defined]
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ✓ Carga concluída: {loaded:,} linhas carregadas")
    print(f"[{now_utc_iso()}] [BRONZE:ORACLE] ========== Carga Oracle finalizada ==========\n")

    return {
        "oracle_table": oracle_table,
        "source_estimate": source_estimate,
        "bronze_row_count": loaded,
    }


def _apply_cast_plan(df: DataFrame, schema: Dict[str, Any]) -> DataFrame:
    cols = schema.get("columns") or []
    select_exprs = []

    for c in cols:
        name = c["name"]
        typ = str(c["type"]).lower()

        src = F.col(name)
        src_str = F.col(name).cast("string")

        if typ in ("string", "binary"):
            expr = src.cast(typ)
        elif typ in ("int", "long", "short", "byte", "float", "double"):
            cleaned = F.regexp_replace(src_str, r"\\s+", "")
            cleaned = F.when(cleaned == F.lit(""), F.lit(None)).otherwise(cleaned)
            expr = cleaned.cast(typ)
        elif typ == "boolean":
            cleaned = F.lower(F.trim(src_str))
            cleaned = F.when(cleaned == F.lit(""), F.lit(None)).otherwise(cleaned)
            expr = cleaned.cast("boolean")
        elif typ in ("date", "timestamp"):
            cleaned = F.trim(src_str)
            cleaned = F.when(cleaned == F.lit(""), F.lit(None)).otherwise(cleaned)
            expr = cleaned.cast(typ)
        elif typ == "decimal":
            dec = c.get("decimal") or {}
            p = int(dec.get("precision", 38))
            s = int(dec.get("scale", 0))
            cleaned = F.regexp_replace(src_str, r"\\s+", "")
            cleaned = F.when(cleaned == F.lit(""), F.lit(None)).otherwise(cleaned)
            expr = cleaned.cast(f"decimal({p},{s})")
        else:
            raise SchemaError(f"Unsupported type in contract: {typ} ({name})")

        select_exprs.append(expr.alias(name))

    out = df.select(*select_exprs)

    # nullability check (MVP)
    for c in cols:
        if not bool(c.get("nullable", True)):
            name = c["name"]
            n = out.filter(F.col(name).isNull()).limit(1).count()
            if n > 0:
                raise SchemaError(f"NULLABILITY_VIOLATION: {name}")

    return out


def _dedupe_lww(df: DataFrame, schema: Dict[str, Any]) -> DataFrame:
    pk = schema.get("primary_key") or []
    order_col = schema.get("order_column")

    if not pk:
        return df

    if not order_col:
        raise SchemaError("PRIMARY_KEY requires order_column")

    w = Window.partitionBy(*[F.col(c) for c in pk]).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _merge_to_silver(df: DataFrame, silver_table: str, schema: Dict[str, Any], write_mode: str = "auto", replace_condition: str = None) -> int:
    """
    Promove dados para Silver com MERGE (PK) ou replaceWhere/OVERWRITE/APPEND (sem PK).
    
    write_mode:
      - "auto": MERGE se PK, senão OVERWRITE (padrão seguro)
      - "REPLACE_WHERE": replaceWhere atômico pela janela de watermark (SEM PK incremental)
      - "APPEND": Sempre append (para APPEND_LOG)
      - "OVERWRITE": Sempre overwrite
    
    replace_condition:
      - Expressão SQL para replaceWhere (ex: "DATALT >= CURRENT_TIMESTAMP() - INTERVAL 3 DAYS")
    """
    from delta.tables import DeltaTable

    # Create empty table if missing
    table_is_new = False
    if not _table_exists(silver_table):
        table_is_new = True
        cols = schema.get("columns") or []
        ddl_cols = []
        for c in cols:
            name = c["name"]
            typ = str(c["type"]).lower()
            if typ == "decimal":
                dec = c.get("decimal") or {}
                p = int(dec.get("precision", 38))
                s = int(dec.get("scale", 0))
                ddl_cols.append(f"{name} DECIMAL({p},{s})")
            else:
                ddl_cols.append(f"{name} {typ.upper()}")

        spark.sql(  # type: ignore[name-defined]
            f"CREATE TABLE IF NOT EXISTS {silver_table} (\n  " + ",\n  ".join(ddl_cols) + "\n) USING DELTA"
        )

    pk = schema.get("primary_key") or []
    if not pk:
        # Sem PK: decidir entre REPLACE_WHERE, OVERWRITE e APPEND
        if write_mode == "REPLACE_WHERE" and replace_condition and not table_is_new:
            # replaceWhere atômico: substitui apenas a janela de watermark na Silver
            print(f"[SILVER] Sem PK → replaceWhere: {replace_condition}")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", replace_condition)
                .option("mergeSchema", "true")
                .saveAsTable(silver_table)
            )
        elif write_mode == "APPEND":
            print(f"[SILVER] Sem PK → APPEND (APPEND_LOG mode)")
            df.write.format("delta").mode("append").saveAsTable(silver_table)
        else:
            # OVERWRITE: tabela nova ou fallback seguro
            print(f"[SILVER] Sem PK → OVERWRITE (tabela nova ou fallback)")
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
        return int(df.count())

    dt = DeltaTable.forName(spark, silver_table)  # type: ignore[name-defined]
    cond = " AND ".join([f"t.{c} = s.{c}" for c in pk])
    (
        dt.alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    return int(df.count())


def _update_watermark(dataset_id: str, schema: Dict[str, Any], df_casted: DataFrame, run_id: str) -> None:
    wm = schema.get("watermark")
    if not wm:
        return

    col = str(wm.get("column"))
    typ = str(wm.get("type", "string")).lower()

    if col not in df_casted.columns:
        return

    max_row = df_casted.agg(F.max(F.col(col)).alias("mx")).collect()[0]
    mx = max_row["mx"]
    if mx is None:
        return

    if typ == "timestamp":
        value = str(mx)
        wtype = "TIMESTAMP"
    elif typ == "numeric":
        value = str(mx)
        wtype = "NUMERIC"
    else:
        value = str(mx)
        wtype = "STRING"

    spark.sql(  # type: ignore[name-defined]
        f"""
        MERGE INTO {OPS}.dataset_watermark t
        USING (SELECT
          {_sql_string_literal(dataset_id)} AS dataset_id,
          {_sql_string_literal(wtype)} AS watermark_type,
          {_sql_string_literal(col)} AS watermark_column,
          {_sql_string_literal(value)} AS watermark_value,
          {_sql_string_literal(run_id)} AS last_run_id
        ) s
        ON t.dataset_id = s.dataset_id
        WHEN MATCHED THEN UPDATE SET
          t.watermark_type = s.watermark_type,
          t.watermark_column = s.watermark_column,
          t.watermark_value = s.watermark_value,
          t.last_run_id = s.last_run_id,
          t.last_updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          dataset_id, watermark_type, watermark_column, watermark_value, last_run_id, last_updated_at
        ) VALUES (
          s.dataset_id, s.watermark_type, s.watermark_column, s.watermark_value, s.last_run_id, current_timestamp()
        )
        """
    )


def claim_pending(max_items: int, target_dataset_id: str = "", job_id: str = "") -> List[Dict[str, Any]]:
    """Claims pending jobs from the queue. Supports targeted execution for a specific dataset or job.
    
    Args:
        max_items: Maximum number of jobs to claim
        target_dataset_id: If provided, only claims jobs for this specific dataset (targeted execution)
        job_id: If provided, only claims jobs for this specific scheduled job
    
    Returns:
        List of claimed job dictionaries
    """
    # Build filter clause for targeted execution
    # NOTE: Portal jobs use `correlation_id` to link queue items to a scheduled job.
    # The `job_id` column may not exist in older schemas, so we always filter by `correlation_id`.
    target_filter = ""
    if job_id:
        target_filter = f"AND correlation_id = {_sql_string_literal(job_id)}"
        print(f"[CLAIM] 💼 MODO JOB: job_id={job_id} (filtering by correlation_id)")
        print(f"[CLAIM] Claiming jobs ONLY for the specified scheduled job")
    elif target_dataset_id:
        target_filter = f"AND dataset_id = {_sql_string_literal(target_dataset_id)}"
        print(f"[CLAIM] 🎯 MODO TARGETIZADO: dataset_id={target_dataset_id}")
        print(f"[CLAIM] Claiming jobs ONLY for the specified dataset")
    else:
        # Legacy mode: process only items without a scheduled job association (backward compatibility)
        target_filter = "AND (correlation_id IS NULL OR correlation_id = '')"
        print(f"[CLAIM] 📦 MODO BATCH (Legacy): Iniciando claim de até {max_items} jobs PENDING sem job associado...")
    
    candidate_rows = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT queue_id
        FROM {OPS}.run_queue
        WHERE status = 'PENDING'
          AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp())
          {target_filter}
        ORDER BY priority ASC, requested_at ASC
        LIMIT {int(max_items)}
        """
    ).collect()

    claimed_queue_ids: List[str] = []
    for c in candidate_rows:
        qid = str(c["queue_id"])
        token = str(uuid.uuid4())
        _metric_inc("claim_attempt_count")
        _retry_delta_sql(
            f"""
            UPDATE {OPS}.run_queue
            SET
              status = 'CLAIMED',
              claim_owner = {_sql_string_literal(CLAIM_OWNER)},
              claimed_by = {_sql_string_literal(JOB_INSTANCE_ID)},
              claim_token = {_sql_string_literal(token)},
              claimed_at = current_timestamp(),
              claim_timestamp = current_timestamp(),
              attempt_number = COALESCE(attempt_number, attempt, 0),
              state_version = COALESCE(state_version, 0) + 1
            WHERE queue_id = {_sql_string_literal(qid)}
              AND status = 'PENDING'
            """,
            context="CLAIM_ROW",
            on_conflict_metric="claim_conflict_count",
        )

        owner_check = spark.sql(  # type: ignore[name-defined]
            f"""
            SELECT queue_id
            FROM {OPS}.run_queue
            WHERE queue_id = {_sql_string_literal(qid)}
              AND claim_token = {_sql_string_literal(token)}
              AND status = 'CLAIMED'
            LIMIT 1
            """
        ).collect()
        if owner_check:
            claimed_queue_ids.append(qid)
        else:
            _metric_inc("claim_lost_count")

    if claimed_queue_ids:
        claimed_in = ", ".join([_sql_string_literal(x) for x in claimed_queue_ids])
        rows = spark.sql(  # type: ignore[name-defined]
            f"""
            SELECT *
            FROM {OPS}.run_queue
            WHERE queue_id IN ({claimed_in})
            ORDER BY claimed_at ASC
            """
        ).collect()
    else:
        rows = []

    claimed_list = []
    for r in rows:
        d = r.asDict(recursive=True)
        if d.get("attempt") is None:
            d["attempt"] = int(d.get("attempt_number") or 0)
        claimed_list.append(d)
    
    # Validation: if targeted execution but nothing was claimed, raise error
    if target_dataset_id and len(claimed_list) == 0:
        error_msg = f"Dataset {target_dataset_id} não encontrado na fila PENDING ou não elegível para execução"
        print(f"[CLAIM] ❌ {error_msg}")
        raise ValueError(error_msg)
    
    print(f"[CLAIM] ✓ {len(claimed_list)} jobs foram claimed com sucesso")
    for item in claimed_list:
        print(f"[CLAIM]   - queue_id={item['queue_id'][:8]}..., dataset_id={item['dataset_id']}, attempt={item['attempt']}")
    
    return claimed_list


def run_one(item: Dict[str, Any]) -> Dict[str, Any]:
    queue_id = str(item["queue_id"])
    dataset_id = str(item["dataset_id"])
    claim_token = str(item.get("claim_token") or "")
    
    print(f"\n{'='*80}")
    print(f"[RUN] Iniciando execução - queue_id={queue_id[:8]}..., dataset_id={dataset_id}")
    print(f"[RUN] Attempt: {item.get('attempt', 0)} / {item.get('max_retries', 3)}")
    print(f"{'='*80}")

    run_id = str(uuid.uuid4())
    print(f"[RUN] run_id gerado: {run_id}")
    print(f"[RUN] Marcando queue como RUNNING e criando batch_process...")
    
    _set_run_queue_status(queue_id=queue_id, status="RUNNING", run_id=run_id, started=True, claim_token=claim_token)
    _create_batch_process(run_id=run_id, dataset_id=dataset_id, queue_id=queue_id)

    _ensure_steps_table()
    run_step = _insert_step(
        run_id=run_id,
        dataset_id=dataset_id,
        phase="ORCHESTRATOR",
        step_key="RUN_STARTED",
        status="RUNNING",
        message=f"queue_id={queue_id}",
    )

    # Load dataset_control
    ds_rows = spark.sql(  # type: ignore[name-defined]
        f"SELECT * FROM {CTRL}.dataset_control WHERE dataset_id = {_sql_string_literal(dataset_id)} LIMIT 1"
    ).collect()

    if not ds_rows:
        msg = "DATASET_NOT_FOUND"
        _update_step(step_id=run_step, status="FAILED", message=msg, finished=True, details={"sre_metrics": dict(SRE_METRICS)})
        _finish_batch_process(run_id=run_id, status="FAILED", error_class="RUNTIME_ERROR", error_message=msg)
        _set_run_queue_status(queue_id=queue_id, status="FAILED", error_class="RUNTIME_ERROR", error_message=msg, finished=True, claim_token=claim_token)
        return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "FAILED", "error": msg}

    ds = ds_rows[0].asDict(recursive=True)

    state = str(ds.get("execution_state") or "").upper()
    bronze_table = str(ds.get("bronze_table") or "")
    silver_table = str(ds.get("silver_table") or "")
    source_type = str(ds.get("source_type") or "").upper()
    connection_id = str(ds.get("connection_id") or "")
    dataset_name = str(ds.get("dataset_name") or dataset_id)

    if state in ("PAUSED", "DEPRECATED", "BLOCKED_SCHEMA_CHANGE"):
        msg = f"NOT_ELIGIBLE state={state}"
        _update_step(step_id=run_step, status="SKIPPED", message=msg, finished=True, details={"sre_metrics": dict(SRE_METRICS)})
        _finish_batch_process(run_id=run_id, status="SKIPPED", error_class="UNKNOWN", error_message=msg)
        _set_run_queue_status(queue_id=queue_id, status="CANCELLED", error_class="UNKNOWN", error_message=msg, finished=True, claim_token=claim_token)
        return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "CANCELLED", "error": msg}

    bronze_count = None
    silver_count = None
    
    # Tracking de carga incremental
    load_type = "FULL"  # Default: carga completa
    enable_incremental = bool(ds.get("enable_incremental", False))  # Default: sem incremental (evita UnboundLocalError em fontes não-Oracle)
    incremental_rows_read = None  # Apenas linhas incrementais
    watermark_start = None  # Início do range de watermark
    watermark_end = None  # Fim do range de watermark
    is_short_circuited = False  # Flag: carga pulada por ausência de dados novos (otimização incremental)

    try:
        # Bronze
        print(f"[RUN:BRONZE] Iniciando carga Bronze...")
        print(f"[RUN:BRONZE] source_type={source_type}, dataset_name={dataset_name}")
        print(f"[RUN:BRONZE] bronze_table={bronze_table}")
        
        bronze_step = _insert_step(
            run_id=run_id,
            dataset_id=dataset_id,
            phase="BRONZE",
            step_key="BRONZE_LOAD",
            status="RUNNING",
            message=f"source={dataset_name}",
            details={"source_type": source_type, "connection_id": connection_id, "bronze_table": bronze_table},
        )

        # ================================================================
        # BRONZE SOURCE DISPATCH — CONTRATO OBRIGATÓRIO PARA NOVAS FONTES
        # ================================================================
        # Ao adicionar uma nova fonte (ex: MYSQL, SQLSERVER, API, S3, etc.),
        # o bloco DEVE implementar:
        #   1. Leitura de enable_incremental e incremental_metadata (wm_col, lookback_days, pk)
        #   2. Consulta a {OPS}.dataset_watermark para obter o último watermark
        #   3. Filtro na origem quando incremental (WHERE wm_col >= cutoff)
        #   4. Escrita no Bronze com o modo correto:
        #      - CURRENT + PK  → MERGE (Delta MERGE por PK)
        #      - APPEND_LOG    → APPEND
        #      - SNAPSHOT / sem PK → OVERWRITE
        #   5. Colunas técnicas: _op (INCREMENTAL|FULL_REFRESH), _watermark_col,
        #      _watermark_value (valor por linha), _batch_id, _ingestion_ts
        #   6. Atualização das variáveis de tracking:
        #      load_type, incremental_rows_read, watermark_start, watermark_end
        # Sem isso, enable_incremental=True no portal não terá efeito real.
        # Referência: bloco ORACLE e bloco SUPABASE abaixo.
        # ================================================================
        if source_type == "ORACLE":
            try:
                # ===========================
                # INCREMENTAL LOADING LOGIC
                # ===========================
                # Check if incremental loading is enabled for this dataset
                enable_incremental = ds.get("enable_incremental", False)
                incremental_strategy = ds.get("incremental_strategy", "SNAPSHOT")
                discovery_status = ds.get("discovery_status", "")
                
                b = None  # Result from load function
                operation_type = "OVERWRITE"  # Default
                
                # Call incremental function if: (1) enabled OR (2) discovery pending
                if enable_incremental or discovery_status == "PENDING":
                    print(f"[RUN:BRONZE] 🔄 INCREMENTAL MODE ENABLED")
                    print(f"[RUN:BRONZE] Strategy: {incremental_strategy}")
                    
                    try:
                        # Call incremental loading function
                        b = _load_oracle_bronze_incremental(  # type: ignore[name-defined]
                            dataset_id=dataset_id,
                            dataset_name=dataset_name,
                            connection_id=connection_id,
                            bronze_table=bronze_table,
                            run_id=run_id,
                            catalog=CATALOG,
                        )
                        
                        # If function returns None, fallback to full refresh
                        if b is None:
                            print(f"[RUN:BRONZE] ⚠️ Incremental returned None, using fallback (full refresh)...")
                            b = _load_oracle_bronze(
                                dataset_id=dataset_id,
                                dataset_name=dataset_name,
                                connection_id=connection_id,
                                bronze_table=bronze_table,
                                run_id=run_id,
                            )
                        else:
                            # Incremental succeeded
                            print(f"[RUN:BRONZE] ✓ Incremental load completed!")
                            # Determine operation type from result
                            bronze_mode = ds.get("bronze_mode", "SNAPSHOT")
                            if bronze_mode == "CURRENT":
                                operation_type = "MERGE"
                            elif bronze_mode == "APPEND_LOG":
                                operation_type = "APPEND"
                            else:
                                operation_type = "OVERWRITE"
                    
                    except NameError:
                        # Incremental function not available (import failed)
                        print(f"[RUN:BRONZE] ⚠️ Incremental functions not available, using fallback...")
                        b = _load_oracle_bronze(
                            dataset_id=dataset_id,
                            dataset_name=dataset_name,
                            connection_id=connection_id,
                            bronze_table=bronze_table,
                            run_id=run_id,
                        )
                    except Exception as incr_error:
                        # Incremental failed, log and fallback
                        _incr_tb = traceback.format_exc()
                        print(f"[RUN:BRONZE] ❌ Incremental load failed: {incr_error}")
                        print(f"[RUN:BRONZE] ❌ Traceback: {_incr_tb}")
                        print(f"[RUN:BRONZE] ⚠️ Falling back to full refresh...")
                        # Salvar erro para diagnóstico via SQL
                        try:
                            spark.sql(f"""  -- type: ignore
                                UPDATE {OPS}.batch_process
                                SET error_message = {_sql_string_literal(f'INCREMENTAL_FALLBACK: {str(incr_error)[:500]}')}
                                WHERE run_id = {_sql_string_literal(run_id)}
                            """)
                        except Exception:
                            pass
                        b = _load_oracle_bronze(
                            dataset_id=dataset_id,
                            dataset_name=dataset_name,
                            connection_id=connection_id,
                            bronze_table=bronze_table,
                            run_id=run_id,
                        )
                else:
                    # Incremental disabled, use standard full refresh
                    print(f"[RUN:BRONZE] 📦 FULL REFRESH MODE (incremental disabled)")
                    b = _load_oracle_bronze(
                        dataset_id=dataset_id,
                        dataset_name=dataset_name,
                        connection_id=connection_id,
                        bronze_table=bronze_table,
                        run_id=run_id,
                    )
                
                # Process result (same for both incremental and full refresh)
                bronze_count = int(b.get("bronze_row_count") or 0)
                src_est = b.get("source_estimate")
                
                # ===========================
                # CAPTURAR INFORMAÇÕES INCREMENTAIS
                # ===========================
                # Determinar tipo de carga baseado no resultado
                if b.get("incremental") and enable_incremental:
                    load_type = "INCREMENTAL"
                    incremental_rows_read = bronze_count  # Total de linhas lidas na carga incremental
                    
                    # Extrair watermark diretamente do resultado da função incremental
                    if b.get("watermark_start"):
                        watermark_start = str(b["watermark_start"])
                    if b.get("watermark_end"):
                        watermark_end = str(b["watermark_end"])
                    
                    if watermark_start or watermark_end:
                        print(f"[RUN:BRONZE] 📊 Watermark range: {watermark_start} → {watermark_end}")
                    else:
                        # Fallback: tentar extrair da Bronze table (compat com fontes que não retornam no dict)
                        metadata_json = ds.get("incremental_metadata")
                        if metadata_json:
                            try:
                                metadata = json.loads(metadata_json) if isinstance(metadata_json, str) else metadata_json
                                watermark_col = metadata.get("watermark_column") or metadata.get("watermark_col")
                                
                                if watermark_col and _table_exists(bronze_table):
                                    try:
                                        wm_stats = spark.sql(f"""  # type: ignore[name-defined]
                                            SELECT 
                                                MIN(_watermark_value) as wm_start,
                                                MAX(_watermark_value) as wm_end
                                            FROM {bronze_table}
                                            WHERE _batch_id = {_sql_string_literal(run_id)}
                                        """).collect()
                                        
                                        if wm_stats and wm_stats[0].wm_start:
                                            watermark_start = str(wm_stats[0].wm_start)
                                            watermark_end = str(wm_stats[0].wm_end)
                                            print(f"[RUN:BRONZE] 📊 Watermark range (fallback): {watermark_start} → {watermark_end}")
                                    except Exception as wm_error:
                                        print(f"[RUN:BRONZE] ⚠️ Não foi possível capturar watermark: {wm_error}")
                            except Exception as json_error:
                                print(f"[RUN:BRONZE] ⚠️ Erro ao parsear metadata: {json_error}")
                elif incremental_strategy == "SNAPSHOT":
                    load_type = "SNAPSHOT"
                else:
                    load_type = "FULL"
                
                # Detectar short-circuit (sem dados novos na origem)
                is_short_circuited = bool(b.get("short_circuited", False))
                
                if is_short_circuited:
                    print(f"[RUN:BRONZE] ⚡ SHORT-CIRCUIT: Nenhum dado novo na origem Oracle")
                    print(f"[RUN:BRONZE] ⚡ Bronze e Silver permanecem inalterados")
                    load_type = "INCREMENTAL"
                    incremental_rows_read = 0
                    if b.get("watermark_end"):
                        watermark_end = str(b["watermark_end"])
                    if b.get("watermark_start"):
                        watermark_start = str(b["watermark_start"])
                else:
                    print(f"[RUN:BRONZE] ✓ Carga concluída com sucesso!")
                    print(f"[RUN:BRONZE] Tipo de carga: {load_type}")
                    print(f"[RUN:BRONZE] Registros carregados: {bronze_count:,}")
                    if load_type == "INCREMENTAL" and incremental_rows_read:
                        print(f"[RUN:BRONZE] Linhas incrementais: {incremental_rows_read:,}")
                    if src_est is not None:
                        print(f"[RUN:BRONZE] Estimativa da origem: {src_est:,}")
                
                # Check if OPTIMIZE was executed (incremental only)
                if b.get("optimize_executed"):
                    print(f"[RUN:BRONZE] ✨ OPTIMIZE ZORDER executado durante esta carga")
                
                _update_step(step_id=bronze_step, progress_total=src_est if src_est is not None else None)
                _write_table_details(
                    run_id=run_id,
                    dataset_id=dataset_id,
                    layer="BRONZE",
                    table_name=bronze_table,
                    operation="SHORT_CIRCUIT" if is_short_circuited else operation_type,
                    status="SUCCEEDED",
                    row_count=bronze_count,
                )
                _sc_msg = "bronze short-circuited (no new data)" if is_short_circuited else ("bronze loaded" + (" (incremental)" if enable_incremental else ""))
                _update_step(
                    step_id=bronze_step,
                    status="SUCCEEDED",
                    progress_current=bronze_count,
                    finished=True,
                    message=_sc_msg,
                    details=b,
                )
            except Exception as e:
                _update_step(step_id=bronze_step, status="FAILED", finished=True, message=str(e))
                raise
        elif source_type == "SUPABASE":
            # ================================================================
            # SUPABASE INGESTION — com suporte completo a carga incremental
            # ================================================================
            # CONTRATO PARA NOVAS FONTES DE DADOS:
            # Toda nova fonte (projeto/área/conexão) DEVE implementar:
            #   1. Leitura de enable_incremental / incremental_metadata
            #   2. Consulta ao dataset_watermark para obter último valor
            #   3. Filtro na origem (WHERE wm_col >= cutoff) quando incremental
            #   4. Escrita no bronze com modo correto (MERGE/APPEND/OVERWRITE)
            #   5. Colunas técnicas: _op, _watermark_col, _watermark_value
            #   6. Tracking: load_type, incremental_rows_read, watermark_start/end
            # Sem isso, enable_incremental=True no portal não terá efeito.
            # Veja o bloco ORACLE acima como referência completa.
            # ================================================================
            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ========== Iniciando carga Supabase ==========")
            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] dataset_name={dataset_name}")
            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] bronze_table={bronze_table}")
            
            try:
                # --- 1. Parse table name ---
                supabase_schema = "public"
                supabase_table = dataset_name
                if "_" in dataset_name:
                    first_part = dataset_name.split("_", 1)[0]
                    if first_part.lower() in ("public", "auth", "storage", "extensions"):
                        supabase_schema = first_part
                        supabase_table = dataset_name.split("_", 1)[1]
                
                source_table_fqn = f"{supabase_schema}.{supabase_table}"  # PostgreSQL fully-qualified name
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] Supabase table={source_table_fqn}")
                
                # --- 2. JDBC connection (widgets → spark conf → secrets) ---
                supabase_jdbc_url = _get_widget("supabase_jdbc_url", "")
                supabase_user = _get_widget("supabase_user", "")
                supabase_password = _get_widget("supabase_password", "")
                
                if not supabase_jdbc_url:
                    try:
                        supabase_jdbc_url = spark.conf.get("spark.supabase.jdbc_url", "")  # type: ignore[name-defined]
                    except Exception:
                        pass
                if not supabase_user:
                    try:
                        supabase_user = spark.conf.get("spark.supabase.user", "")  # type: ignore[name-defined]
                    except Exception:
                        pass
                if not supabase_password:
                    try:
                        supabase_password = spark.conf.get("spark.supabase.password", "")  # type: ignore[name-defined]
                    except Exception:
                        pass
                
                if not supabase_jdbc_url:
                    try:
                        supabase_host = dbutils.secrets.get("supabase", "host")  # type: ignore[name-defined]
                        supabase_port = dbutils.secrets.get("supabase", "port")  # type: ignore[name-defined]
                        supabase_db = dbutils.secrets.get("supabase", "database")  # type: ignore[name-defined]
                        supabase_jdbc_url = f"jdbc:postgresql://{supabase_host}:{supabase_port}/{supabase_db}?sslmode=require"
                        supabase_user = dbutils.secrets.get("supabase", "user")  # type: ignore[name-defined]
                        supabase_password = dbutils.secrets.get("supabase", "password")  # type: ignore[name-defined]
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ Credenciais recuperadas do Databricks Secrets")
                    except Exception as sec_err:
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ Secrets não encontrados: {sec_err}")
                
                if not supabase_jdbc_url:
                    raise SourceError("Supabase JDBC URL not configured. Set supabase_jdbc_url widget or create secrets scope 'supabase'.")
                
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] JDBC URL configurado (length={len(supabase_jdbc_url)})")
                
                # --- 3. Incremental configuration ---
                bronze_mode = ds.get("bronze_mode", "SNAPSHOT")
                operation_type = "OVERWRITE"
                is_incremental_run = False
                
                wm_col = None
                lookback_days = 3
                pk_cols: List[str] = []
                
                metadata_json = ds.get("incremental_metadata")
                _meta_debug = {"raw_type": type(metadata_json).__name__, "raw_repr": repr(metadata_json)[:500] if metadata_json else None}
                if metadata_json:
                    try:
                        _meta = json.loads(metadata_json) if isinstance(metadata_json, str) else metadata_json
                        _meta_debug["parsed_type"] = type(_meta).__name__
                        _meta_debug["parsed_keys"] = list(_meta.keys()) if isinstance(_meta, dict) else "NOT_A_DICT"
                        _meta_debug["wm_column_raw"] = repr(_meta.get("watermark_column"))
                        _meta_debug["wm_col_raw"] = repr(_meta.get("watermark_col"))
                        wm_col = _meta.get("watermark_column") or _meta.get("watermark_col")
                        lookback_days = int(_meta.get("lookback_days", 3))
                        pk_cols = _meta.get("pk", []) or []
                        _meta_debug["parse_ok"] = True
                    except Exception as _meta_err:
                        _meta_debug["parse_error"] = str(_meta_err)
                        import traceback as _tb_meta
                        _meta_debug["parse_traceback"] = _tb_meta.format_exc()[:500]
                
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] enable_incremental={enable_incremental} (type={type(enable_incremental).__name__}), bronze_mode={bronze_mode}")
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] wm_col={wm_col}, lookback_days={lookback_days}, pk_cols={pk_cols}")
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] metadata_json raw={repr(metadata_json)}")
                
                # --- 4. Get last watermark value ---
                last_wm_value = None
                if enable_incremental and wm_col:
                    _wm_sql = f"SELECT watermark_value FROM {OPS}.dataset_watermark WHERE dataset_id = {_sql_string_literal(dataset_id)} ORDER BY last_updated_at DESC LIMIT 1"
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Watermark SQL: {_wm_sql}")
                    try:
                        _wm_rows = spark.sql(_wm_sql).collect()  # type: ignore[name-defined]
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Watermark rows count={len(_wm_rows)}")
                        if _wm_rows:
                            _raw_wm = _wm_rows[0].watermark_value
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Raw watermark_value={repr(_raw_wm)} type={type(_raw_wm).__name__} truthy={bool(_raw_wm)}")
                        if _wm_rows and _wm_rows[0].watermark_value:
                            last_wm_value = str(_wm_rows[0].watermark_value)
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ Último watermark: {last_wm_value}")
                        else:
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Watermark not set: rows={bool(_wm_rows)}, value_truthy={bool(_wm_rows[0].watermark_value) if _wm_rows else 'N/A'}")
                    except Exception as _wm_err:
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ Sem watermark anterior: {_wm_err}")
                        import traceback as _tb
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Watermark traceback: {_tb.format_exc()}")
                else:
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Skipped watermark read: enable_incremental={enable_incremental}, wm_col={wm_col}")
                
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] [DEBUG] Decision vars: enable_incremental={enable_incremental}, wm_col={repr(wm_col)}, last_wm_value={repr(last_wm_value)}, bronze_mode={repr(bronze_mode)}, pk_cols={pk_cols}")
                
                # --- 5. Build JDBC query ---
                dbtable = source_table_fqn
                
                if enable_incremental and wm_col and last_wm_value:
                    # ---- INCREMENTAL: filter source by watermark ----
                    # Decide if we CAN do incremental based on bronze_mode
                    if bronze_mode == "CURRENT" and pk_cols:
                        # MERGE by PK — read only delta from source
                        cutoff_sql = f"'{last_wm_value}'::timestamp - interval '{lookback_days} days'"
                        dbtable = f'(SELECT * FROM {source_table_fqn} WHERE "{wm_col}" >= {cutoff_sql}) AS incr_q'
                        operation_type = "MERGE"
                        is_incremental_run = True
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 🔄 INCREMENTAL MERGE (CURRENT + PK)")
                    elif bronze_mode == "APPEND_LOG":
                        # APPEND — read only delta, append to bronze
                        cutoff_sql = f"'{last_wm_value}'::timestamp - interval '{lookback_days} days'"
                        dbtable = f'(SELECT * FROM {source_table_fqn} WHERE "{wm_col}" >= {cutoff_sql}) AS incr_q'
                        operation_type = "APPEND"
                        is_incremental_run = True
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 🔄 INCREMENTAL APPEND (APPEND_LOG)")
                    else:
                        # SNAPSHOT or CURRENT without PK — cannot merge, full read required
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 📦 FULL REFRESH (bronze_mode={bronze_mode}, pk_cols={pk_cols})")
                elif enable_incremental and wm_col and not last_wm_value:
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 📦 PRIMEIRA EXECUÇÃO: carga completa para estabelecer watermark baseline")
                else:
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 📦 FULL REFRESH (incremental desabilitado ou sem watermark configurado)")
                
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] dbtable={dbtable}")
                
                # --- 6. Read from Supabase via JDBC ---
                df = (
                    spark.read.format("jdbc")  # type: ignore[name-defined]
                    .option("url", supabase_jdbc_url)
                    .option("dbtable", dbtable)
                    .option("user", supabase_user)
                    .option("password", supabase_password)
                    .option("driver", "org.postgresql.Driver")
                    .option("fetchsize", "10000")
                    .load()
                )
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ DataFrame criado com sucesso")
                
                # --- 6b. Auto-detect PKs if not in metadata (CURRENT mode) ---
                # Must happen AFTER DataFrame creation so we can use df.schema as fallback.
                # Strategy: 1) Query PostgreSQL information_schema  2) Fallback: look for 'id' column
                if enable_incremental and bronze_mode == "CURRENT" and not pk_cols:
                    # Strategy 1: Query PostgreSQL information_schema for real PKs
                    try:
                        _pk_query = f"""(SELECT kcu.column_name
                            FROM information_schema.key_column_usage kcu
                            JOIN information_schema.table_constraints tc
                              ON kcu.constraint_name = tc.constraint_name
                              AND kcu.table_schema = tc.table_schema
                            WHERE tc.constraint_type = 'PRIMARY KEY'
                              AND tc.table_schema = '{supabase_schema}'
                              AND tc.table_name = '{supabase_table}'
                            ORDER BY kcu.ordinal_position) AS pk_q"""
                        _pk_df = (
                            spark.read.format("jdbc")  # type: ignore[name-defined]
                            .option("url", supabase_jdbc_url)
                            .option("dbtable", _pk_query)
                            .option("user", supabase_user)
                            .option("password", supabase_password)
                            .option("driver", "org.postgresql.Driver")
                            .load()
                        )
                        _detected_pks = [row.column_name for row in _pk_df.collect()]
                        if _detected_pks:
                            pk_cols = _detected_pks
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ PKs auto-detectadas (information_schema): {pk_cols}")
                    except Exception as _pk_err:
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ information_schema falhou: {_pk_err}")
                    
                    # Strategy 2: Fallback — look for 'id' column in DataFrame schema
                    if not pk_cols:
                        _df_col_names = [f.name for f in df.schema.fields if not f.name.startswith("_")]
                        if "id" in _df_col_names:
                            pk_cols = ["id"]
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ PK fallback: coluna 'id' detectada no DataFrame")
                        else:
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ Sem PK detectada — MERGE não será possível")
                    
                    # Persist detected PKs for future runs
                    if pk_cols:
                        try:
                            _updated_meta = json.loads(metadata_json) if isinstance(metadata_json, str) else (metadata_json or {})
                            _updated_meta["pk"] = pk_cols
                            _updated_meta_str = json.dumps(_updated_meta, ensure_ascii=False)
                            spark.sql(  # type: ignore[name-defined]
                                f"UPDATE {CTRL}.dataset_control SET incremental_metadata = {_sql_string_literal(_updated_meta_str)}, updated_at = current_timestamp(), updated_by = 'orchestrator_pk_detect' WHERE dataset_id = {_sql_string_literal(dataset_id)}"
                            )
                            ds["incremental_metadata"] = _updated_meta_str
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ PKs persistidas no incremental_metadata: {pk_cols}")
                        except Exception as _pk_persist_err:
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ Erro ao persistir PKs: {_pk_persist_err}")
                    
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] pk_cols (após auto-detect)={pk_cols}")
                    
                    # Re-evaluate incremental mode now that PKs are available
                    if pk_cols and last_wm_value and _table_exists(bronze_table):
                        # Filter the already-loaded DataFrame by watermark (avoid re-reading JDBC)
                        _cutoff_ts = f"{last_wm_value}"
                        from datetime import datetime, timedelta
                        try:
                            _wm_dt = datetime.fromisoformat(_cutoff_ts.replace("Z", "+00:00")) if "T" in _cutoff_ts else datetime.strptime(_cutoff_ts, "%Y-%m-%d %H:%M:%S.%f")
                        except Exception:
                            _wm_dt = datetime.strptime(_cutoff_ts[:19], "%Y-%m-%d %H:%M:%S")
                        _cutoff_dt = _wm_dt - timedelta(days=lookback_days)
                        df = df.filter(F.col(wm_col) >= F.lit(_cutoff_dt))
                        operation_type = "MERGE"
                        is_incremental_run = True
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 🔄 INCREMENTAL MERGE ativado (PKs auto-detectadas, filtro in-memory)")
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] Cutoff: {wm_col} >= {_cutoff_dt}")
                
                # --- 7. Add technical metadata columns ---
                df = df.withColumn("_ingestion_ts", F.current_timestamp())
                df = df.withColumn("_batch_id", F.lit(run_id))
                df = df.withColumn("_source_table", F.lit(source_table_fqn))
                df = df.withColumn("_op", F.lit("INCREMENTAL" if is_incremental_run else "FULL_REFRESH"))
                df = df.withColumn("_watermark_col", F.lit(wm_col).cast("string"))
                if wm_col and wm_col in [f.name for f in df.schema.fields]:
                    df = df.withColumn("_watermark_value", F.col(wm_col).cast("string"))
                else:
                    df = df.withColumn("_watermark_value", F.lit(None).cast("string"))
                df = df.withColumn("_row_hash", F.lit(None).cast("string"))
                df = df.withColumn("_is_deleted", F.lit(False))
                
                # --- 8. Write to Bronze ---
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] Escrevendo na tabela Delta: '{bronze_table}' (mode={operation_type})")
                
                # Track merge metrics
                _merge_inserted = None
                _merge_updated = None
                
                if is_incremental_run and operation_type == "MERGE" and pk_cols and _table_exists(bronze_table):
                    # MERGE into existing bronze table by PK
                    from delta.tables import DeltaTable
                    dt = DeltaTable.forName(spark, bronze_table)  # type: ignore[name-defined]
                    merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])
                    (
                        dt.alias("t")
                        .merge(df.alias("s"), merge_cond)
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    # Capture MERGE metrics from Delta HISTORY
                    try:
                        _hist = spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 1").collect()  # type: ignore[name-defined]
                        if _hist:
                            _metrics = _hist[0].operationMetrics
                            if _metrics:
                                _merge_updated = int(_metrics.get("numTargetRowsUpdated", 0))
                                _merge_inserted = int(_metrics.get("numTargetRowsInserted", 0))
                                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 📊 MERGE metrics: {_merge_inserted} inserted, {_merge_updated} updated")
                    except Exception as _hist_err:
                        print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ Erro ao capturar MERGE metrics: {_hist_err}")
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ MERGE executado (PKs: {pk_cols})")
                elif is_incremental_run and operation_type == "APPEND":
                    # APPEND to bronze (APPEND_LOG mode)
                    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(bronze_table)
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ APPEND executado")
                else:
                    # OVERWRITE (first run, full refresh, or fallback)
                    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ OVERWRITE executado")
                
                # --- 9. Track results ---
                bronze_count = int(spark.table(bronze_table).count())  # type: ignore[name-defined]
                
                if is_incremental_run:
                    load_type = "INCREMENTAL"
                    # Count only rows from this batch (affected by MERGE or APPEND)
                    try:
                        _batch_cnt = spark.sql(  # type: ignore[name-defined]
                            f"SELECT COUNT(*) AS cnt FROM {bronze_table} WHERE _batch_id = {_sql_string_literal(run_id)}"
                        ).collect()[0].cnt
                        incremental_rows_read = int(_batch_cnt)
                    except Exception:
                        incremental_rows_read = bronze_count
                    
                    # Capture watermark range for this run
                    if wm_col:
                        try:
                            _wm_stats = spark.sql(  # type: ignore[name-defined]
                                f"SELECT MIN(_watermark_value) AS wm_start, MAX(_watermark_value) AS wm_end FROM {bronze_table} WHERE _batch_id = {_sql_string_literal(run_id)}"
                            ).collect()
                            if _wm_stats and _wm_stats[0].wm_start:
                                watermark_start = str(_wm_stats[0].wm_start)
                                watermark_end = str(_wm_stats[0].wm_end)
                                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] 📊 Watermark range: {watermark_start} → {watermark_end}")
                        except Exception as _wms_err:
                            print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ⚠ Erro ao capturar watermark range: {_wms_err}")
                
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ✓ Carga concluída: {bronze_count:,} linhas (load_type={load_type})")
                if is_incremental_run and incremental_rows_read is not None:
                    print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] Linhas incrementais neste batch: {incremental_rows_read:,}")
                print(f"[{now_utc_iso()}] [BRONZE:SUPABASE] ========== Carga Supabase finalizada ==========\n")
                
                _write_table_details(
                    run_id=run_id,
                    dataset_id=dataset_id,
                    layer="BRONZE",
                    table_name=bronze_table,
                    operation=operation_type,
                    status="SUCCEEDED",
                    row_count=bronze_count,
                    inserted_count=_merge_inserted,
                    updated_count=_merge_updated,
                )
                # Store debug info in step details for diagnostic queries
                _debug_details = {
                    "source_type": source_type,
                    "connection_id": connection_id,
                    "bronze_table": bronze_table,
                    "metadata_debug": _meta_debug,
                    "incremental_debug": {
                        "enable_incremental": enable_incremental,
                        "enable_incremental_type": type(enable_incremental).__name__,
                        "bronze_mode": bronze_mode,
                        "wm_col": wm_col,
                        "pk_cols": pk_cols,
                        "last_wm_value": last_wm_value,
                        "is_incremental_run": is_incremental_run,
                        "operation_type": operation_type,
                        "load_type": load_type,
                        "dbtable": dbtable[:200] if dbtable else None,
                    }
                }
                _update_step(
                    step_id=bronze_step,
                    status="SUCCEEDED",
                    progress_current=bronze_count,
                    finished=True,
                    message=f"supabase bronze loaded ({bronze_count} rows" + (", incremental" if is_incremental_run else "") + ")",
                    details=_debug_details,
                )
            except Exception as e:
                _update_step(step_id=bronze_step, status="FAILED", finished=True, message=str(e))
                raise SourceError(f"SUPABASE_LOAD_FAILED: {e}")
        else:
            # Other sources not implemented
            if not _table_exists(bronze_table):
                raise SourceError(f"BRONZE_TABLE_NOT_FOUND: {bronze_table}")
            bronze_count = int(spark.table(bronze_table).count())  # type: ignore[name-defined]
            _write_table_details(
                run_id=run_id,
                dataset_id=dataset_id,
                layer="BRONZE",
                table_name=bronze_table,
                operation="APPEND",
                status="SUCCEEDED",
                row_count=bronze_count,
            )
            _update_step(step_id=bronze_step, status="SUCCEEDED", progress_current=bronze_count, finished=True, message="bronze counted")

        # -------------------------------------------------------
        # SHORT-CIRCUIT: Pular Silver/Watermark quando não há dados novos
        # Economiza tempo significativo evitando MERGE redundante
        # -------------------------------------------------------
        if is_short_circuited:
            print(f"[RUN:SILVER] ⚡ SHORT-CIRCUIT: Pulando promoção Silver (sem dados novos)")
            silver_count = bronze_count  # Manter contagem consistente (0 ou existente)
            
            # Contar registros atuais na Silver para exibição correta
            if _table_exists(silver_table):
                try:
                    silver_count = int(spark.table(silver_table).count())  # type: ignore[name-defined]
                except Exception:
                    pass
            
            silver_step = _insert_step(
                run_id=run_id,
                dataset_id=dataset_id,
                phase="SILVER",
                step_key="SILVER_PROMOTE",
                status="RUNNING",
                details={"silver_table": silver_table},
            )
            _write_table_details(
                run_id=run_id,
                dataset_id=dataset_id,
                layer="SILVER",
                table_name=silver_table,
                operation="SHORT_CIRCUIT",
                status="SUCCEEDED",
                row_count=silver_count,
            )
            _update_step(
                step_id=silver_step,
                status="SUCCEEDED",
                progress_current=silver_count,
                finished=True,
                message="silver short-circuited (no new data)",
            )
            
            # Watermark: não atualizar (não há dados novos)
            wm_step = _insert_step(run_id=run_id, dataset_id=dataset_id, phase="ORCHESTRATOR", step_key="WATERMARK", status="RUNNING")
            _update_step(step_id=wm_step, status="SUCCEEDED", finished=True, message="skipped (short-circuit)")
        else:
            # Silver (execução normal)
            print(f"[RUN:SILVER] Iniciando promoção para Silver...")
            print(f"[RUN:SILVER] silver_table={silver_table}")
            
            silver_step = _insert_step(
                run_id=run_id,
                dataset_id=dataset_id,
                phase="SILVER",
                step_key="SILVER_PROMOTE",
                status="RUNNING",
                details={"silver_table": silver_table},
            )

            print(f"[RUN:SILVER] Buscando schema ACTIVE para dataset_id={dataset_id}...")
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
            
            # Sync schema with incremental_metadata if divergent
            # (resolves: schema created before incremental was enabled)
            schema = _sync_schema_with_incremental(dataset_id, schema, ds)
            
            print(f"[RUN:SILVER] Aplicando transformações (cast + dedupe + merge)...")

            # -------------------------------------------------------
            # SILVER INCREMENTAL: Filtrar Bronze pela janela de watermark
            # Quando SEM PK + watermark: lê apenas o range do lookback
            # Quando COM PK: lê tudo (MERGE por PK resolve dedupe)
            # -------------------------------------------------------
            bronze_df = spark.table(bronze_table)  # type: ignore[name-defined]
            silver_replace_condition = None
            silver_write_mode = "auto"
            silver_operation = "MERGE" if schema.get("primary_key") else "OVERWRITE"
            
            ds_bronze_mode = ds.get("bronze_mode", "SNAPSHOT")
            
            if enable_incremental and load_type == "INCREMENTAL" and not schema.get("primary_key"):
                # SEM PK incremental: tentar usar replaceWhere na Silver
                try:
                    _meta_json = ds.get("incremental_metadata")
                    _meta = json.loads(_meta_json) if isinstance(_meta_json, str) else (_meta_json or {})
                    _wm_col = _meta.get("watermark_column") or _meta.get("watermark_col")
                    _pk = _meta.get("pk", [])
                    _lb_days = int(_meta.get("lookback_days", 3))
                    
                    if _wm_col and not _pk:
                        # Verificar se a coluna de watermark existe no schema Silver
                        silver_cols = [c["name"] for c in (schema.get("columns") or [])]
                        if _wm_col in silver_cols:
                            silver_replace_condition = f"{_wm_col} >= CURRENT_TIMESTAMP() - INTERVAL {_lb_days} DAYS"
                            silver_write_mode = "REPLACE_WHERE"
                            silver_operation = "REPLACE_WHERE"
                            
                            # Filtrar Bronze: ler apenas a janela do lookback (não toda a tabela)
                            bronze_df = bronze_df.filter(F.col(_wm_col) >= F.expr(f"CURRENT_TIMESTAMP() - INTERVAL {_lb_days} DAYS"))
                            print(f"[RUN:SILVER] ✨ SEM PK incremental: lendo Bronze filtrada por {_wm_col} (lookback {_lb_days} dias)")
                            print(f"[RUN:SILVER] replaceWhere condition: {silver_replace_condition}")
                        else:
                            print(f"[RUN:SILVER] ⚠️ Coluna {_wm_col} não encontrada no schema Silver → OVERWRITE")
                except Exception as meta_err:
                    print(f"[RUN:SILVER] ⚠️ Erro ao parsear metadata para Silver incremental: {meta_err}")
            
            # Fallback: APPEND_LOG ou OVERWRITE
            if silver_write_mode == "auto":
                if ds_bronze_mode == "APPEND_LOG":
                    silver_write_mode = "APPEND"
                    silver_operation = "APPEND"
            
            print(f"[RUN:SILVER] Aplicando cast plan...")
            casted = _apply_cast_plan(bronze_df, schema)
            print(f"[RUN:SILVER] Aplicando deduplicação LWW...")
            deduped = _dedupe_lww(casted, schema)
            
            print(f"[RUN:SILVER] Executando escrita Silver (write_mode={silver_write_mode})...")
            silver_count = _merge_to_silver(deduped, silver_table, schema, write_mode=silver_write_mode, replace_condition=silver_replace_condition)
            print(f"[RUN:SILVER] ✓ Silver concluído! Registros processados: {silver_count:,}")

            _write_table_details(
                run_id=run_id,
                dataset_id=dataset_id,
                layer="SILVER",
                table_name=silver_table,
                operation=silver_operation,
                status="SUCCEEDED",
                row_count=silver_count,
            )
            _update_step(step_id=silver_step, status="SUCCEEDED", progress_current=silver_count, finished=True, message="silver promoted")

            # Watermark
            wm_step = _insert_step(run_id=run_id, dataset_id=dataset_id, phase="ORCHESTRATOR", step_key="WATERMARK", status="RUNNING")
            _update_watermark(dataset_id, schema, casted, run_id)
            _update_step(step_id=wm_step, status="SUCCEEDED", finished=True)

        # finalize
        print(f"[RUN] ✓ EXECUÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"[RUN] Bronze: {bronze_count:,} registros, Silver: {silver_count:,} registros")
        print(f"[RUN] Finalizando run_id={run_id}...")
        
        _update_step(step_id=run_step, status="SUCCEEDED", finished=True, message="run finished", details={"sre_metrics": dict(SRE_METRICS)})
        _finish_batch_process(
            run_id=run_id, 
            status="SUCCEEDED", 
            bronze_row_count=bronze_count, 
            silver_row_count=silver_count,
            load_type=load_type,
            incremental_rows_read=incremental_rows_read,
            watermark_start=watermark_start,
            watermark_end=watermark_end
        )
        _set_run_queue_status(queue_id=queue_id, status="SUCCEEDED", finished=True, claim_token=claim_token)

        spark.sql(  # type: ignore[name-defined]
            f"UPDATE {CTRL}.dataset_control SET last_success_run_id = {_sql_string_literal(run_id)}, updated_at = current_timestamp(), updated_by = {_sql_string_literal('orchestrator')} WHERE dataset_id = {_sql_string_literal(dataset_id)}"
        )
        
        print(f"{'='*80}\n")
        return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "SUCCEEDED"}

    except SchemaError as e:
        msg = str(e)
        st = traceback.format_exc(limit=50)

        # bronze may have succeeded; record silver as skipped/failed
        _write_table_details(
            run_id=run_id,
            dataset_id=dataset_id,
            layer="SILVER",
            table_name=silver_table,
            operation="MERGE",
            status="SKIPPED",
            error_message=msg,
        )

        _update_step(step_id=run_step, status="FAILED", finished=True, message=msg, details={"sre_metrics": dict(SRE_METRICS)})
        _finish_batch_process(
            run_id=run_id,
            status="FAILED",
            bronze_row_count=bronze_count,
            silver_row_count=None,
            error_class="SCHEMA_ERROR",
            error_message=msg,
            error_stacktrace=st,
            load_type=load_type,
            incremental_rows_read=incremental_rows_read,
        )
        _set_run_queue_status(queue_id=queue_id, status="FAILED", error_class="SCHEMA_ERROR", error_message=msg, finished=True, claim_token=claim_token)

        return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "FAILED", "error": msg}

    except SourceError as e:
        msg = str(e)
        st = traceback.format_exc(limit=50)

        _update_step(step_id=run_step, status="FAILED", finished=True, message=msg, details={"sre_metrics": dict(SRE_METRICS)})
        _finish_batch_process(
            run_id=run_id,
            status="FAILED",
            bronze_row_count=bronze_count,
            error_class="SOURCE_ERROR",
            error_message=msg,
            error_stacktrace=st,
            load_type=load_type,
        )

        # Retry logic
        attempt = int(item.get("attempt") or 0)
        max_retries = int(item.get("max_retries") or 0)
        next_attempt = attempt + 1
        if next_attempt <= max_retries:
            backoff_minutes = min(60, 2 ** attempt * 5)
            _set_run_queue_status(
                queue_id=queue_id,
                status="PENDING",
                error_class="SOURCE_ERROR",
                error_message=msg,
                attempt_inc=True,
                next_retry_at_minutes=backoff_minutes,
                claim_token=claim_token,
            )
            return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "RETRY", "error": msg}

        _set_run_queue_status(queue_id=queue_id, status="FAILED", error_class="SOURCE_ERROR", error_message=msg, finished=True, claim_token=claim_token)
        return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "FAILED", "error": msg}

    except Exception as e:
        msg = str(e)
        st = traceback.format_exc(limit=50)

        _update_step(step_id=run_step, status="FAILED", finished=True, message=msg, details={"sre_metrics": dict(SRE_METRICS)})
        _finish_batch_process(
            run_id=run_id,
            status="FAILED",
            bronze_row_count=bronze_count,
            silver_row_count=silver_count,
            error_class="RUNTIME_ERROR",
            error_message=msg,
            error_stacktrace=st,
        )

        attempt = int(item.get("attempt") or 0)
        max_retries = int(item.get("max_retries") or 0)
        next_attempt = attempt + 1
        if next_attempt <= max_retries:
            backoff_minutes = min(60, 2 ** attempt * 5)
            _set_run_queue_status(
                queue_id=queue_id,
                status="PENDING",
                error_class="RUNTIME_ERROR",
                error_message=msg,
                attempt_inc=True,
                next_retry_at_minutes=backoff_minutes,
                claim_token=claim_token,
            )
            return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "RETRY", "error": msg}

        _set_run_queue_status(queue_id=queue_id, status="FAILED", error_class="RUNTIME_ERROR", error_message=msg, finished=True, claim_token=claim_token)
        return {"queue_id": queue_id, "dataset_id": dataset_id, "run_id": run_id, "status": "FAILED", "error": msg}


# -----------------------------
# Main
# -----------------------------

print(f"[{now_utc_iso()}] governed_ingestion_orchestrator starting")
print(f"catalog={CATALOG} ctrl={CTRL} ops={OPS}")
print(f"claim_owner={CLAIM_OWNER} max_items={MAX_ITEMS} max_parallelism={MAX_PARALLELISM}")

if TARGET_DATASET_ID:
    print(f"🎯 EXECUÇÃO TARGETIZADA ativada para dataset_id={TARGET_DATASET_ID}")
else:
    print(f"📦 EXECUÇÃO BATCH (processando múltiplos datasets)")

_ensure_steps_table()
_ensure_run_queue_transitions_table()

# Cleanup: Marca jobs órfãos como FAILED (RUNNING/PENDING há mais de 2 horas)
timeout_threshold_minutes = 120
stale_claim_threshold_minutes = 30
print(f"\n[CLEANUP] Verificando jobs órfãos (timeout > {timeout_threshold_minutes} minutos)...")
try:
    _retry_delta_sql(
        f"""
        UPDATE {OPS}.run_queue
        SET 
          status = 'FAILED',
          last_error_class = 'TIMEOUT',
          last_error_message = 'Job órfão detectado - excedeu {timeout_threshold_minutes} minutos sem finalizar',
          finished_at = current_timestamp()
        WHERE status IN ('RUNNING', 'PENDING')
          AND started_at IS NOT NULL
          AND finished_at IS NULL
          AND started_at < current_timestamp() - INTERVAL {timeout_threshold_minutes} MINUTES
        """,
        context="CLEANUP"
    )
    print(f"[CLEANUP] ✓ Limpeza de órfãos concluída")
except Exception as cleanup_error:
    print(f"[CLEANUP] ⚠️  Erro ao limpar órfãos (continuando): {cleanup_error}")

print(f"[CLEANUP] Verificando CLAIMED órfãos (timeout > {stale_claim_threshold_minutes} minutos)...")
try:
    stale_claim_rows = spark.sql(  # type: ignore[name-defined]
        f"""
        SELECT COUNT(*) AS c
        FROM {OPS}.run_queue
        WHERE status = 'CLAIMED'
          AND claim_timestamp IS NOT NULL
          AND claim_timestamp < current_timestamp() - INTERVAL {stale_claim_threshold_minutes} MINUTES
        """
    ).collect()
    stale_claim_count = int(stale_claim_rows[0]["c"]) if stale_claim_rows else 0
    _retry_delta_sql(
        f"""
        UPDATE {OPS}.run_queue
        SET
          status = 'PENDING',
          claim_owner = NULL,
          claimed_by = NULL,
          claim_token = NULL,
          claimed_at = NULL,
          claim_timestamp = NULL,
          next_retry_at = current_timestamp(),
          last_error_class = 'CLAIM_TIMEOUT',
          last_error_message = 'Claim órfão resetado por timeout',
          state_version = COALESCE(state_version, 0) + 1
        WHERE status = 'CLAIMED'
          AND claim_timestamp IS NOT NULL
          AND claim_timestamp < current_timestamp() - INTERVAL {stale_claim_threshold_minutes} MINUTES
        """,
        context="CLEANUP_CLAIMED"
    )
    if stale_claim_count > 0:
        _metric_inc("stale_claim_resets", stale_claim_count)
    print(f"[CLEANUP] ✓ Limpeza de CLAIMED órfãos concluída")
except Exception as cleanup_claim_error:
    print(f"[CLEANUP] ⚠️ Erro ao limpar CLAIMED órfãos (continuando): {cleanup_claim_error}")

status_counts_rows = spark.sql(  # type: ignore[name-defined]
    f"SELECT status, COUNT(*) AS cnt FROM {OPS}.run_queue GROUP BY status ORDER BY status"
).collect()
status_counts = {str(r["status"]): int(r["cnt"]) for r in status_counts_rows}

eligible_pending = int(
    spark.sql(  # type: ignore[name-defined]
        f"SELECT COUNT(*) AS c FROM {OPS}.run_queue WHERE status = 'PENDING' AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp())"
    ).collect()[0]["c"]
)

pending_rows = spark.sql(  # type: ignore[name-defined]
    f"""
    SELECT
      queue_id, dataset_id, trigger_type,
      requested_at, priority,
      attempt, max_retries, next_retry_at,
      last_error_class, last_error_message
    FROM {OPS}.run_queue
    WHERE status = 'PENDING'
    ORDER BY requested_at DESC
    LIMIT 20
    """
).collect()

failed_rows = spark.sql(  # type: ignore[name-defined]
    f"""
    SELECT
      queue_id, dataset_id, trigger_type,
      requested_at, priority,
      attempt, max_retries, next_retry_at,
      last_error_class, last_error_message
    FROM {OPS}.run_queue
    WHERE status = 'FAILED'
    ORDER BY finished_at DESC NULLS LAST, requested_at DESC
    LIMIT 20
    """
).collect()

pending_items = [r.asDict(recursive=True) for r in pending_rows]
failed_items = [r.asDict(recursive=True) for r in failed_rows]

# Claim and process in batches of MAX_PARALLELISM to limit concurrent DB connections
# (prevents ORA-12516 / TNS:listener exhaustion when processing many Oracle datasets)
results = []
total_claimed = 0
batch_num = 0

while total_claimed < MAX_ITEMS:
    batch_num += 1
    batch_size = min(MAX_PARALLELISM, MAX_ITEMS - total_claimed)

    print(f"\n[BATCH {batch_num}] Claiming up to {batch_size} items (total so far: {total_claimed}/{MAX_ITEMS})...")
    try:
        items = claim_pending(batch_size, target_dataset_id=TARGET_DATASET_ID, job_id=JOB_ID)
    except ValueError:
        # claim_pending raises ValueError when targeted dataset not found — stop gracefully
        if total_claimed > 0:
            print(f"[BATCH {batch_num}] No more pending items. Stopping.")
            break
        raise

    if not items:
        print(f"[BATCH {batch_num}] No more pending items. Stopping.")
        break

    print(f"[BATCH {batch_num}] Processing {len(items)} items...")
    for it in items:
        results.append(run_one(it))

    total_claimed += len(items)
    print(f"[BATCH {batch_num}] ✓ Batch complete. Total processed: {total_claimed}")

print(f"\n[ORCHESTRATOR] All batches complete. Total claimed: {total_claimed}, Results: {len(results)}")

payload = {
    "claimed": total_claimed,
    "eligible_pending": eligible_pending,
    "status_counts": status_counts,
    "pending_items": pending_items,
    "failed_items": failed_items,
    "results": results,
    "sre_metrics": dict(SRE_METRICS),
}

# ===== FORMATTED EXECUTION SUMMARY =====
print("\n" + "="*80)
print("RESUMO DA EXECUÇÃO DO ORCHESTRATOR")
print("="*80)

print(f"\n📅 Data/Hora: {now_utc_iso()}")
print(f"⚙️  Catalog: {CATALOG}")
print(f"🎯 Max Items: {MAX_ITEMS}")

print("\n📊 Status da Fila (run_queue):")
for status, count in sorted(status_counts.items()):
    emoji = "🟢" if status == "SUCCEEDED" else "🔴" if status == "FAILED" else "🟡" if status == "PENDING" else "🔵"
    print(f"  {emoji} {status}: {count}")

print(f"\n⏱️  Pending Elegíveis (agora): {eligible_pending}")
print(f"✅ Jobs Claimed nesta execução: {total_claimed} (em {batch_num} batches de {MAX_PARALLELISM})")

if results:
    print("\n📄 Resultados da Execução:")
    for idx, res in enumerate(results, 1):
        status = res.get("status", "UNKNOWN")
        dataset_id = res.get("dataset_id", "N/A")
        run_id = res.get("run_id", "N/A")
        error = res.get("error", "")
        
        status_emoji = "✅" if status == "SUCCEEDED" else "❌" if status == "FAILED" else "🔄" if status == "RETRY" else "⚠️"
        
        print(f"\n  [{idx}] {status_emoji} Dataset: {dataset_id}")
        print(f"      Run ID: {run_id}")
        print(f"      Status: {status}")
        if error:
            print(f"      Erro: {error[:150]}..." if len(error) > 150 else f"      Erro: {error}")
else:
    print("\nℹ️  Nenhum job foi executado nesta rodada.")

if pending_items:
    print(f"\n🗒️  Próximos {len(pending_items)} PENDING (mais recentes):")
    for idx, item in enumerate(pending_items[:5], 1):  # Show only first 5
        dataset_id = item.get("dataset_id", "N/A")
        attempt = item.get("attempt", 0)
        max_retries = item.get("max_retries", 0)
        last_error = item.get("last_error_message", "")
        print(f"  {idx}. {dataset_id} (tentativa {attempt}/{max_retries})")
        if last_error:
            print(f"     ⚠️  Último erro: {last_error[:100]}..." if len(last_error) > 100 else f"     ⚠️  Último erro: {last_error}")

if failed_items:
    print(f"\n❌ FAILED recentes ({len(failed_items)} total):")
    for idx, item in enumerate(failed_items[:5], 1):  # Show only first 5
        dataset_id = item.get("dataset_id", "N/A")
        error_class = item.get("last_error_class", "N/A")
        error_msg = item.get("last_error_message", "")
        print(f"  {idx}. {dataset_id} ({error_class})")
        if error_msg:
            print(f"     {error_msg[:100]}..." if len(error_msg) > 100 else f"     {error_msg}")

print("\n" + "="*80)
print("✅ ORCHESTRATOR FINALIZADO")
print("="*80)

print("\n📝 Detalhes completos (JSON):")
print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))

print(f"\n[{now_utc_iso()}] governed_ingestion_orchestrator finished")

# Make the result retrievable via Jobs "get-run-output".
try:
    dbutils.notebook.exit(json.dumps(payload, ensure_ascii=False, default=str))  # type: ignore[name-defined]
except Exception:
    pass
