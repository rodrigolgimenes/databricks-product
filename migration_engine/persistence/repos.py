"""
Persistence Repos — manages Delta tables for migration analysis,
execution logs, and results.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from migration_engine.config import CONTROL_SCHEMA, Status
from migration_engine.connectors.databricks_connector import DatabricksConnector

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Schema Setup
# ═══════════════════════════════════════════════════════════════════

def create_control_schema() -> None:
    """Create the migration control schema if it doesn't exist."""
    DatabricksConnector.execute_write(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
    logger.info("Ensured schema exists: %s", CONTROL_SCHEMA)


def create_control_tables() -> None:
    """Create all control tables if they don't exist."""
    create_control_schema()

    # migration_analysis
    DatabricksConnector.execute_write(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.migration_analysis (
            view_name STRING NOT NULL,
            source_schema STRING,
            analysis_ts TIMESTAMP,
            run_id STRING,
            original_sql STRING,
            table_count INT,
            join_count INT,
            subquery_count INT,
            subquery_max_depth INT,
            group_by_count INT,
            order_by_count INT,
            case_count INT,
            aggregate_count INT,
            analytic_function_count INT,
            has_unmapped_function BOOLEAN,
            has_legacy_outer_join BOOLEAN,
            has_connect_by BOOLEAN,
            has_plsql_construct BOOLEAN,
            has_external_dependency BOOLEAN,
            dependency_count INT,
            tables_json STRING,
            functions_json STRING,
            structural_score INT,
            risk_score INT,
            complexity_level INT,
            risk_level STRING,
            classification STRING,
            eligible_for_auto_migration BOOLEAN,
            ineligibility_reasons_json STRING,
            department STRING,
            in_use STRING
        )
        USING DELTA
    """)

    # migration_execution_log
    DatabricksConnector.execute_write(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.migration_execution_log (
            view_name STRING NOT NULL,
            stage STRING NOT NULL,
            status STRING NOT NULL,
            error_message STRING,
            run_id STRING,
            ts TIMESTAMP,
            payload_json STRING
        )
        USING DELTA
    """)

    # migration_results
    DatabricksConnector.execute_write(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.migration_results (
            view_name STRING NOT NULL,
            final_status STRING NOT NULL,
            complexity_level INT,
            risk_level STRING,
            classification STRING,
            rewritten_sql STRING,
            published_ts TIMESTAMP,
            warning_json STRING,
            error_message STRING,
            run_id STRING
        )
        USING DELTA
    """)

    logger.info("Control tables created in %s", CONTROL_SCHEMA)


# ═══════════════════════════════════════════════════════════════════
# Analysis Repo
# ═══════════════════════════════════════════════════════════════════

class AnalysisRepo:
    """Persists view analysis results."""

    @staticmethod
    def save(
        view_name: str,
        run_id: str,
        metrics: Any,
        classification: Any,
        original_sql: str = "",
        department: str = "",
        in_use: str = "",
    ) -> None:
        """Save analysis result for a view."""
        sql = f"""
            INSERT INTO {CONTROL_SCHEMA}.migration_analysis VALUES (
                '{_esc(view_name)}',
                NULL,
                current_timestamp(),
                '{_esc(run_id)}',
                '{_esc(original_sql[:4000])}',
                {metrics.table_count},
                {metrics.join_count},
                {metrics.subquery_count},
                {metrics.subquery_max_depth},
                {metrics.group_by_count},
                {metrics.order_by_count},
                {metrics.case_count},
                {metrics.aggregate_count},
                {metrics.analytic_function_count},
                {str(metrics.has_unmapped_function).lower()},
                {str(metrics.has_legacy_outer_join).lower()},
                {str(metrics.has_connect_by).lower()},
                {str(metrics.has_plsql_construct).lower()},
                {str(metrics.has_external_dependency).lower()},
                {metrics.dependency_count},
                '{_esc(json.dumps(metrics.tables))}',
                '{_esc(json.dumps(metrics.functions_used))}',
                {classification.structural_score},
                {classification.risk_score},
                {classification.complexity_level},
                '{classification.risk_level}',
                '{classification.classification}',
                {str(classification.eligible_for_auto_migration).lower()},
                '{_esc(json.dumps(classification.ineligibility_reasons))}',
                '{_esc(department or "")}',
                '{_esc(in_use or "")}'
            )
        """
        DatabricksConnector.execute_write(sql)

    @staticmethod
    def get_all(run_id: Optional[str] = None) -> List[Dict]:
        """Get all analysis records, optionally filtered by run_id."""
        where = f"WHERE run_id = '{_esc(run_id)}'" if run_id else ""
        return DatabricksConnector.execute_read(
            f"SELECT * FROM {CONTROL_SCHEMA}.migration_analysis {where}"
        )

    @staticmethod
    def get_eligible_views(run_id: Optional[str] = None) -> List[Dict]:
        """Get views eligible for auto-migration."""
        where = "WHERE eligible_for_auto_migration = true"
        if run_id:
            where += f" AND run_id = '{_esc(run_id)}'"
        return DatabricksConnector.execute_read(
            f"SELECT * FROM {CONTROL_SCHEMA}.migration_analysis {where}"
        )


# ═══════════════════════════════════════════════════════════════════
# Execution Log Repo
# ═══════════════════════════════════════════════════════════════════

class ExecutionLogRepo:
    """Persists execution stage checkpoints."""

    @staticmethod
    def log(
        view_name: str,
        stage: str,
        status: str,
        run_id: str,
        error_message: str = "",
        payload: Optional[Dict] = None,
    ) -> None:
        """Log a stage execution."""
        payload_json = json.dumps(payload) if payload else ""
        sql = f"""
            INSERT INTO {CONTROL_SCHEMA}.migration_execution_log VALUES (
                '{_esc(view_name)}',
                '{_esc(stage)}',
                '{_esc(status)}',
                '{_esc(error_message[:4000])}',
                '{_esc(run_id)}',
                current_timestamp(),
                '{_esc(payload_json)}'
            )
        """
        DatabricksConnector.execute_write(sql)

    @staticmethod
    def is_completed(view_name: str, stage: str, run_id: str) -> bool:
        """Check if a stage is already completed for a view."""
        rows = DatabricksConnector.execute_read(f"""
            SELECT 1 FROM {CONTROL_SCHEMA}.migration_execution_log
            WHERE view_name = '{_esc(view_name)}'
              AND stage = '{_esc(stage)}'
              AND status = 'DONE'
              AND run_id = '{_esc(run_id)}'
            LIMIT 1
        """)
        return len(rows) > 0

    @staticmethod
    def get_failure_count(view_name: str, stage: str, run_id: str) -> int:
        """Count failures for a view at a specific stage."""
        rows = DatabricksConnector.execute_read(f"""
            SELECT COUNT(*) as cnt FROM {CONTROL_SCHEMA}.migration_execution_log
            WHERE view_name = '{_esc(view_name)}'
              AND stage = '{_esc(stage)}'
              AND status = 'FAILED'
              AND run_id = '{_esc(run_id)}'
        """)
        return rows[0]["cnt"] if rows else 0


# ═══════════════════════════════════════════════════════════════════
# Results Repo
# ═══════════════════════════════════════════════════════════════════

class ResultRepo:
    """Persists final migration results."""

    @staticmethod
    def save(
        view_name: str,
        final_status: str,
        run_id: str,
        complexity_level: int = 0,
        risk_level: str = "",
        classification: str = "",
        rewritten_sql: str = "",
        warnings: Optional[List[str]] = None,
        error_message: str = "",
    ) -> None:
        """Save or update final result for a view."""
        warning_json = json.dumps(warnings) if warnings else "[]"
        published_ts = "current_timestamp()" if final_status == Status.MIGRATED_SUCCESS else "NULL"

        sql = f"""
            INSERT INTO {CONTROL_SCHEMA}.migration_results VALUES (
                '{_esc(view_name)}',
                '{_esc(final_status)}',
                {complexity_level},
                '{_esc(risk_level)}',
                '{_esc(classification)}',
                '{_esc(rewritten_sql[:8000])}',
                {published_ts},
                '{_esc(warning_json)}',
                '{_esc(error_message[:4000])}',
                '{_esc(run_id)}'
            )
        """
        DatabricksConnector.execute_write(sql)

    @staticmethod
    def get_all(run_id: Optional[str] = None) -> List[Dict]:
        """Get all results."""
        where = f"WHERE run_id = '{_esc(run_id)}'" if run_id else ""
        return DatabricksConnector.execute_read(
            f"SELECT * FROM {CONTROL_SCHEMA}.migration_results {where}"
        )

    @staticmethod
    def get_summary(run_id: Optional[str] = None) -> Dict[str, int]:
        """Get count of views by final_status."""
        where = f"WHERE run_id = '{_esc(run_id)}'" if run_id else ""
        rows = DatabricksConnector.execute_read(f"""
            SELECT final_status, COUNT(*) as cnt
            FROM {CONTROL_SCHEMA}.migration_results {where}
            GROUP BY final_status
        """)
        return {row["final_status"]: row["cnt"] for row in rows}


# ═══════════════════════════════════════════════════════════════════
# Utility
# ═══════════════════════════════════════════════════════════════════

def _esc(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    if not value:
        return ""
    return value.replace("'", "''").replace("\\", "\\\\")
