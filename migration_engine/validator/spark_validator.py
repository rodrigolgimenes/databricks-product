"""
Spark Validator — validates rewritten SQL on Databricks via EXPLAIN,
SELECT LIMIT 1, temp view creation, and view promotion.
"""

import logging
from typing import Optional

from migration_engine.config import TARGET_SCHEMA
from migration_engine.connectors.databricks_connector import DatabricksConnector

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when Spark validation fails."""
    pass


def validate_explain(sql: str, view_name: str = "") -> str:
    """
    Run EXPLAIN on the SQL. Returns the explain plan.
    Raises ValidationError if EXPLAIN fails.
    """
    try:
        plan = DatabricksConnector.explain(sql)
        logger.debug("[%s] EXPLAIN succeeded", view_name)
        return plan
    except Exception as e:
        raise ValidationError(f"EXPLAIN failed for {view_name}: {e}")


def validate_sample(sql: str, view_name: str = "") -> bool:
    """
    Run SELECT * FROM (sql) LIMIT 1 to verify the SQL executes.
    Returns True if successful, raises ValidationError otherwise.
    """
    try:
        success = DatabricksConnector.test_query(sql)
        if not success:
            raise ValidationError(f"SELECT LIMIT 1 returned no data for {view_name}")
        logger.debug("[%s] Sample query succeeded", view_name)
        return True
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Sample query failed for {view_name}: {e}")


def create_temp_view(view_name: str, sql: str) -> None:
    """
    Create a temporary view for validation.
    """
    temp_name = f"tmp_migration_{view_name}".lower()
    ddl = f"CREATE OR REPLACE TEMPORARY VIEW {temp_name} AS {sql}"
    try:
        DatabricksConnector.execute_write(ddl)
        logger.debug("[%s] Temp view created: %s", view_name, temp_name)
    except Exception as e:
        raise ValidationError(f"Failed to create temp view for {view_name}: {e}")


def promote_view(view_name: str, sql: str, target_schema: Optional[str] = None) -> str:
    """
    Create the final production view in the target schema.

    Returns the fully qualified view name.
    """
    schema = target_schema or TARGET_SCHEMA
    fqn = f"{schema}.{view_name}".lower()

    ddl = f"CREATE OR REPLACE VIEW {fqn} AS {sql}"
    try:
        DatabricksConnector.execute_write(ddl)
        logger.info("[%s] View promoted: %s", view_name, fqn)
        return fqn
    except Exception as e:
        raise ValidationError(f"Failed to promote view {view_name}: {e}")


def drop_temp_view(view_name: str) -> None:
    """Drop the temporary migration view (cleanup)."""
    temp_name = f"tmp_migration_{view_name}".lower()
    try:
        DatabricksConnector.execute_write(f"DROP VIEW IF EXISTS {temp_name}")
    except Exception:
        pass
