"""
Databricks SQL Connector — thread-safe wrapper with retry logic.
"""

import threading
import time
import logging
from typing import Any, Dict, List, Optional

from databricks.sql import connect
from databricks.sql.client import Connection

from migration_engine.config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    DATABRICKS_HTTP_PATH,
    MAX_RETRIES,
)

logger = logging.getLogger(__name__)

_local = threading.local()


class DatabricksConnector:
    """Manages Databricks SQL connections (one per thread)."""

    @staticmethod
    def _get_connection() -> Connection:
        """Return a thread-local connection, creating one if needed."""
        conn = getattr(_local, "connection", None)
        if conn is None:
            if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH]):
                raise ValueError("Missing Databricks connection credentials in .env")
            conn = connect(
                server_hostname=DATABRICKS_HOST,
                http_path=DATABRICKS_HTTP_PATH,
                access_token=DATABRICKS_TOKEN,
            )
            _local.connection = conn
        return conn

    @classmethod
    def execute(
        cls,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        fetch: bool = True,
        retries: int = MAX_RETRIES,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL statement and optionally return rows as list of dicts.

        Retries on transient errors (connection reset, timeout).
        """
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                conn = cls._get_connection()
                cursor = conn.cursor()
                cursor.execute(sql, parameters=params)

                if not fetch or cursor.description is None:
                    cursor.close()
                    return None

                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                cursor.close()
                return [dict(zip(columns, row)) for row in rows]

            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                is_transient = any(
                    kw in error_str
                    for kw in ["timeout", "connection", "reset", "unavailable", "throttl"]
                )
                if is_transient and attempt < retries:
                    wait = 2 ** attempt
                    logger.warning(
                        "Transient error (attempt %d/%d), retrying in %ds: %s",
                        attempt, retries, wait, e,
                    )
                    # Force new connection on next attempt
                    _local.connection = None
                    time.sleep(wait)
                else:
                    raise

        raise last_error  # type: ignore[misc]

    @classmethod
    def execute_read(cls, sql: str) -> List[Dict[str, Any]]:
        """Execute a read-only query and return rows."""
        result = cls.execute(sql, fetch=True)
        return result or []

    @classmethod
    def execute_write(cls, sql: str) -> None:
        """Execute a DDL/DML statement (no result)."""
        cls.execute(sql, fetch=False)

    @classmethod
    def explain(cls, sql: str) -> str:
        """Run EXPLAIN on a SQL statement, return the plan as string."""
        rows = cls.execute(f"EXPLAIN {sql}", fetch=True)
        if rows:
            return "\n".join(str(list(r.values())[0]) for r in rows)
        return ""

    @classmethod
    def test_query(cls, sql: str) -> bool:
        """Run SELECT ... LIMIT 1 and return True if it succeeds."""
        try:
            cls.execute(f"SELECT * FROM ({sql}) __t LIMIT 1", fetch=True)
            return True
        except Exception as e:
            logger.warning("Test query failed: %s", e)
            return False

    @classmethod
    def close_thread_connection(cls) -> None:
        """Explicitly close the current thread's connection."""
        conn = getattr(_local, "connection", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            _local.connection = None
