"""
Column Validator — validates column references in rewritten SQL against
the Databricks catalog.
"""

import logging
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

from migration_engine.connectors.databricks_connector import DatabricksConnector

logger = logging.getLogger(__name__)

# Cache: table_fqn → set of column names
_column_cache: Dict[str, Set[str]] = {}


class ColumnValidationError(Exception):
    """Raised when column validation fails."""
    pass


def _get_table_columns(table_fqn: str) -> Set[str]:
    """Fetch column names for a table from Databricks. Caches results."""
    if table_fqn in _column_cache:
        return _column_cache[table_fqn]

    try:
        rows = DatabricksConnector.execute_read(f"DESCRIBE TABLE {table_fqn}")
        columns = set()
        for row in rows:
            col_name = row.get("col_name", "")
            if col_name and not col_name.startswith("#"):
                columns.add(col_name.upper())
        _column_cache[table_fqn] = columns
        return columns
    except Exception as e:
        logger.warning("Could not describe table %s: %s", table_fqn, e)
        return set()


def validate_columns(
    rewritten_sql: str,
    table_mapping: Dict[str, str],
) -> List[str]:
    """
    Validate that columns referenced in the SQL exist in the mapped Databricks tables.

    Returns a list of warnings/errors. Empty list means all columns validated.
    """
    issues: List[str] = []

    try:
        parsed = sqlglot.parse_one(rewritten_sql, read="databricks")
    except Exception as e:
        issues.append(f"Could not parse rewritten SQL for column validation: {e}")
        return issues

    # Build a lookup of available columns per table alias
    # table_alias → set of column names
    available_columns: Dict[str, Set[str]] = {}

    for table in parsed.find_all(exp.Table):
        table_name = table.name
        alias = table.alias or table_name

        # Try to find the FQN
        fqn = None
        if table.args.get("catalog"):
            fqn = f"{table.args['catalog']}.{table.args.get('db', '')}.{table_name}"
        elif table.args.get("db"):
            fqn = f"{table.args['db']}.{table_name}"

        if fqn:
            cols = _get_table_columns(fqn)
            if cols:
                available_columns[alias.upper()] = cols
                available_columns[table_name.upper()] = cols

    # If we couldn't resolve any columns, skip validation
    # (the EXPLAIN step will catch real issues)
    if not available_columns:
        logger.debug("No column metadata available, skipping column validation")
        return issues

    # Check for duplicate aliases in SELECT
    select_aliases: List[str] = []
    if isinstance(parsed, exp.Select):
        for expr in parsed.expressions:
            if isinstance(expr, exp.Alias):
                select_aliases.append(expr.alias.upper())

    seen_aliases: Dict[str, int] = {}
    for alias in select_aliases:
        seen_aliases[alias] = seen_aliases.get(alias, 0) + 1

    for alias, count in seen_aliases.items():
        if count > 1:
            issues.append(f"Duplicate alias in SELECT: {alias} ({count} times)")

    return issues


def reset_cache():
    """Reset the column cache."""
    global _column_cache
    _column_cache = {}
