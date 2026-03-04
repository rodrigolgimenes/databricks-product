"""
SQL Rewriter — transpiles Oracle SQL to Databricks Spark SQL via sqlglot,
then applies table name replacements.
"""

import logging
import re
from dataclasses import dataclass
from typing import Dict, Optional

import sqlglot
from sqlglot import exp

from migration_engine.converter.function_library import get_unsupported_functions

logger = logging.getLogger(__name__)


class RewriteError(Exception):
    """Raised when SQL rewrite fails."""
    pass


@dataclass
class RewriteResult:
    """Result of SQL rewrite."""
    success: bool
    rewritten_sql: str = ""
    error: Optional[str] = None
    warnings: list = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


def rewrite_sql(
    original_sql: str,
    table_mapping: Dict[str, str],
    view_name: str = "",
) -> RewriteResult:
    """
    Transpile Oracle SQL to Databricks Spark SQL.

    Steps:
    1. Use sqlglot to transpile Oracle → Databricks dialect
    2. Replace table references with fully-qualified Databricks names
    3. Validate no unsupported functions remain

    Args:
        original_sql: Original Oracle SQL
        table_mapping: Dict mapping Oracle table names → Databricks FQN
        view_name: For logging context

    Returns:
        RewriteResult with the transpiled SQL or error details
    """
    warnings = []

    try:
        # Step 1: Transpile via sqlglot
        transpiled = sqlglot.transpile(
            original_sql,
            read="oracle",
            write="databricks",
            error_level=sqlglot.ErrorLevel.WARN,
        )

        if not transpiled:
            return RewriteResult(success=False, error="Transpilation returned empty result")

        result_sql = transpiled[0]

        # Step 2: Replace table references
        # Parse the transpiled SQL to find and replace table names
        try:
            parsed = sqlglot.parse_one(result_sql, read="databricks")

            for table in parsed.find_all(exp.Table):
                oracle_name = table.name.upper() if table.name else ""
                # Also check with schema prefix
                schema = table.args.get("db")
                if schema:
                    full_oracle = f"{schema}.{oracle_name}".upper()
                    if full_oracle in table_mapping:
                        _apply_table_mapping(table, table_mapping[full_oracle])
                        continue

                if oracle_name in table_mapping:
                    _apply_table_mapping(table, table_mapping[oracle_name])

            result_sql = parsed.sql(dialect="databricks")

        except Exception as e:
            # If re-parsing fails, fall back to string replacement
            logger.warning("[%s] Re-parse failed, using string replacement: %s", view_name, e)
            warnings.append(f"Fell back to string replacement: {e}")
            result_sql = _string_replace_tables(result_sql, table_mapping)

        # Step 3: Check for unsupported functions in the final SQL
        try:
            final_parsed = sqlglot.parse_one(result_sql, read="databricks")
            fn_names = set()
            for func in final_parsed.find_all(exp.Func):
                if isinstance(func, exp.Anonymous):
                    fn_names.add(func.name.upper())
            unsupported = get_unsupported_functions(list(fn_names))
            if unsupported:
                warnings.append(f"Potentially unsupported functions in output: {unsupported}")
        except Exception:
            pass

        return RewriteResult(success=True, rewritten_sql=result_sql, warnings=warnings)

    except sqlglot.errors.ParseError as e:
        return RewriteResult(success=False, error=f"Parse error: {e}")
    except Exception as e:
        return RewriteResult(success=False, error=f"Rewrite error: {e}")


def _apply_table_mapping(table_node: exp.Table, fqn: str) -> None:
    """Replace a table node's catalog/db/name with a fully qualified Databricks name."""
    parts = fqn.split(".")
    if len(parts) == 3:
        table_node.set("catalog", exp.to_identifier(parts[0]))
        table_node.set("db", exp.to_identifier(parts[1]))
        table_node.set("this", exp.to_identifier(parts[2]))
    elif len(parts) == 2:
        table_node.set("db", exp.to_identifier(parts[0]))
        table_node.set("this", exp.to_identifier(parts[1]))
    else:
        table_node.set("this", exp.to_identifier(fqn))


def _string_replace_tables(sql: str, table_mapping: Dict[str, str]) -> str:
    """Fallback: replace table names via regex in the SQL string."""
    result = sql
    # Sort by length descending to replace longer names first
    for oracle_name, dbx_name in sorted(table_mapping.items(), key=lambda x: -len(x[0])):
        # Replace whole-word occurrences (case-insensitive)
        pattern = re.compile(r"\b" + re.escape(oracle_name) + r"\b", re.IGNORECASE)
        result = pattern.sub(dbx_name, result)
    return result
