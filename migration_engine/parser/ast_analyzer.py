"""
AST Analyzer — extract structural metrics from a parsed Oracle SQL expression.
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Set

from sqlglot import exp

logger = logging.getLogger(__name__)


@dataclass
class StructuralMetrics:
    """Metrics extracted from AST analysis."""
    # Counts
    table_count: int = 0
    join_count: int = 0
    subquery_count: int = 0
    subquery_max_depth: int = 0
    group_by_count: int = 0
    order_by_count: int = 0
    case_count: int = 0
    aggregate_count: int = 0
    analytic_function_count: int = 0

    # Risk flags
    has_unmapped_function: bool = False
    has_legacy_outer_join: bool = False
    has_connect_by: bool = False
    has_plsql_construct: bool = False
    has_external_dependency: bool = False
    dependency_count: int = 0

    # Extracted elements
    tables: List[str] = field(default_factory=list)
    functions_used: List[str] = field(default_factory=list)
    referenced_views: List[str] = field(default_factory=list)

    # Raw SQL for fallback analysis
    raw_sql: str = ""


# Known Oracle aggregate functions
_AGGREGATE_FUNCTIONS = {
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "LISTAGG", "XMLAGG", "COLLECT",
    "STDDEV", "VARIANCE", "MEDIAN",
}

# Known Oracle analytic/window functions
_ANALYTIC_FUNCTIONS = {
    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
    "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
    "CUME_DIST", "PERCENT_RANK", "NTH_VALUE",
    "RATIO_TO_REPORT",
}

# Oracle system tables/views that indicate external dependency
_SYSTEM_TABLES = {
    "USER_OBJECTS", "ALL_OBJECTS", "DBA_OBJECTS",
    "USER_TABLES", "ALL_TABLES", "DBA_TABLES",
    "USER_TAB_COLUMNS", "ALL_TAB_COLUMNS",
    "USER_VIEWS", "ALL_VIEWS", "DBA_VIEWS",
    "USER_SOURCE", "ALL_SOURCE", "DBA_SOURCE",
    "DUAL", "V$SESSION", "V$INSTANCE",
}

# PL/SQL-related keywords
_PLSQL_KEYWORDS = re.compile(
    r"\b(CURSOR\s+IS|BEGIN\s|END\s*;|EXCEPTION\s|DECLARE\s|PRAGMA\s|LOOP\s|EXIT\s+WHEN|FETCH\s)",
    re.IGNORECASE,
)


def _count_subquery_depth(node: exp.Expression) -> int:
    """Find max subquery nesting depth by iterating the AST."""
    max_depth = 0
    for subquery in node.find_all(exp.Subquery):
        depth = 0
        parent = subquery.parent
        while parent is not None:
            if isinstance(parent, exp.Subquery):
                depth += 1
            parent = getattr(parent, 'parent', None)
        max_depth = max(max_depth, depth + 1)
    return max_depth


def analyze(expression: exp.Expression, raw_sql: str = "") -> StructuralMetrics:
    """
    Analyze a sqlglot expression and extract structural metrics.
    """
    metrics = StructuralMetrics(raw_sql=raw_sql)

    # ── Tables ──────────────────────────────────────────────────
    tables_seen: Set[str] = set()
    for table in expression.find_all(exp.Table):
        name = table.name.upper() if table.name else ""
        if name and name not in tables_seen:
            tables_seen.add(name)
            catalog = (table.args.get("db") or table.args.get("catalog"))
            full_name = name
            if catalog:
                full_name = f"{catalog}.{name}".upper()
            metrics.tables.append(full_name)

            # Check for system table references
            if name in _SYSTEM_TABLES:
                metrics.has_external_dependency = True

    metrics.table_count = len(metrics.tables)

    # ── Joins ───────────────────────────────────────────────────
    joins = list(expression.find_all(exp.Join))
    metrics.join_count = len(joins)

    # ── Subqueries ──────────────────────────────────────────────
    subqueries = list(expression.find_all(exp.Subquery))
    metrics.subquery_count = len(subqueries)
    if subqueries:
        metrics.subquery_max_depth = _count_subquery_depth(expression)

    # ── GROUP BY ────────────────────────────────────────────────
    groups = list(expression.find_all(exp.Group))
    metrics.group_by_count = len(groups)

    # ── ORDER BY ────────────────────────────────────────────────
    orders = list(expression.find_all(exp.Order))
    metrics.order_by_count = len(orders)

    # ── CASE expressions ────────────────────────────────────────
    cases = list(expression.find_all(exp.Case))
    metrics.case_count = len(cases)

    # ── Functions ───────────────────────────────────────────────
    functions_seen: Set[str] = set()
    for func in expression.find_all(exp.Func):
        # Get function name
        fn_name = type(func).__name__.upper()
        # Also try sql_name for named functions
        if hasattr(func, "sql_name"):
            fn_name = func.sql_name().upper()
        elif isinstance(func, exp.Anonymous):
            fn_name = func.name.upper()

        if fn_name and fn_name not in functions_seen:
            functions_seen.add(fn_name)
            metrics.functions_used.append(fn_name)

    # Count aggregates and analytics
    for fn in functions_seen:
        if fn in _AGGREGATE_FUNCTIONS:
            metrics.aggregate_count += 1
        if fn in _ANALYTIC_FUNCTIONS:
            metrics.analytic_function_count += 1

    # Also count window functions by checking for Window expressions
    windows = list(expression.find_all(exp.Window))
    metrics.analytic_function_count = max(metrics.analytic_function_count, len(windows))

    # ── Legacy outer join (+) ───────────────────────────────────
    # sqlglot may convert (+) but let's also check raw SQL
    if raw_sql and "(+)" in raw_sql:
        metrics.has_legacy_outer_join = True

    # ── CONNECT BY ──────────────────────────────────────────────
    for node in expression.walk():
        node_type = type(node).__name__
        if "connect" in node_type.lower():
            metrics.has_connect_by = True
            break
    # Fallback: check raw SQL
    if raw_sql and re.search(r"\bCONNECT\s+BY\b", raw_sql, re.IGNORECASE):
        metrics.has_connect_by = True

    # ── PL/SQL constructs ───────────────────────────────────────
    if raw_sql and _PLSQL_KEYWORDS.search(raw_sql):
        metrics.has_plsql_construct = True

    # ── Referenced views (for dependency graph) ─────────────────
    # A table reference that matches a known view name is a view dependency.
    # This is populated later by the dependency resolver.

    return metrics
