"""
Oracle SQL Parser using sqlglot.

Includes a pre-parse sanitization layer (SQLSanitizer) that cleans
Excel-sourced SQL before passing to the parser.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

import sqlglot
from sqlglot import exp

from migration_engine.sanitizer.sql_sanitizer import SQLSanitizer

logger = logging.getLogger(__name__)

# Module-level singleton
_sanitizer = SQLSanitizer()


@dataclass
class ParseResult:
    """Result of parsing an Oracle SQL statement."""
    success: bool
    expression: Optional[exp.Expression] = None
    error: Optional[str] = None
    raw_sql: str = ""
    sanitized_sql: str = ""
    fixes_applied: Optional[List[str]] = None
    pre_parse_valid: bool = True


def parse_oracle_sql(sql: str) -> ParseResult:
    """
    Parse Oracle SQL into a sqlglot AST.

    Pipeline: sanitize → validate → parse.
    Returns ParseResult with success=False if parsing fails.
    """
    # Phase 1: Sanitize
    san = _sanitizer.sanitize(sql)
    sanitized = san.sanitized_sql
    fixes = san.fixes_applied

    # Phase 2: Pre-parse validation (advisory)
    # NOTE: Do not block parsing on these checks. sqlglot with error_level=WARN can
    # often still parse statements with minor quote imbalances, and downstream logic
    # benefits from a best-effort AST.
    pre_parse_valid = san.pre_parse_valid
    pre_parse_error = san.validation_error

    # Phase 3: Parse
    try:
        expressions = sqlglot.parse(sanitized, read="oracle", error_level=sqlglot.ErrorLevel.WARN)
        if not expressions or expressions[0] is None:
            error_msg = "Empty parse result"
            if pre_parse_error:
                error_msg += f" | Pre-parse: {pre_parse_error}"
            return ParseResult(
                success=False, error=error_msg,
                raw_sql=sql, sanitized_sql=sanitized, fixes_applied=fixes,
                pre_parse_valid=pre_parse_valid,
            )

        return ParseResult(
            success=True, expression=expressions[0],
            raw_sql=sql, sanitized_sql=sanitized, fixes_applied=fixes,
            pre_parse_valid=pre_parse_valid,
        )

    except sqlglot.errors.ParseError as e:
        logger.warning("Parse error: %s", e)
        error_msg = str(e)
        if pre_parse_error:
            error_msg += f" | Pre-parse: {pre_parse_error}"
        return ParseResult(
            success=False, error=error_msg,
            raw_sql=sql, sanitized_sql=sanitized, fixes_applied=fixes,
            pre_parse_valid=pre_parse_valid,
        )
    except Exception as e:
        logger.warning("Unexpected parse error: %s", e)
        error_msg = str(e)
        if pre_parse_error:
            error_msg += f" | Pre-parse: {pre_parse_error}"
        return ParseResult(
            success=False, error=error_msg,
            raw_sql=sql, sanitized_sql=sanitized, fixes_applied=fixes,
            pre_parse_valid=pre_parse_valid,
        )
