"""
Function Library — defines which Oracle functions are safe to convert to Spark SQL.

sqlglot handles most conversions automatically. This module serves as a whitelist
to flag any function NOT in this list as requiring manual review.
"""

# Functions that sqlglot converts automatically or that exist natively in Spark SQL.
# Key = normalized uppercase Oracle function name.
SUPPORTED_FUNCTIONS = {
    # ── Null handling ─────────────────────────────────────────
    "NVL",           # → COALESCE
    "NVL2",          # → CASE WHEN
    "COALESCE",      # native
    "NULLIF",        # native
    "DECODE",        # → CASE WHEN

    # ── String functions ──────────────────────────────────────
    "SUBSTR",        # → SUBSTRING
    "SUBSTRING",     # native
    "INSTR",         # → LOCATE / instr
    "LENGTH",        # native
    "LOWER",         # native
    "UPPER",         # native
    "TRIM",          # native
    "LTRIM",         # native
    "RTRIM",         # native
    "LPAD",          # native
    "RPAD",          # native
    "REPLACE",       # native
    "TRANSLATE",     # → TRANSLATE
    "INITCAP",       # native
    "CONCAT",        # native
    "CHR",           # → CHR
    "ASCII",         # native
    "REGEXP_SUBSTR",  # → REGEXP_EXTRACT
    "REGEXP_REPLACE", # native
    "REGEXP_LIKE",    # → RLIKE (pattern)
    "REGEXP_EXTRACT", # native (Spark)

    # ── Date/Time functions ───────────────────────────────────
    "SYSDATE",       # → CURRENT_DATE / CURRENT_TIMESTAMP
    "CURRENT_DATE",  # native
    "CURRENT_TIMESTAMP", # native
    "TO_DATE",       # → TO_DATE
    "TO_CHAR",       # → DATE_FORMAT / CAST
    "TO_NUMBER",     # → CAST
    "TO_TIMESTAMP",  # native
    "TRUNC",         # → DATE_TRUNC (for dates)
    "ADD_MONTHS",    # native
    "MONTHS_BETWEEN", # native
    "LAST_DAY",      # native
    "NEXT_DAY",      # native
    "EXTRACT",       # native
    "DATE_TRUNC",    # native (Spark)
    "DATEDIFF",      # native (Spark)
    "DATE_ADD",      # native (Spark)
    "DATE_SUB",      # native (Spark)

    # ── Numeric functions ─────────────────────────────────────
    "ROUND",         # native
    "CEIL",          # native
    "FLOOR",         # native
    "ABS",           # native
    "MOD",           # native
    "SIGN",          # native
    "POWER",         # native
    "SQRT",          # native
    "GREATEST",      # native
    "LEAST",         # native

    # ── Aggregate functions ───────────────────────────────────
    "COUNT",         # native
    "SUM",           # native
    "AVG",           # native
    "MIN",           # native
    "MAX",           # native
    "STDDEV",        # native
    "VARIANCE",      # native

    # ── Analytic/Window functions ─────────────────────────────
    "ROW_NUMBER",    # native
    "RANK",          # native
    "DENSE_RANK",    # native
    "NTILE",         # native
    "LAG",           # native
    "LEAD",          # native
    "FIRST_VALUE",   # native
    "LAST_VALUE",    # native
    "CUME_DIST",     # native
    "PERCENT_RANK",  # native
    "NTH_VALUE",     # native

    # ── Type conversion ───────────────────────────────────────
    "CAST",          # native
    "CONVERT",       # native

    # ── Conditional ───────────────────────────────────────────
    "CASE",          # native
    "IF",            # native (Spark)
    "IIF",           # → IF
    "IFNULL",        # → COALESCE

    # ── Other common Oracle functions ─────────────────────────
    "ROWNUM",        # → ROW_NUMBER() (context-dependent)
    "LISTAGG",       # → CONCAT_WS + COLLECT_LIST (partial)
    "WM_CONCAT",     # → CONCAT_WS + COLLECT_LIST

    # ── SQL operators (sqlglot may surface these as Func nodes) ─
    "AND",           # logical AND
    "OR",            # logical OR
    "NOT",           # logical NOT
    "IN",            # IN clause
    "EXISTS",        # EXISTS subquery
    "BETWEEN",       # BETWEEN
    "LIKE",          # LIKE pattern
    "IS",            # IS NULL / IS NOT NULL
    "EQ",            # =
    "NEQ",           # !=
    "GT",            # >
    "GTE",           # >=
    "LT",            # <
    "LTE",           # <=
    "ADD",           # +
    "SUB",           # -
    "MUL",           # *
    "DIV",           # /
    "NEG",           # unary minus
    "BITWISEAND",    # &
    "BITWISEOR",     # |
    "BITWISEXOR",    # ^
    "PAREN",         # parentheses
    "STAR",          # *
    "DISTINCT",      # DISTINCT
    "ALL",           # ALL
    "ANY",           # ANY
    "UNION",         # UNION
    "INTERSECT",     # INTERSECT
    "EXCEPT",        # EXCEPT
    "SELECT",        # SELECT

    # ── sqlglot internal representations ──────────────────────
    # These are how sqlglot names certain node types internally
    "ANONYMOUS",     # generic function wrapper
    "DPIPE",         # || concatenation → CONCAT
    "CURRENTDATE",   # CURRENT_DATE
    "CURRENTTIMESTAMP", # CURRENT_TIMESTAMP
    "TOCHAR",        # TO_CHAR
    "TODATE",        # TO_DATE
    "TONUMBER",      # TO_NUMBER
    "REGEXPEXTRACT", # REGEXP_EXTRACT
    "REGEXPREPLACE", # REGEXP_REPLACE
    "REGEXPLIKE",    # REGEXP_LIKE
    "DATETRUNC",     # DATE_TRUNC
    "TIMESTAMPTRUNC", # TIMESTAMP_TRUNC
    "MONTHSBETWEEN", # MONTHS_BETWEEN
    "ADDMONTHS",     # ADD_MONTHS
    "LASTDAY",       # LAST_DAY
    "NEXTDAY",       # NEXT_DAY
    "SUBSTRING",     # SUBSTR → SUBSTRING
    "YEARDAY",       # internal
    "DAY",           # internal
    "MONTH",         # internal
    "YEAR",          # internal
    "DATEADD",       # internal
    "DECODE_CASE",   # sqlglot DECODE → CASE conversion
    "STR_TO_DATE",   # sqlglot TO_DATE representation
    "PAD",           # sqlglot LPAD/RPAD internal
    "STR_POSITION",  # sqlglot INSTR internal
    "TIME_TO_STR",   # sqlglot TO_CHAR(time) internal
    "TIME_DIFF",     # sqlglot date diff internal
    "STRTODATE",     # variant
    "TIMETOSTR",     # variant
    "STRPOSITION",   # variant
    "SAFEDIVIDE",    # safe division
    "TRY_CAST",      # Spark TRY_CAST
    "DATE_FORMAT",   # Spark DATE_FORMAT
    "COLLECT_LIST",  # Spark COLLECT_LIST
    "CONCAT_WS",     # Spark CONCAT_WS
    "SPLIT",         # Spark SPLIT
    "SIZE",          # Spark SIZE
    "ARRAY",         # Spark ARRAY
    "EXPLODE",       # Spark EXPLODE
    "STRUCT",        # Spark STRUCT
    "NAMED_STRUCT",  # Spark NAMED_STRUCT
    "WHEN",          # CASE WHEN internal
    "ISNULL",        # IS NULL internal
    "GROUPCONCAT",   # GROUP_CONCAT
    "LOCATE",        # LOCATE (Spark)
    "POSITION",      # POSITION
    "CHARINDEX",     # CHARINDEX
    "LEFT",          # LEFT
    "RIGHT",         # RIGHT
    "REVERSE",       # REVERSE
    "SPACE",         # SPACE
    "REPEAT",        # REPEAT
    "NUMTODSINTERVAL", # Oracle interval function
    "NVL",           # already above but ensure coverage
}


def is_function_supported(function_name: str) -> bool:
    """Check if an Oracle function is in the supported/convertible list."""
    return function_name.upper() in SUPPORTED_FUNCTIONS


def get_unsupported_functions(function_names: list) -> list:
    """Return list of function names not in the supported list."""
    return [fn for fn in function_names if not is_function_supported(fn)]
