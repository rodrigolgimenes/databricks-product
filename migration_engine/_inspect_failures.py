"""Inspect contamination patterns in parse-failed views."""
import sys, re, logging
from collections import Counter

# Silence parse-error spam for this analysis
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger("sqlglot").setLevel(logging.CRITICAL)
logging.getLogger("migration_engine").setLevel(logging.CRITICAL)

sys.path.insert(0, "C:/dev/cm-databricks")
from migration_engine.extractor.xlsx_extractor import extract_views
from migration_engine.parser.sql_parser import parse_oracle_sql

views = extract_views()
failed = []
for v in views:
    pr = parse_oracle_sql(v.original_sql)
    if not pr.success:
        failed.append((v, pr.error or ""))

print(f"Total failed: {len(failed)}")
print()

# Pattern detection
counts = Counter()
for v, err in failed:
    sql = v.original_sql

    if re.search(r"<br\s*/?>", sql, re.I):
        counts["html_br"] += 1
    if re.search(r"<(?!br)[a-z]+[^>]*>", sql, re.I):
        counts["html_other_tags"] += 1
    if "_x000D_" in sql:
        counts["excel_x000D"] += 1
    if re.search(r"THEN\s+\d+\s*-\s*[A-Z]{3,}", sql, re.I):
        counts["unquoted_case_text"] += 1
    if "|| - ||" in sql or "||'-'||" not in sql and re.search(r"\|\|\s*-\s*\|\|", sql):
        counts["broken_concat_dash"] += 1
    if re.search(r'"[A-Z_]+"\s*\n\s*\.\s*"', sql):
        counts["double_quote_linebreak"] += 1
    if re.search(r"\w+\s*=>\s*\w+", sql):
        counts["named_params_arrow"] += 1
    if re.search(r"\w+\.\w+\.\w+\(", sql):
        counts["pkg_function_call"] += 1
    if "(+)" in sql:
        counts["outer_join_plus"] += 1
    if re.search(r"CONNECT\s+BY", sql, re.I):
        counts["connect_by"] += 1
    if sql.count("(") != sql.count(")"):
        counts["parens_unbalanced"] += 1
    if sql.count('"') % 2 != 0:
        counts["quotes_unbalanced"] += 1
    if sql.count("'") % 2 != 0:
        counts["single_quotes_unbalanced"] += 1

    # Error-category patterns
    err_lower = err.lower()
    if "tokenizing" in err_lower:
        counts["err_tokenizing"] += 1
    if "expected end after case" in err_lower:
        counts["err_case_end"] += 1
    if "expecting )" in err_lower:
        counts["err_expecting_paren"] += 1
    if "invalid expression" in err_lower:
        counts["err_invalid_expr"] += 1
    if "missing" in err_lower and "keyword" in err_lower:
        counts["err_missing_keyword"] += 1

print("=== Contamination patterns in FAILED views ===")
for pat_name, cnt in counts.most_common():
    pct = cnt * 100 / len(failed)
    print(f"  {pat_name:35s}: {cnt:4d} ({pct:5.1f}%)")

# Now look at the top error patterns from sqlglot
print()
print("=== Error samples by type ===")

# Group by first error line
error_types = Counter()
for v, err in failed:
    first_line = err.split("\n")[0][:80] if err else "unknown"
    error_types[first_line] += 1

for et, cnt in error_types.most_common(15):
    print(f"  [{cnt:3d}] {et}")

# Show a few examples of the worst patterns
print()
print("=== Sample failed SQL snippets (first 200 chars) ===")
for v, err in failed[:5]:
    print(f"--- {v.view_name} ---")
    print(v.original_sql[:200])
    print(f"  ERROR: {(err or '')[:120]}")
    print()
