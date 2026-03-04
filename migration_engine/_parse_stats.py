"""Compute parse success stats with SQLSanitizer enabled."""
import sys
from collections import Counter

sys.path.insert(0, "C:/dev/cm-databricks")

from migration_engine.extractor.xlsx_extractor import extract_views
from migration_engine.parser.sql_parser import parse_oracle_sql

views = extract_views()

parsed_ok = 0
parsed_fail = 0
precheck_fail = 0
fix_counts = Counter()
error_prefix = Counter()

for v in views:
    pr = parse_oracle_sql(v.original_sql)
    if pr.success:
        parsed_ok += 1
    else:
        parsed_fail += 1
        if pr.pre_parse_valid is False:
            precheck_fail += 1
        if pr.error:
            error_prefix[pr.error.split("\n")[0][:90]] += 1
    for fx in (pr.fixes_applied or []):
        fix_counts[fx] += 1

print(f"Total views: {len(views)}")
print(f"Parsed OK:   {parsed_ok}")
print(f"Parsed FAIL: {parsed_fail}")
print(f"Precheck FAIL (subset of parse fail): {precheck_fail}")
print()

print("Top sanitizer fixes applied:")
for fx, cnt in fix_counts.most_common(10):
    print(f"  {fx:35s} {cnt}")

print()
print("Top error prefixes:")
for e, cnt in error_prefix.most_common(10):
    print(f"  [{cnt:3d}] {e}")
