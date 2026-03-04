"""
Pre-Parse SQL Sanitizer — cleans and repairs Excel-sourced Oracle SQL
before passing to sqlglot parser.

This is a deterministic, regex-based repair engine. Every rule is:
  - idempotent
  - logged (returns list of applied fixes)
  - non-destructive to valid SQL

Applied BEFORE sqlglot.parse().
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Tuple

logger = logging.getLogger(__name__)


@dataclass
class SanitizeResult:
    """Result of the sanitization process."""
    original_sql: str
    sanitized_sql: str
    fixes_applied: List[str] = field(default_factory=list)
    pre_parse_valid: bool = True
    validation_error: str = ""


class SQLSanitizer:
    """
    Pre-parse SQL repair engine for Excel-exported Oracle views.

    Handles:
      Phase A — Cleanup (HTML, Excel escapes, whitespace)
      Phase B — Repair (broken strings, parens, concat, comments)
      Phase C — Pre-parse validation (balanced parens/quotes, SELECT present)
    """

    def sanitize(self, raw_sql: str) -> SanitizeResult:
        """Run the full sanitization pipeline."""
        result = SanitizeResult(original_sql=raw_sql, sanitized_sql=raw_sql)
        sql = raw_sql

        # ─── Phase A: Cleanup ────────────────────────────────────
        sql = self._apply(sql, result, self._normalize_line_breaks, "normalize_line_breaks")
        sql = self._apply(sql, result, self._remove_html_tags, "remove_html_tags")
        sql = self._apply(sql, result, self._remove_excel_escapes, "remove_excel_escapes")
        sql = self._apply(sql, result, self._strip_sql_comments, "strip_sql_comments")

        # ─── Phase B: Repair ─────────────────────────────────────
        sql = self._apply(sql, result, self._fix_rogue_semicolons, "fix_rogue_semicolons")
        sql = self._apply(sql, result, self._fix_broken_double_quote_identifiers, "fix_broken_dquote_identifiers")
        sql = self._apply(sql, result, self._fix_named_params, "fix_named_params")
        sql = self._apply(sql, result, self._fix_broken_concat, "fix_broken_concat")
        sql = self._apply(sql, result, self._fix_trailing_comma_before_from, "fix_trailing_comma_before_from")
        sql = self._apply(sql, result, self._fix_duplicate_as, "fix_duplicate_as")
        sql = self._apply(sql, result, self._fix_invalid_number_literal, "fix_invalid_number_literal")
        sql = self._apply(sql, result, self._fix_missing_comparison_operator, "fix_missing_comparison_operator")
        sql = self._apply(sql, result, self._fix_broken_single_quotes, "fix_broken_single_quotes")
        sql = self._apply(sql, result, self._balance_double_quotes, "balance_double_quotes")
        sql = self._apply(sql, result, self._balance_single_quotes, "balance_single_quotes")
        sql = self._apply(sql, result, self._balance_parentheses, "balance_parentheses")
        sql = self._apply(sql, result, self._normalize_whitespace, "normalize_whitespace")

        # Final strip
        sql = sql.strip()
        # Remove trailing semicolons
        sql = sql.rstrip(";").strip()

        result.sanitized_sql = sql

        # ─── Phase C: Pre-parse validation ───────────────────────
        self._validate_preparse(result)

        return result

    @staticmethod
    def _apply(sql: str, result: SanitizeResult, fn, name: str) -> str:
        """Apply a fix function; record it if the SQL changed."""
        new_sql = fn(sql)
        if new_sql != sql:
            result.fixes_applied.append(name)
        return new_sql

    # ═══════════════════════════════════════════════════════════════
    # Phase A: Cleanup
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def _normalize_line_breaks(sql: str) -> str:
        """Normalize all line endings to \n."""
        return sql.replace("\r\n", "\n").replace("\r", "\n")

    @staticmethod
    def _remove_html_tags(sql: str) -> str:
        """Remove <br>, <br/>, and any other HTML tags."""
        sql = re.sub(r"<br\s*/?>", "\n", sql, flags=re.IGNORECASE)
        sql = re.sub(r"<[^>]+>", "", sql)
        return sql

    @staticmethod
    def _remove_excel_escapes(sql: str) -> str:
        """Remove Excel-specific escape sequences."""
        sql = sql.replace("_x000D_", "")
        # Remove other _xNNNN_ Excel XML escapes
        sql = re.sub(r"_x[0-9A-Fa-f]{4}_", "", sql)
        return sql

    @staticmethod
    def _strip_sql_comments(sql: str) -> str:
        """
        Remove SQL single-line comments (--) that are NOT inside string literals.
        Preserves multi-line /* */ comments (sqlglot handles those).
        Also handles broken comments like --(+) which are commented-out outer joins.
        """
        lines = sql.split("\n")
        cleaned = []
        in_string = False
        for line in lines:
            new_line = []
            i = 0
            in_str_char = None
            while i < len(line):
                ch = line[i]

                # Track string literal state
                if ch in ("'", '"') and not in_str_char:
                    in_str_char = ch
                elif ch == in_str_char:
                    # Check for escaped quote (doubled)
                    if i + 1 < len(line) and line[i + 1] == ch:
                        new_line.append(ch)
                        i += 1  # skip the doubled quote
                    else:
                        in_str_char = None

                # Detect -- outside string
                if ch == '-' and i + 1 < len(line) and line[i + 1] == '-' and not in_str_char:
                    # Rest of line is comment — drop it
                    break

                new_line.append(ch)
                i += 1

            cleaned.append("".join(new_line).rstrip())

        return "\n".join(cleaned)

    # ═══════════════════════════════════════════════════════════════
    # Phase B: Repair
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def _fix_broken_double_quote_identifiers(sql: str) -> str:
        """
        Fix identifiers broken across lines by Excel:
          "TABLE_NAME"  \\n  ."COLUMN_NAME"  →  "TABLE_NAME"."COLUMN_NAME"
        """
        sql = re.sub(r'"\s*\n\s*\.\s*"', '"."', sql)
        return sql

    @staticmethod
    def _fix_named_params(sql: str) -> str:
        """
        Convert Oracle PL/SQL named parameter syntax to positional:
          FUNC(PARAM_NAME => value)  →  FUNC(value)

        This removes the parameter names since Spark SQL doesn't support named params.
        """
        # Match WORD => (not inside strings)
        sql = re.sub(r"\b\w+\s*=>\s*", "", sql)
        return sql

    @staticmethod
    def _fix_broken_concat(sql: str) -> str:
        """Fix common broken concatenation patterns."""
        # || - || → || '-' ||
        sql = sql.replace("|| - ||", "|| '-' ||")
        # Handle || with missing operand: || || → || '' ||
        sql = re.sub(r"\|\|\s*\|\|", "|| '' ||", sql)
        return sql

    @staticmethod
    def _fix_trailing_comma_before_from(sql: str) -> str:
        """Remove trailing comma before FROM clause."""
        return re.sub(r",\s*\n(\s*FROM\b)", r"\n\1", sql, flags=re.IGNORECASE)

    @staticmethod
    def _fix_duplicate_as(sql: str) -> str:
        """Fix duplicated AS keyword: 'AS AS' → 'AS'."""
        return re.sub(r"\bAS\s+AS\b", "AS", sql, flags=re.IGNORECASE)

    @staticmethod
    def _fix_broken_single_quotes(sql: str) -> str:
        """
        Attempt to fix unbalanced single quotes by finding obvious patterns.

        Strategy: find lines where a string literal starts but doesn't close
        on the same logical expression, and close it.

        This is conservative — only fixes clear patterns like:
          THEN '45 - OPERACIONAL  (missing closing quote at end of known text)
        """
        # Pattern: THEN 'text without closing quote at end of line
        # Only fix if the text looks like a label (alpha + spaces + dashes)
        def fix_unclosed_then_literal(m):
            prefix = m.group(1)
            text = m.group(2)
            # If text doesn't end with quote, add one
            return f"{prefix}'{text}'"

        sql = re.sub(
            r"(THEN\s+)'([A-Za-zÀ-ú0-9\s\.\-/]+?)(?=\s*\n)",
            fix_unclosed_then_literal,
            sql,
            flags=re.IGNORECASE,
        )

        return sql

    @staticmethod
    def _fix_rogue_semicolons(sql: str) -> str:
        """
        Remove semicolons embedded inside a statement that break CTE / subquery parsing.
        E.g. `));\n  and` — the `;` after `)` is a rogue terminator.
        Also strip leading/trailing semicolons that don't delimit multi-statement blocks.
        """
        # Pattern: ");" followed by SQL continuation keywords
        sql = re.sub(
            r"\);(\s*(?:AND|OR|WHERE|FROM|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|GROUP|ORDER|UNION|HAVING|ON)\b)",
            r")\1",
            sql,
            flags=re.IGNORECASE,
        )
        return sql

    @staticmethod
    def _fix_invalid_number_literal(sql: str) -> str:
        """Fix number literals like '0.' followed by a keyword (Excel truncation)."""
        # 0. AND → 0 AND  ;  0. OR → 0 OR
        sql = re.sub(r"\b(\d+)\.\s+(AND|OR|THEN|ELSE|END|FROM|WHERE|GROUP|ORDER)\b",
                     r"\1 \2", sql, flags=re.IGNORECASE)
        return sql

    @staticmethod
    def _fix_missing_comparison_operator(sql: str) -> str:
        """
        Fix patterns where a comparison operator was lost (Excel truncation).
        E.g. column_name '31/12/2016'  →  column_name > '31/12/2016'
        Also: column_name 'string_value'  after AND/OR.

        This is conservative — only applies when a bare identifier is followed by
        a string literal with no intervening operator or keyword.
        """
        # Pattern: (identifier) (string_literal)  with no operator between them,
        # preceded by AND/OR/WHERE and not by keywords like THEN, AS, IN, LIKE
        sql = re.sub(
            r"(\b(?:AND|OR|WHERE)\s+)(\w+(?:\.\w+)*)\s+('[^']*')\b",
            r"\1\2 > \3",
            sql,
            flags=re.IGNORECASE,
        )
        return sql

    @staticmethod
    def _balance_double_quotes(sql: str) -> str:
        """
        If double-quote count is odd, try to fix it.
        Strategy: Find the last lone double-quote on a line that looks like
        truncated identifier and either close or remove it.
        """
        dq_count = sql.count('"')
        if dq_count % 2 == 0:
            return sql

        # Heuristic: if the last line ends mid-identifier (no closing quote),
        # add a closing double-quote at the very end.
        lines = sql.split("\n")

        # Walk backwards to find the orphan quote
        for i in range(len(lines) - 1, -1, -1):
            line_dq = lines[i].count('"')
            if line_dq % 2 != 0:
                # This line has the orphan quote
                stripped = lines[i].rstrip()
                if stripped.endswith('"'):
                    # Trailing orphan quote — likely truncated; remove it
                    lines[i] = stripped[:-1]
                else:
                    # Quote opened but never closed — close at end of line
                    lines[i] = stripped + '"'
                break

        return "\n".join(lines)

    @staticmethod
    def _balance_single_quotes(sql: str) -> str:
        """
        If single-quote count (excluding doubled '') is odd, try to fix.
        """
        # Remove all doubled single-quotes before counting
        temp = sql.replace("''", "")
        sq_count = temp.count("'")
        if sq_count % 2 == 0:
            return sql

        # Heuristic: find the last line with an orphan single-quote and close it
        lines = sql.split("\n")
        for i in range(len(lines) - 1, -1, -1):
            temp_line = lines[i].replace("''", "")
            if temp_line.count("'") % 2 != 0:
                stripped = lines[i].rstrip()
                if stripped.endswith("'"):
                    # Trailing orphan — might be truncated; leave as-is
                    # (removing would break the actual literal)
                    pass
                else:
                    lines[i] = stripped + "'"
                break

        return "\n".join(lines)

    @staticmethod
    def _balance_parentheses(sql: str) -> str:
        """
        If there are more open parens than close, append missing close parens.
        If more close than open, this is unfixable — leave it.

        This is a last-resort safety net, not a structural fix.
        """
        opens = sql.count("(")
        closes = sql.count(")")
        diff = opens - closes

        if diff > 0:
            # Append missing close parens at the end
            sql = sql.rstrip() + ")" * diff
        return sql

    @staticmethod
    def _normalize_whitespace(sql: str) -> str:
        """Normalize excessive whitespace (but preserve single newlines for readability)."""
        # Collapse multiple blank lines into one
        sql = re.sub(r"\n{3,}", "\n\n", sql)
        # Collapse multiple spaces/tabs on same line
        sql = re.sub(r"[ \t]+", " ", sql)
        return sql

    # ═══════════════════════════════════════════════════════════════
    # Phase C: Pre-parse Validation
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def _validate_preparse(result: SanitizeResult) -> None:
        """Run basic pre-parse checks. Sets pre_parse_valid flag."""
        sql = result.sanitized_sql
        errors = []

        # Must contain SELECT
        if not re.search(r"\bSELECT\b", sql, re.IGNORECASE):
            errors.append("Missing SELECT keyword")

        # Check double-quote balance (after sanitization)
        dq_count = sql.count('"')
        if dq_count % 2 != 0:
            errors.append(f"Unbalanced double quotes ({dq_count})")

        # Check single-quote balance (rough — doesn't account for escaped quotes)
        # Count non-escaped single quotes
        sq_count = len(re.findall(r"(?<!')'(?!')", sql))
        if sq_count % 2 != 0:
            errors.append(f"Unbalanced single quotes (~{sq_count})")

        if errors:
            result.pre_parse_valid = False
            result.validation_error = "; ".join(errors)
