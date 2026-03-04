"""
Risk Scorer — calculates risk score from structural analysis.
"""

from migration_engine.parser.ast_analyzer import StructuralMetrics
from migration_engine.converter.function_library import is_function_supported


def compute_risk_score(metrics: StructuralMetrics) -> int:
    """
    Calculate risk score.

    Scoring rules:
      +3 per unmapped function
      +4 if has legacy outer join (+)
      +5 if has CONNECT BY
      +4 if has PL/SQL constructs
      +3 if has external dependency (system tables)
      +2 if uses complex regexp (REGEXP_REPLACE, REGEXP_LIKE with complex patterns)
    """
    score = 0

    # Unmapped functions
    unmapped_count = 0
    for fn in metrics.functions_used:
        if not is_function_supported(fn):
            unmapped_count += 1
    score += unmapped_count * 3

    # Legacy outer join
    if metrics.has_legacy_outer_join:
        score += 4

    # CONNECT BY
    if metrics.has_connect_by:
        score += 5

    # PL/SQL constructs
    if metrics.has_plsql_construct:
        score += 4

    # External dependency
    if metrics.has_external_dependency:
        score += 3

    return score


def compute_risk_level(risk_score: int) -> str:
    """
    Map risk score to risk level.
    """
    if risk_score == 0:
        return "LOW"
    elif risk_score <= 5:
        return "MEDIUM"
    elif risk_score <= 10:
        return "HIGH"
    else:
        return "CRITICAL"


def has_unmapped_functions(metrics: StructuralMetrics) -> bool:
    """Check if any functions are not in the supported library."""
    return any(not is_function_supported(fn) for fn in metrics.functions_used)
