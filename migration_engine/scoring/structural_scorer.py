"""
Structural Scorer — calculates complexity score from AST metrics.
"""

from migration_engine.parser.ast_analyzer import StructuralMetrics
from migration_engine.config import STRUCTURAL_LEVEL_THRESHOLDS


def compute_structural_score(metrics: StructuralMetrics) -> int:
    """
    Calculate structural complexity score.

    Scoring rules:
      +1 per JOIN
      +2 per subquery
      +2 if subquery_depth > 1
      +1 per GROUP BY
      +1 per aggregate function
      +2 per window/analytic function
      +1 per CASE expression
    """
    score = 0
    score += metrics.join_count * 1
    score += metrics.subquery_count * 2
    if metrics.subquery_max_depth > 1:
        score += (metrics.subquery_max_depth - 1) * 2
    score += metrics.group_by_count * 1
    score += metrics.aggregate_count * 1
    score += metrics.analytic_function_count * 2
    score += metrics.case_count * 1
    return score


def compute_complexity_level(structural_score: int) -> int:
    """
    Map structural score to complexity level (1-5).
    """
    for threshold, level in STRUCTURAL_LEVEL_THRESHOLDS:
        if structural_score <= threshold:
            return level
    return 5
