"""
Classifier — determines complexity level (N1-N5) and auto-migration eligibility.
"""

from dataclasses import dataclass

from migration_engine.config import MAX_ELIGIBLE_COMPLEXITY, ELIGIBLE_RISK_LEVEL
from migration_engine.scoring.structural_scorer import (
    compute_structural_score,
    compute_complexity_level,
)
from migration_engine.scoring.risk_scorer import (
    compute_risk_score,
    compute_risk_level,
    has_unmapped_functions,
)
from migration_engine.parser.ast_analyzer import StructuralMetrics


@dataclass
class Classification:
    """Full classification result for a view."""
    structural_score: int
    risk_score: int
    complexity_level: int        # 1-5
    risk_level: str              # LOW, MEDIUM, HIGH, CRITICAL
    classification: str          # N1-N5
    eligible_for_auto_migration: bool
    ineligibility_reasons: list  # why not eligible


def _compute_classification(complexity_level: int, risk_level: str) -> str:
    """
    Classification matrix:
      Structural Low  + Risk Low     → N1
      Structural Med  + Risk Low     → N2
      Structural Med  + Risk Medium  → N3
      Structural High + Risk Medium  → N4
      Any             + Risk High/Critical → N5
    """
    if risk_level in ("HIGH", "CRITICAL"):
        return "N5"

    if complexity_level <= 2 and risk_level == "LOW":
        return "N1"
    elif complexity_level <= 3 and risk_level == "LOW":
        return "N2"
    elif complexity_level <= 3 and risk_level == "MEDIUM":
        return "N3"
    elif complexity_level <= 4 and risk_level == "MEDIUM":
        return "N4"
    else:
        return "N5"


def classify(metrics: StructuralMetrics, dependencies_resolved: bool = True) -> Classification:
    """
    Full classification pipeline for a view.
    """
    structural_score = compute_structural_score(metrics)
    risk_score = compute_risk_score(metrics)
    complexity_level = compute_complexity_level(structural_score)
    risk_level = compute_risk_level(risk_score)
    classification = _compute_classification(complexity_level, risk_level)

    # Eligibility check
    reasons = []

    if complexity_level > MAX_ELIGIBLE_COMPLEXITY:
        reasons.append(f"complexity_level={complexity_level} > {MAX_ELIGIBLE_COMPLEXITY}")

    if risk_level != ELIGIBLE_RISK_LEVEL:
        reasons.append(f"risk_level={risk_level} != {ELIGIBLE_RISK_LEVEL}")

    if has_unmapped_functions(metrics):
        from migration_engine.converter.function_library import is_function_supported
        unmapped = [fn for fn in metrics.functions_used if not is_function_supported(fn)]
        reasons.append(f"unmapped_functions={unmapped}")

    if not dependencies_resolved:
        reasons.append("dependencies_not_resolved")

    if metrics.has_connect_by:
        reasons.append("has_connect_by")

    if metrics.has_plsql_construct:
        reasons.append("has_plsql_construct")

    eligible = len(reasons) == 0

    return Classification(
        structural_score=structural_score,
        risk_score=risk_score,
        complexity_level=complexity_level,
        risk_level=risk_level,
        classification=classification,
        eligible_for_auto_migration=eligible,
        ineligibility_reasons=reasons,
    )
