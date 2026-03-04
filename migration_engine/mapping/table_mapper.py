"""
Table Mapper — maps Oracle table names to Databricks Unity Catalog tables
using similarity matching with dynamic thresholds.
"""

import logging
import re
import threading
from typing import Dict, List, Optional, Tuple

from Levenshtein import ratio as levenshtein_ratio

from migration_engine.config import (
    SOURCE_SCHEMAS,
    SIMILARITY_WEIGHTS,
    dynamic_threshold,
)
from migration_engine.connectors.databricks_connector import DatabricksConnector

logger = logging.getLogger(__name__)

# Cache of Databricks catalog tables (populated once)
_catalog_cache: Optional[Dict[str, str]] = None
_cache_lock = threading.Lock()


class TableMappingError(Exception):
    """Raised when table mapping fails due to ambiguity or no match."""
    pass


def _load_catalog_tables() -> Dict[str, str]:
    """
    Load all tables from Databricks source schemas.
    Returns dict: normalized_name → fully_qualified_name
    """
    global _catalog_cache
    with _cache_lock:
        if _catalog_cache is not None:
            return _catalog_cache

        catalog = {}
        for schema in SOURCE_SCHEMAS:
            try:
                rows = DatabricksConnector.execute_read(f"SHOW TABLES IN {schema}")
                for row in rows:
                    table_name = row.get("tableName", "")
                    if table_name:
                        fqn = f"{schema}.{table_name}"
                        normalized = _normalize_name(table_name)
                        catalog[normalized] = fqn
            except Exception as e:
                logger.warning("Failed to list tables in %s: %s", schema, e)

        _catalog_cache = catalog
        logger.info("Loaded %d tables from catalog", len(catalog))
        return catalog


def _normalize_name(name: str) -> str:
    """
    Normalize a table name for comparison:
    - lowercase
    - remove schema prefixes (owner.table → table)
    - remove common environment prefixes
    - standardize underscores
    """
    name = name.lower().strip()

    # Remove schema prefix if present (e.g., "OWNER.TABLE" → "TABLE")
    if "." in name:
        name = name.split(".")[-1]

    # Remove common Oracle prefixes
    name = re.sub(r"^(cmaster_|rhmeta_|civil_\d+_rhp_)", "", name)

    # Standardize multiple underscores
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")

    return name


def _jaccard_tokens(a: str, b: str) -> float:
    """Jaccard similarity on underscore-split tokens."""
    tokens_a = set(a.split("_"))
    tokens_b = set(b.split("_"))
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def _prefix_match(a: str, b: str) -> float:
    """Common prefix ratio."""
    min_len = min(len(a), len(b))
    if min_len == 0:
        return 0.0
    common = 0
    for i in range(min_len):
        if a[i] == b[i]:
            common += 1
        else:
            break
    return common / max(len(a), len(b))


def _compute_similarity(oracle_name: str, catalog_name: str) -> float:
    """
    Compute weighted similarity score.
    final_score = 0.5 * levenshtein + 0.3 * jaccard + 0.2 * prefix_match
    """
    lev = levenshtein_ratio(oracle_name, catalog_name)
    jac = _jaccard_tokens(oracle_name, catalog_name)
    pfx = _prefix_match(oracle_name, catalog_name)

    return (
        SIMILARITY_WEIGHTS["levenshtein"] * lev
        + SIMILARITY_WEIGHTS["jaccard"] * jac
        + SIMILARITY_WEIGHTS["prefix"] * pfx
    )


def map_tables(oracle_tables: List[str]) -> Dict[str, str]:
    """
    Map a list of Oracle table names to Databricks fully-qualified names.

    Returns dict: ORACLE_TABLE_NAME → databricks_fqn

    Raises TableMappingError if any table is ambiguous or unmapped.
    """
    catalog = _load_catalog_tables()
    mapping: Dict[str, str] = {}
    errors: List[str] = []

    for oracle_table in oracle_tables:
        normalized_oracle = _normalize_name(oracle_table)
        threshold = dynamic_threshold(len(normalized_oracle))

        # Check for exact match first
        if normalized_oracle in catalog:
            mapping[oracle_table.upper()] = catalog[normalized_oracle]
            continue

        # Similarity search
        candidates: List[Tuple[str, float, str]] = []  # (catalog_name, score, fqn)
        for cat_name, fqn in catalog.items():
            score = _compute_similarity(normalized_oracle, cat_name)
            if score >= threshold:
                candidates.append((cat_name, score, fqn))

        if len(candidates) == 1:
            # Unique match above threshold → ACCEPT
            mapping[oracle_table.upper()] = candidates[0][2]
            logger.debug(
                "Mapped %s → %s (score=%.3f)",
                oracle_table, candidates[0][2], candidates[0][1],
            )
        elif len(candidates) > 1:
            # Multiple matches → AMBIGUOUS → PENDING
            candidates.sort(key=lambda x: -x[1])
            top_score = candidates[0][1]
            second_score = candidates[1][1]
            # If top candidate is significantly better (>0.05 gap), accept it
            if top_score - second_score > 0.05:
                mapping[oracle_table.upper()] = candidates[0][2]
                logger.debug(
                    "Mapped %s → %s (score=%.3f, runner-up=%.3f)",
                    oracle_table, candidates[0][2], top_score, second_score,
                )
            else:
                errors.append(
                    f"Ambiguous mapping for {oracle_table}: "
                    f"{[(c[0], f'{c[1]:.3f}') for c in candidates[:3]]}"
                )
        else:
            # No match
            errors.append(f"No mapping found for {oracle_table} (threshold={threshold:.2f})")

    if errors:
        raise TableMappingError("; ".join(errors))

    return mapping


def reset_cache():
    """Reset the catalog cache (useful for testing)."""
    global _catalog_cache
    with _cache_lock:
        _catalog_cache = None
