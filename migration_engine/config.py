"""
Migration Engine Configuration.
Loads settings from environment variables and defines constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

# ── Databricks Connection ──────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = os.getenv(
    "DATABRICKS_HTTP_PATH",
    f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_SQL_WAREHOUSE_ID', '')}",
)

# ── Unity Catalog Paths ───────────────────────────────────────────
UC_CATALOG = os.getenv("UC_CATALOG", "cm_dbx_dev")

# Source schemas where bronze tables live
SOURCE_SCHEMAS = [
    f"{UC_CATALOG}.bronze",
    f"{UC_CATALOG}.bronze_mega",
    f"{UC_CATALOG}.silver_mega",
]

# Target schema for migrated views
TARGET_SCHEMA = f"{UC_CATALOG}.silver_business"

# Control schema for migration metadata
CONTROL_SCHEMA = f"{UC_CATALOG}.migration_control"

# ── Views Source ──────────────────────────────────────────────────
VIEWS_XLSX_PATH = str(_PROJECT_ROOT / "Views.xlsx")

# ── Parallelism ──────────────────────────────────────────────────
MAX_WORKERS_ANALYSIS = int(os.getenv("MAX_WORKERS_ANALYSIS", "16"))
MAX_WORKERS_MIGRATION = int(os.getenv("MAX_WORKERS_MIGRATION", "5"))

# ── Batching ─────────────────────────────────────────────────────
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_PAUSE_SECONDS = float(os.getenv("BATCH_PAUSE_SECONDS", "2.0"))

# ── Retry / Dead-letter ─────────────────────────────────────────
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# ── Table Mapping Thresholds ─────────────────────────────────────
SIMILARITY_WEIGHTS = {
    "levenshtein": 0.5,
    "jaccard": 0.3,
    "prefix": 0.2,
}

def dynamic_threshold(name_length: int) -> float:
    """Return minimum similarity score based on table name length."""
    if name_length < 10:
        return 0.95
    elif name_length <= 25:
        return 0.90
    else:
        return 0.85

# ── Scoring Thresholds ───────────────────────────────────────────
# Structural score → complexity_level
STRUCTURAL_LEVEL_THRESHOLDS = [
    (5, 1),   # score <= 5  → level 1
    (10, 2),  # score <= 10 → level 2
    (18, 3),  # score <= 18 → level 3
    (30, 4),  # score <= 30 → level 4
]
# anything above → level 5

# Risk score → risk_level
RISK_LEVEL_THRESHOLDS = {
    0: "LOW",        # score == 0
    5: "MEDIUM",     # score <= 5
    10: "HIGH",      # score <= 10
}
# anything above → CRITICAL

# ── Eligibility ──────────────────────────────────────────────────
MAX_ELIGIBLE_COMPLEXITY = 3
ELIGIBLE_RISK_LEVEL = "LOW"

# ── Stages ───────────────────────────────────────────────────────
STAGES = [
    "EXTRACT",
    "PARSE",
    "CLASSIFY",
    "DEPENDENCY_RESOLUTION",
    "MAP_TABLES",
    "REWRITE_SQL",
    "COLUMN_VALIDATION",
    "VALIDATE_EXPLAIN",
    "VALIDATE_SAMPLE",
    "TEMP_VIEW_CREATE",
    "PROMOTE_VIEW",
    "FINALIZE",
]

# ── Final Statuses ───────────────────────────────────────────────
class Status:
    MIGRATED_SUCCESS = "MIGRATED_SUCCESS"
    MIGRATED_WITH_WARNING = "MIGRATED_WITH_WARNING"
    PENDING_REVIEW = "PENDING_REVIEW"
    FAILED_CONVERSION = "FAILED_CONVERSION"
    PARSER_FAILED = "PARSER_FAILED"
    DEAD_LETTER = "DEAD_LETTER"
