"""
Extract Oracle view definitions from Views.xlsx.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

import openpyxl

from migration_engine.config import VIEWS_XLSX_PATH

logger = logging.getLogger(__name__)


@dataclass
class ViewRecord:
    """Raw view record extracted from spreadsheet."""
    view_name: str
    original_sql: str
    department: Optional[str] = None
    in_use: Optional[str] = None
    purpose: Optional[str] = None


def _normalize_sql(raw: str) -> str:
    """
    Clean up Excel-stored SQL:
    - Replace _x000D_\\n with real newline
    - Strip trailing whitespace
    """
    if not raw:
        return ""
    sql = raw.replace("_x000D_\n", "\n")
    sql = re.sub(r"\r\n?", "\n", sql)
    sql = sql.strip()
    # Remove trailing semicolons (Oracle style, not valid in Spark)
    sql = sql.rstrip(";").strip()
    return sql


def extract_views(xlsx_path: Optional[str] = None) -> List[ViewRecord]:
    """
    Read all views from the spreadsheet.

    Expected columns (by position):
      0: DEPARTAMENTO
      1: EM USO
      2: FINALIDADE
      3: ROTINA_PLANILHA
      4: OBJ_PRINCIPAL
      5: View (name)
      6: (unused)
      7: (unused)
      8: Query (SQL)
    """
    path = xlsx_path or VIEWS_XLSX_PATH
    logger.info("Loading views from %s", path)

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active

    views: List[ViewRecord] = []
    skipped = 0

    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        # Ensure enough columns
        cells = list(row) + [None] * 9
        view_name = cells[5]
        raw_sql = cells[8]

        if not view_name or not raw_sql:
            skipped += 1
            continue

        view_name = str(view_name).strip().upper()
        sql = _normalize_sql(str(raw_sql))

        if not sql:
            skipped += 1
            continue

        views.append(
            ViewRecord(
                view_name=view_name,
                original_sql=sql,
                department=str(cells[0]).strip() if cells[0] else None,
                in_use=str(cells[1]).strip() if cells[1] else None,
                purpose=str(cells[2]).strip() if cells[2] else None,
            )
        )

    wb.close()
    logger.info("Extracted %d views (%d skipped)", len(views), skipped)
    return views
