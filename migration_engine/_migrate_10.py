"""
Targeted migration of 10 views whose source tables ALL exist in Databricks.
Bypasses the full pipeline — directly transpiles and creates views.
"""
import sys, json, re, time, logging

sys.path.insert(0, "C:/dev/cm-databricks")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

import sqlglot
from migration_engine.extractor.xlsx_extractor import extract_views
from migration_engine.sanitizer.sql_sanitizer import SQLSanitizer
from migration_engine.connectors.databricks_connector import DatabricksConnector

sanitizer = SQLSanitizer()

# ── Table mapping: Oracle name → Databricks FQN ──────────────────
# Based on actual tables in bronze/bronze_mega/silver_mega
TABLE_MAP = {
    # CIVIL_10465_RHP tables (for CM_AFASTAMENTOS)
    "R034FUN":                    "cm_dbx_dev.bronze.r034fun",
    "R018CCU":                    "cm_dbx_dev.bronze.r018ccu",
    "R010SIT":                    "cm_dbx_dev.bronze.r010sit",
    "R038AFA":                    "cm_dbx_dev.bronze.r038afa",
    "R024CAR":                    "cm_dbx_dev.bronze.r024car",
    "CIVIL_10465_RHP.R034FUN":    "cm_dbx_dev.bronze_mega.civil_10465_rhp_r034fun",
    "CIVIL_10465_RHP.R018CCU":    "cm_dbx_dev.bronze_mega.civil_10465_rhp_r018ccu",
    "CIVIL_10465_RHP.R010SIT":    "cm_dbx_dev.bronze_mega.civil_10465_rhp_r010sit",
    "CIVIL_10465_RHP.R038AFA":    "cm_dbx_dev.bronze_mega.civil_10465_rhp_r038afa",
    "CIVIL_10465_RHP.R024CAR":    "cm_dbx_dev.bronze_mega.civil_10465_rhp_r024car",
    # Core CMASTER tables
    "CON_VW_RAZAO_CCPRO":        "cm_dbx_dev.bronze.con_vw_razao_ccpro",
    "CON_VW_PLANO_CONTA":        "cm_dbx_dev.bronze.con_vw_plano_conta",
    "CON_CENTRO_CUSTO":           "cm_dbx_dev.bronze.con_centro_custo",
    "CON_PLANO_CONTA":            "cm_dbx_dev.bronze.con_plano_conta",
    "GLO_AGENTES":                "cm_dbx_dev.bronze.glo_agentes",
    "GLO_PROJETOS":               "cm_dbx_dev.bronze.glo_projetos",
    "GLO_GRUPO_USUARIO":          "cm_dbx_dev.bronze.glo_grupo_usuario",
    "FRO_OS":                     "cm_dbx_dev.bronze.fro_os",
    "FRO_DESPESAS":               "cm_dbx_dev.bronze.fro_despesas",
    "EST_PRODUTOS":               "cm_dbx_dev.bronze.est_produtos",
    "EST_PEDCOMPRAS":             "cm_dbx_dev.bronze.est_pedcompras",
    "FIN_CONTASCLASSES":          "cm_dbx_dev.bronze.fin_contasclasses",
    "VWCMBENEFICIO":              "cm_dbx_dev.bronze.vwcmbeneficio",
    "CMVW_GERDER_TOTAL":          "cm_dbx_dev.bronze.cmvw_gerder_total",
    "CMVW_GERDER_CURRENTY1":      "cm_dbx_dev.bronze.cmvw_gerder_currenty1",
    # With schema prefix
    "CMASTER.CON_VW_RAZAO_CCPRO": "cm_dbx_dev.bronze.con_vw_razao_ccpro",
    "CMASTER.CON_VW_PLANO_CONTA": "cm_dbx_dev.bronze.con_vw_plano_conta",
    "CMASTER.GLO_AGENTES":        "cm_dbx_dev.bronze.glo_agentes",
    "CMASTER.GLO_PROJETOS":       "cm_dbx_dev.bronze.glo_projetos",
    "CMASTER.VWCMBENEFICIO":      "cm_dbx_dev.bronze.vwcmbeneficio",
    "CMASTER.CMVW_GERDER_TOTAL":  "cm_dbx_dev.bronze.cmvw_gerder_total",
    "CMASTER.FRO_OS":             "cm_dbx_dev.bronze.fro_os",
    "CMASTER.FRO_DESPESAS":       "cm_dbx_dev.bronze.fro_despesas",
}

# ── Target views to migrate ──────────────────────────────────────
TARGET_VIEWS = [
    "CM_AFASTAMENTOS",           # 5 tables: R034FUN, R018CCU, R010SIT, R038AFA, R024CAR
    "CM_VW_RAZAO_FORNECEDOR",    # 2 tables: CON_VW_RAZAO_CCPRO, GLO_AGENTES (already created)
    "CMVWGERDER_GH_2",           # 2 tables: CON_VW_RAZAO_CCPRO, CON_VW_PLANO_CONTA
    "CM_FRO_SERVINT",            # 2 tables: FRO_OS, FRO_DESPESAS
    "CM_CAC_VALIDACAO",          # 2 tables: VWCMBENEFICIO, GLO_PROJETOS
    "CM_USUARIOS_MEGA",          # 1 table:  GLO_GRUPO_USUARIO (already created)
    "CMVW_FILIAIS",              # 1 table:  GLO_AGENTES (already created)
    "CMGERDER_INFO",             # 1 table:  CMVW_GERDER_TOTAL
    "CMVWGERDER_8_TEST38",       # 2 tables: VWCMBENEFICIO, GLO_PROJETOS
    "CVWGERDER_8_P43",           # 2 tables: VWCMBENEFICIO, GLO_PROJETOS
    "CMVWGERDER_8_TEST21",       # 1 table:  GLO_PROJETOS
    "CMVWGERDER_8_TEST27",       # 1 table:  GLO_PROJETOS
    "CMVWGERDER_8_TEST36",       # 1 table:  GLO_PROJETOS
    "CMVWGERDER_LOG",            # 1 table:  CMVW_GERDER_TOTAL
]

TARGET_SCHEMA = "cm_dbx_dev.silver_business"


def replace_table_names(sql: str, table_map: dict) -> str:
    """
    Replace Oracle table names with Databricks FQN in SQL string.
    Uses placeholder approach to avoid double-replacement.
    """
    result = sql
    placeholders = {}

    # Phase 1: Replace Oracle names with unique placeholders (longest first)
    for i, (oracle_name, dbx_fqn) in enumerate(
        sorted(table_map.items(), key=lambda x: -len(x[0]))
    ):
        ph = f"__TBL_PH_{i}__"
        placeholders[ph] = dbx_fqn
        pattern = re.compile(r"\b" + re.escape(oracle_name) + r"\b", re.IGNORECASE)
        result = pattern.sub(ph, result)

    # Phase 2: Replace placeholders with actual FQN
    for ph, fqn in placeholders.items():
        result = result.replace(ph, fqn)

    return result


def fix_spark_compat(sql: str) -> str:
    """Fix Spark-specific compatibility issues after transpilation."""
    # Oracle TRUNC(date) → Spark TRUNC(date, 'DD')
    # Match TRUNC(expr) with only 1 arg (no comma inside)
    sql = re.sub(
        r"\bTRUNC\(([^,()]+)\)",
        r"TRUNC(\1, 'DD')",
        sql,
        flags=re.IGNORECASE,
    )
    # Oracle SYSDATE → Spark CURRENT_DATE()
    sql = re.sub(r"\bSYSDATE\b", "CURRENT_DATE()", sql, flags=re.IGNORECASE)
    return sql


def transpile_and_map(original_sql: str, view_name: str) -> str:
    """Sanitize, transpile Oracle→Spark, and replace table names."""
    # 1. Sanitize
    san = sanitizer.sanitize(original_sql)
    sql = san.sanitized_sql

    # 2. Transpile via sqlglot
    transpiled = sqlglot.transpile(
        sql, read="oracle", write="databricks",
        error_level=sqlglot.ErrorLevel.WARN,
    )
    if not transpiled:
        raise ValueError(f"Transpilation returned empty for {view_name}")

    spark_sql = transpiled[0]

    # 3. Replace table names
    spark_sql = replace_table_names(spark_sql, TABLE_MAP)

    # 4. Fix Spark compatibility
    spark_sql = fix_spark_compat(spark_sql)

    return spark_sql


def main():
    # Load all views from Excel
    all_views = extract_views()
    view_dict = {v.view_name.upper(): v for v in all_views}

    results = {"success": [], "failed": []}

    for view_name in TARGET_VIEWS:
        view = view_dict.get(view_name.upper())
        if not view:
            logger.error("[%s] Not found in Excel", view_name)
            results["failed"].append((view_name, "Not found in Excel"))
            continue

        logger.info("=" * 60)
        logger.info("[%s] Processing...", view_name)

        try:
            # Transpile and map
            spark_sql = transpile_and_map(view.original_sql, view_name)
            logger.info("[%s] Transpiled OK (%d chars)", view_name, len(spark_sql))

            # Validate with EXPLAIN
            try:
                DatabricksConnector.explain(spark_sql)
                logger.info("[%s] EXPLAIN OK", view_name)
            except Exception as e:
                logger.warning("[%s] EXPLAIN failed: %s", view_name, str(e)[:200])
                # Still try to create — some EXPLAIN failures are benign

            # Create the view
            fqn = f"{TARGET_SCHEMA}.{view_name}".lower()
            ddl = f"CREATE OR REPLACE VIEW {fqn} AS {spark_sql}"
            DatabricksConnector.execute_write(ddl)
            logger.info("[%s] ✓ VIEW CREATED: %s", view_name, fqn)
            results["success"].append(view_name)

        except Exception as e:
            logger.error("[%s] ✗ FAILED: %s", view_name, str(e)[:300])
            results["failed"].append((view_name, str(e)[:200]))

        # Small pause to avoid throttling
        time.sleep(1)

    # Summary
    print("\n" + "=" * 60)
    print(f"  MIGRATION RESULTS")
    print(f"  Success: {len(results['success'])}")
    print(f"  Failed:  {len(results['failed'])}")
    print("=" * 60)

    if results["success"]:
        print("\n  ✓ Created views:")
        for vn in results["success"]:
            print(f"    {TARGET_SCHEMA}.{vn.lower()}")

    if results["failed"]:
        print("\n  ✗ Failed views:")
        for vn, err in results["failed"]:
            print(f"    {vn}: {err[:100]}")


if __name__ == "__main__":
    main()
