"""
Validate Oracle source vs Databricks silver_business view row counts.
Uses oracledb thin mode (no Oracle Client needed) + Databricks REST API for secrets.
"""
import os
import sys
import base64
import requests
import oracledb
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DBX_HOST  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DBX_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DBX_HTTP  = f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_SQL_WAREHOUSE_ID', '')}"

ORACLE_HOST    = "dbconnect.megaerp.online"
ORACLE_PORT    = 4221
ORACLE_SERVICE = "xepdb1"
SECRET_SCOPE   = "civilmaster-oracle"

# ── Helpers ───────────────────────────────────────────────────────

def get_dbx_secret(scope: str, key: str) -> str:
    """Fetch a secret value from Databricks Secrets REST API (returned as base64)."""
    r = requests.get(
        f"{DBX_HOST}/api/2.0/secrets/get",
        params={"scope": scope, "key": key},
        headers={"Authorization": f"Bearer {DBX_TOKEN}"},
        timeout=10,
    )
    r.raise_for_status()
    b64 = r.json()["value"]
    return base64.b64decode(b64).decode("utf-8")


def dbx_sql(query: str) -> list:
    """Execute SQL on Databricks SQL Warehouse and return rows."""
    r = requests.post(
        f"{DBX_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {DBX_TOKEN}"},
        json={
            "warehouse_id": os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
            "statement": query,
            "wait_timeout": "60s",
        },
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    if data["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"DBX SQL failed: {data['status']}")
    result = data.get("result", {})
    return result.get("data_array", [])


def oracle_count(conn, sql: str) -> int:
    """Wrap an Oracle SQL in SELECT COUNT(*) and return the count."""
    count_sql = f"SELECT COUNT(*) FROM ({sql})"
    with conn.cursor() as cur:
        cur.execute(count_sql)
        row = cur.fetchone()
        return row[0] if row else -1


# ── Main ──────────────────────────────────────────────────────────

def main():
    env = sys.argv[1].upper() if len(sys.argv) > 1 else "HML"
    print(f"🔧 Ambiente Oracle: {env}")

    # 1. Fetch Oracle credentials from Databricks Secrets
    prefix = "PRD" if env == "PRD" else "HML"
    ora_user = get_dbx_secret(SECRET_SCOPE, f"{prefix}_MEGA_DB_USER")
    ora_pass = get_dbx_secret(SECRET_SCOPE, f"{prefix}_MEGA_DB_SENHA")
    print(f"✅ Credenciais Oracle obtidas do Databricks Secrets (scope={SECRET_SCOPE})")

    # 2. Connect to Oracle (thin mode – no client needed)
    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    print(f"🔗 Conectando ao Oracle: {dsn}")
    conn = oracledb.connect(user=ora_user, password=ora_pass, dsn=dsn)
    print("✅ Conexão Oracle estabelecida")

    # ── CM_AFASTAMENTOS: original Oracle SQL ──────────────────────
    oracle_sql = """
SELECT
    R034FUN.NUMCAD AS MATRICULA,
    R018CCU.CODCCU AS ID,
    R018CCU.NOMCCU AS "CC NOME",
    R034FUN.NOMFUN AS NOME,
    R034FUN.NUMCPF AS CPF,
    R024CAR.TITCAR AS CARGO,
    R010SIT.DESSIT AS STATUS,
    R034FUN.SITAFA AS SITUACAO,
    MAX(R038AFA.DATAFA) AS "DATA INICIAL",
    MAX(R038AFA.DATTER) AS "DATA TERMINO"
FROM
    CIVIL_10465_RHP.R034FUN
LEFT JOIN CIVIL_10465_RHP.R018CCU
  ON R034FUN.NUMEMP = R018CCU.NUMEMP
  AND R034FUN.CODCCU = R018CCU.CODCCU
LEFT JOIN CIVIL_10465_RHP.R010SIT
  ON R034FUN.SITAFA = R010SIT.CODSIT
LEFT JOIN CIVIL_10465_RHP.R038AFA
  ON R038AFA.NUMEMP = R034FUN.NUMEMP
  AND R038AFA.NUMCAD = R034FUN.NUMCAD
LEFT JOIN CIVIL_10465_RHP.R024CAR
  ON R024CAR.ESTCAR = R034FUN.ESTCAR
  AND R024CAR.CODCAR = R034FUN.CODCAR
WHERE
    R034FUN.TIPCOL = 1
    AND R034FUN.DATADM > TO_DATE('01/01/1990', 'DD/MM/YYYY')
    AND R034FUN.SITAFA != 1
GROUP BY
    R034FUN.NUMCPF,
    R034FUN.NUMCAD,
    R034FUN.NOMFUN,
    R018CCU.CODCCU,
    R018CCU.NOMCCU,
    R010SIT.DESSIT,
    R024CAR.TITCAR,
    R034FUN.SITAFA
ORDER BY
    MATRICULA
"""

    # 3. Count rows on Oracle
    print("\n📊 Executando query no Oracle...")
    ora_count = oracle_count(conn, oracle_sql)
    print(f"   Oracle  CM_AFASTAMENTOS → {ora_count:,} linhas")

    conn.close()

    # 4. Count rows on Databricks
    print("📊 Executando query no Databricks...")
    dbx_rows = dbx_sql("SELECT COUNT(*) FROM cm_dbx_dev.silver_business.cm_afastamentos")
    dbx_count = int(dbx_rows[0][0]) if dbx_rows else -1
    print(f"   Databricks cm_afastamentos → {dbx_count:,} linhas")

    # 5. Compare
    print("\n" + "=" * 60)
    diff = ora_count - dbx_count
    pct = (diff / ora_count * 100) if ora_count > 0 else 0
    if diff == 0:
        print("✅ VALIDAÇÃO OK — contagem idêntica!")
    else:
        print(f"⚠️  DIFERENÇA: {diff:+,} linhas ({pct:+.2f}%)")
        print(f"   Oracle:     {ora_count:,}")
        print(f"   Databricks: {dbx_count:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
