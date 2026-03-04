"""
Validate CM_AFASTAMENTOS: Oracle source vs Databricks silver_business.
Submits Oracle JDBC query via Databricks cluster execution context API.
"""
import os
import sys
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

HOST  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "")
WH_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def create_context():
    r = requests.post(
        f"{HOST}/api/1.2/contexts/create",
        headers=HEADERS,
        json={"clusterId": CLUSTER_ID, "language": "python"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["id"]


def run_command(ctx_id: str, code: str, timeout_sec: int = 300):
    """Submit code to cluster and wait for result."""
    r = requests.post(
        f"{HOST}/api/1.2/commands/execute",
        headers=HEADERS,
        json={"clusterId": CLUSTER_ID, "contextId": ctx_id, "language": "python", "command": code},
        timeout=60,
    )
    r.raise_for_status()
    cmd_id = r.json()["id"]

    # Poll for completion
    start = time.time()
    while time.time() - start < timeout_sec:
        r = requests.get(
            f"{HOST}/api/1.2/commands/status",
            headers=HEADERS,
            params={"clusterId": CLUSTER_ID, "contextId": ctx_id, "commandId": cmd_id},
            timeout=30,
        )
        r.raise_for_status()
        status = r.json()
        if status["status"] in ("Finished", "Error", "Cancelled"):
            return status
        time.sleep(3)
    raise TimeoutError("Command did not finish in time")


def destroy_context(ctx_id: str):
    requests.post(
        f"{HOST}/api/1.2/contexts/destroy",
        headers=HEADERS,
        json={"clusterId": CLUSTER_ID, "contextId": ctx_id},
        timeout=10,
    )


def dbx_sql(query: str) -> list:
    """Execute SQL on Databricks SQL Warehouse."""
    r = requests.post(
        f"{HOST}/api/2.0/sql/statements",
        headers=HEADERS,
json={"warehouse_id": WH_ID, "statement": query, "wait_timeout": "50s"},
        timeout=120,
    )
    if r.status_code != 200:
        print(f"      DBX SQL error {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    data = r.json()
    state = data.get("status", {}).get("state", "")
    if state != "SUCCEEDED":
        # might be PENDING - poll
        stmt_id = data.get("statement_id", "")
        for _ in range(60):
            time.sleep(2)
            pr = requests.get(f"{HOST}/api/2.0/sql/statements/{stmt_id}", headers=HEADERS, timeout=30)
            pd = pr.json()
            state = pd.get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                data = pd
                break
            elif state in ("FAILED", "CANCELED"):
                raise RuntimeError(f"DBX SQL failed: {pd['status']}")
        else:
            raise RuntimeError("DBX SQL timed out")
    return data.get("result", {}).get("data_array", [])


# ── Oracle query code to run on cluster ───────────────────────────
def build_oracle_code(env: str) -> str:
    dblink = 'CMASTERPRD' if env == 'PRD' else 'CMASTER'
    prefix = 'PRD' if env == 'PRD' else 'HML'
    return '''SCOPE = "civilmaster-oracle"
ora_user = dbutils.secrets.get(SCOPE, "''' + prefix + '''_MEGA_DB_USER")
ora_pass = dbutils.secrets.get(SCOPE, "''' + prefix + '''_MEGA_DB_SENHA")

JDBC_URL = "jdbc:oracle:thin:@//dbconnect.megaerp.online:4221/xepdb1"
DBLINK = "''' + dblink + '''"

oracle_query = """(
SELECT COUNT(*) AS cnt FROM (
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
        CIVIL_10465_RHP.R034FUN@''' + dblink + '''
    LEFT JOIN CIVIL_10465_RHP.R018CCU@''' + dblink + '''
      ON R034FUN.NUMEMP = R018CCU.NUMEMP
      AND R034FUN.CODCCU = R018CCU.CODCCU
    LEFT JOIN CIVIL_10465_RHP.R010SIT@''' + dblink + '''
      ON R034FUN.SITAFA = R010SIT.CODSIT
    LEFT JOIN CIVIL_10465_RHP.R038AFA@''' + dblink + '''
      ON R038AFA.NUMEMP = R034FUN.NUMEMP
      AND R038AFA.NUMCAD = R034FUN.NUMCAD
    LEFT JOIN CIVIL_10465_RHP.R024CAR@''' + dblink + '''
      ON R024CAR.ESTCAR = R034FUN.ESTCAR
      AND R024CAR.CODCAR = R034FUN.CODCAR
    WHERE
        R034FUN.TIPCOL = 1
        AND R034FUN.DATADM > TO_DATE(\'01/01/1990\', \'DD/MM/YYYY\')
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
)) src"""

df = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", oracle_query)
    .option("user", ora_user)
    .option("password", ora_pass)
    .option("driver", "oracle.jdbc.OracleDriver")
    .load()
)

result = df.collect()
count = result[0][0]
print("ORACLE_COUNT=" + str(count))
'''


def main():
    env = sys.argv[1].upper() if len(sys.argv) > 1 else "HML"
    print(f"=== Validacao Oracle ({env}) vs Databricks: CM_AFASTAMENTOS ===\n")

    # 1. Query Databricks view count
    print("[1/3] Contando linhas no Databricks silver_business.cm_afastamentos ...")
    dbx_rows = dbx_sql("SELECT COUNT(*) FROM cm_dbx_dev.silver_business.cm_afastamentos")
    dbx_count = int(dbx_rows[0][0]) if dbx_rows else -1
    print(f"      Databricks: {dbx_count:,} linhas\n")

    # 2. Create execution context on cluster
    print(f"[2/3] Criando contexto no cluster {CLUSTER_ID} ...")
    ctx_id = create_context()
    print(f"      Context ID: {ctx_id}")

    try:
        # 3. Run Oracle JDBC count on cluster
        code = build_oracle_code(env)
        print(f"      Executando query Oracle via JDBC no cluster ...\n")
        result = run_command(ctx_id, code, timeout_sec=300)

        if result["status"] == "Finished":
            results_obj = result.get("results", {})
            result_type = results_obj.get("resultType", "")
            output = results_obj.get("data", "")
            cause = results_obj.get("cause", "")
            summary = results_obj.get("summary", "")
            print(f"      resultType: {result_type}")
            if summary:
                print(f"      summary: {summary}")
            if cause:
                # Write full cause to file for inspection
                with open('_oracle_error.txt', 'w', encoding='utf-8') as f:
                    f.write(cause)
                # Print last portion which usually has the actual error
                lines = cause.strip().split('\n')
                print(f"      cause (last 15 lines):")
                for ln in lines[-15:]:
                    print(f"        {ln}")
            print(f"      Cluster output:\n      {output}\n")

            # Parse oracle count from output
            ora_count = -1
            for line in output.split("\n"):
                if "ORACLE_COUNT=" in line:
                    ora_count = int(float(line.split("=")[1].strip()))
                    break

            if ora_count < 0:
                print("      ERRO: nao foi possivel extrair o count do Oracle")
                return

            # 4. Compare
            print(f"[3/3] Comparacao:")
            print(f"      Oracle ({env}):  {ora_count:,} linhas")
            print(f"      Databricks:      {dbx_count:,} linhas")
            print()
            diff = ora_count - dbx_count
            pct = (diff / ora_count * 100) if ora_count > 0 else 0
            if diff == 0:
                print("      VALIDACAO OK - contagem identica!")
            else:
                print(f"      DIFERENCA: {diff:+,} linhas ({pct:+.2f}%)")
        else:
            err = result.get("results", {}).get("cause", "unknown")
            print(f"      ERRO no cluster: {err[:500]}")
    finally:
        destroy_context(ctx_id)
        print("\n      Contexto destruido.")


if __name__ == "__main__":
    main()
