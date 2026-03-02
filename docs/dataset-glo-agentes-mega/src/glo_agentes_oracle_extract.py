# src/glo_agentes_oracle_extract.py
# Example extraction snippet (to be used inside orchestrator Bronze step)
#
# Notes:
# - Do NOT hardcode credentials. Read from secret scope or env vars.
# - Depending on your network/topology, you may query with DBLINK:
#     SELECT * FROM CMASTER.GLO_AGENTES@CMASTERPRD
#   or directly (without @DBLINK) if connecting to the source instance.

from typing import Dict

def build_oracle_query(owner: str, table: str, dblink: str | None, watermark_col: str, last_watermark: str | None):
    base = f"{owner}.{table}"
    if dblink:
        base = f"{base}@{dblink}"

    where = ""
    if last_watermark:
        # Oracle timestamp literal example: TO_TIMESTAMP('2025-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
        where = f" WHERE {watermark_col} > TO_TIMESTAMP('{last_watermark}','YYYY-MM-DD HH24:MI:SS')"

    return f"SELECT * FROM {base}{where}"

def jdbc_options(host: str, port: int, service_name: str, user: str, password: str) -> Dict[str, str]:
    url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    return {
        "url": url,
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.OracleDriver",
        # Recommended for larger tables (tune as needed)
        "fetchsize": "10000",
    }
