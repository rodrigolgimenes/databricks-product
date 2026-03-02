from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
import pandas as pd


DATA_DIR = Path(__file__).parent / "data"
CONFIGS_PATH = DATA_DIR / "load_configs.jsonl"


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            # Ignore malformed lines to keep the demo app resilient
            continue
    return rows


def _append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _get_databricks_connection():
    """
    Get Databricks SQL connection using environment variables.
    Returns None if not configured (allows app to work in demo mode).
    """
    try:
        from databricks import sql
        
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        access_token = os.getenv("DATABRICKS_TOKEN")
        
        if not all([server_hostname, http_path, access_token]):
            return None
            
        return sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )
    except ImportError:
        return None
    except Exception as e:
        st.error(f"Erro ao conectar ao Databricks: {e}")
        return None


def _query_databricks(query: str) -> Optional[pd.DataFrame]:
    """Execute a query against Databricks and return DataFrame."""
    conn = _get_databricks_connection()
    if not conn:
        return None
        
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        st.error(f"Erro na query: {e}")
        return None
    finally:
        conn.close()


def _format_status_badge(status: str) -> str:
    """Return emoji badge for status."""
    status_upper = str(status).upper()
    badges = {
        "SUCCEEDED": "✅",
        "RUNNING": "🔵",
        "FAILED": "❌",
        "RETRY": "🔄",
        "PENDING": "🟡",
        "CANCELLED": "⚪",
        "SKIPPED": "⏭️",
    }
    return badges.get(status_upper, "❓") + " " + status_upper


st.set_page_config(
    page_title=os.getenv("APP_TITLE", "Data Load Tools"),
    layout="wide",
)

st.title(os.getenv("APP_TITLE", "Data Load Tools"))
st.caption("App simples para parametrizar cargas e validar o deploy no Databricks Apps.")

# Check Databricks connection
CATALOG = os.getenv("DATABRICKS_CATALOG", "cm_dbx_dev")
db_connected = _get_databricks_connection() is not None

with st.sidebar:
    st.subheader("Ambiente")
    st.write(
        {
            "app": os.getenv("DATABRICKS_APP_NAME"),
            "workspace_host": os.getenv("DATABRICKS_HOST"),
            "workspace_id": os.getenv("DATABRICKS_WORKSPACE_ID"),
            "catalog": CATALOG,
            "env": os.getenv("APP_ENV", "dev"),
        }
    )
    
    if db_connected:
        st.success("✅ Databricks conectado")
    else:
        st.warning("⚠️ Databricks não configurado (modo demo)")

# Tabs for different sections
tab1, tab2 = st.tabs(["📥 Configuração de Cargas", "📈 Monitoramento"])

with tab1:
    st.markdown("### Parâmetros da carga")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        with st.form("load_form", clear_on_submit=False):
        nome = st.text_input("Nome da carga", placeholder="ex: clientes_diario")
        tipo = st.selectbox("Tipo", ["FULL", "INCREMENTAL"], index=0)

        fonte_tipo = st.selectbox("Fonte", ["Tabela (Unity Catalog)", "Arquivo/Path", "Query"], index=0)
        fonte_valor = st.text_input(
            "Valor da fonte",
            placeholder="ex: catalogo.schema.tabela  |  /Volumes/...  |  SELECT ...",
        )

        destino = st.text_input("Destino (cat.schema.tabela)", placeholder="ex: bronze.crm.clientes")
        write_mode = st.selectbox("Write mode", ["append", "overwrite"], index=0)

        st.markdown("#### Incremental (opcional)")
        inc_col = st.text_input("Coluna incremental", placeholder="ex: updated_at")
        usar_datas = st.checkbox("Filtrar por datas", value=False)
        if usar_datas:
            d1, d2 = st.columns(2)
            with d1:
                dt_ini = st.date_input("Data início", value=date.today())
            with d2:
                dt_fim = st.date_input("Data fim", value=date.today())
        else:
            dt_ini = None
            dt_fim = None

        batch_size = st.number_input("Batch size", min_value=1, value=1000, step=100)
        dry_run = st.checkbox("Dry-run (não executa, só valida)", value=True)

        submitted = st.form_submit_button("Salvar configuração")

    cfg = {
        "name": nome.strip(),
        "type": tipo,
        "source": {"kind": fonte_tipo, "value": fonte_valor.strip()},
        "target": destino.strip(),
        "write_mode": write_mode,
        "incremental": {
            "column": inc_col.strip(),
            "start_date": dt_ini.isoformat() if isinstance(dt_ini, date) else None,
            "end_date": dt_fim.isoformat() if isinstance(dt_fim, date) else None,
        },
        "batch_size": int(batch_size),
        "dry_run": bool(dry_run),
        "created_at": _now_iso(),
    }

    if submitted:
        errors: list[str] = []
        if not cfg["name"]:
            errors.append("Informe o Nome da carga.")
        if not cfg["source"]["value"]:
            errors.append("Informe o Valor da fonte.")
        if not cfg["target"]:
            errors.append("Informe o Destino.")
        if cfg["type"] == "INCREMENTAL" and not cfg["incremental"]["column"]:
            errors.append("Para INCREMENTAL, informe a Coluna incremental.")

        if errors:
            st.error("\n".join(errors))
        else:
            _append_jsonl(CONFIGS_PATH, cfg)
            st.success("Configuração salva (demo).")

    st.markdown("### Preview")
    st.json(cfg)

with col2:
    st.markdown("### Configurações salvas")
    saved = _read_jsonl(CONFIGS_PATH)
    if not saved:
        st.info("Nenhuma configuração salva ainda.")
    else:
        st.dataframe(saved, use_container_width=True)

    st.markdown("### Ações")
    if st.button("Limpar configurações salvas (demo)", type="secondary"):
        if CONFIGS_PATH.exists():
            CONFIGS_PATH.unlink()
        st.rerun()
