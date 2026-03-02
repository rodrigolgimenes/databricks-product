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


# ===== PAGE CONFIG =====
st.set_page_config(
    page_title=os.getenv("APP_TITLE", "Data Load Tools"),
    layout="wide",
)

st.title(os.getenv("APP_TITLE", "Data Load Tools - Monitoring"))
st.caption("Monitoramento de execuções do orchestrator e configuração de cargas.")

# Check Databricks connection
CATALOG = os.getenv("DATABRICKS_CATALOG", "cm_dbx_dev")
OPS_SCHEMA = f"{CATALOG}.ingestion_sys_ops"
CTRL_SCHEMA = f"{CATALOG}.ingestion_sys_ctrl"

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

# ===== TABS =====
tab1, tab2, tab3 = st.tabs(["📈 Monitoramento", "📊 Histórico de Execuções", "📥 Configuração de Cargas"])

# ===== TAB 1: MONITORING OVERVIEW =====
with tab1:
    st.header("📈 Visão Geral do Monitoramento")
    
    if not db_connected:
        st.warning("⚠️ Configure as variáveis de ambiente do Databricks para visualizar dados reais:")
        st.code("""
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_CATALOG="cm_dbx_dev"
        """)
    else:
        # Queue status overview
        st.subheader("🗂️ Status da Fila (run_queue)")
        
        queue_query = f"""
        SELECT 
            status,
            COUNT(*) as count
        FROM {OPS_SCHEMA}.run_queue
        GROUP BY status
        ORDER BY status
        """
        
        queue_df = _query_databricks(queue_query)
        
        if queue_df is not None and not queue_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            for idx, row in queue_df.iterrows():
                status = row['status']
                count = row['count']
                
                if status == 'SUCCEEDED':
                    col1.metric("✅ Succeeded", count)
                elif status == 'FAILED':
                    col2.metric("❌ Failed", count)
                elif status == 'PENDING':
                    col3.metric("🟡 Pending", count)
                elif status == 'RUNNING':
                    col4.metric("🔵 Running", count)
            
            st.dataframe(queue_df, use_container_width=True)
        else:
            st.info("Nenhum dado na fila ainda.")
        
        # Recent executions
        st.subheader("🕒 Execuções Recentes")
        
        recent_query = f"""
        SELECT 
            bp.run_id,
            bp.dataset_id,
            dc.dataset_name,
            bp.status,
            bp.bronze_row_count,
            bp.silver_row_count,
            bp.started_at,
            bp.finished_at,
            TIMESTAMPDIFF(SECOND, bp.started_at, bp.finished_at) as duration_seconds,
            bp.error_class,
            bp.error_message
        FROM {OPS_SCHEMA}.batch_process bp
        LEFT JOIN {CTRL_SCHEMA}.dataset_control dc ON bp.dataset_id = dc.dataset_id
        ORDER BY bp.started_at DESC
        LIMIT 20
        """
        
        recent_df = _query_databricks(recent_query)
        
        if recent_df is not None and not recent_df.empty:
            # Format for display
            display_df = recent_df.copy()
            display_df['status_badge'] = display_df['status'].apply(_format_status_badge)
            display_df['started_at'] = pd.to_datetime(display_df['started_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            st.dataframe(
                display_df[['run_id', 'dataset_name', 'status_badge', 'bronze_row_count', 'silver_row_count', 'started_at', 'duration_seconds']],
                use_container_width=True,
                column_config={
                    "run_id": "Run ID",
                    "dataset_name": "Dataset",
                    "status_badge": "Status",
                    "bronze_row_count": st.column_config.NumberColumn("Bronze Rows", format="%d"),
                    "silver_row_count": st.column_config.NumberColumn("Silver Rows", format="%d"),
                    "started_at": "Iniciado em",
                    "duration_seconds": st.column_config.NumberColumn("Duração (s)", format="%d"),
                }
            )
        else:
            st.info("Nenhuma execução recente encontrada.")
        
        # Failed jobs with errors
        st.subheader("❌ Jobs com Falha")
        
        failed_query = f"""
        SELECT 
            rq.dataset_id,
            dc.dataset_name,
            rq.attempt,
            rq.max_retries,
            rq.last_error_class,
            rq.last_error_message,
            rq.requested_at,
            rq.next_retry_at
        FROM {OPS_SCHEMA}.run_queue rq
        LEFT JOIN {CTRL_SCHEMA}.dataset_control dc ON rq.dataset_id = dc.dataset_id
        WHERE rq.status = 'FAILED'
        ORDER BY rq.requested_at DESC
        LIMIT 10
        """
        
        failed_df = _query_databricks(failed_query)
        
        if failed_df is not None and not failed_df.empty:
            for idx, row in failed_df.iterrows():
                with st.expander(f"❌ {row['dataset_name'] or row['dataset_id']} - {row['last_error_class']}"):
                    st.write(f"**Dataset ID:** {row['dataset_id']}")
                    st.write(f"**Tentativas:** {row['attempt']}/{row['max_retries']}")
                    st.write(f"**Classe do Erro:** {row['last_error_class']}")
                    st.write(f"**Mensagem:**")
                    st.code(row['last_error_message'], language='text')
        else:
            st.success("✅ Nenhum job com falha!")

# ===== TAB 2: EXECUTION HISTORY WITH STEPS =====
with tab2:
    st.header("📊 Histórico de Execuções por Dataset")
    
    if not db_connected:
        st.warning("⚠️ Configure a conexão com Databricks para visualizar o histórico.")
    else:
        # Dataset selector
        datasets_query = f"""
        SELECT DISTINCT 
            dc.dataset_id,
            dc.dataset_name,
            dc.source_type,
            dc.execution_state
        FROM {CTRL_SCHEMA}.dataset_control dc
        ORDER BY dc.dataset_name
        """
        
        datasets_df = _query_databricks(datasets_query)
        
        if datasets_df is not None and not datasets_df.empty:
            dataset_options = {
                f"{row['dataset_name']} ({row['dataset_id']})": row['dataset_id']
                for _, row in datasets_df.iterrows()
            }
            
            selected_dataset_label = st.selectbox(
                "Selecione um Dataset:",
                options=list(dataset_options.keys())
            )
            
            selected_dataset_id = dataset_options[selected_dataset_label]
            
            st.write(f"**Dataset ID:** `{selected_dataset_id}`")
            
            # Get runs for this dataset
            runs_query = f"""
            SELECT 
                bp.run_id,
                bp.status,
                bp.started_at,
                bp.finished_at,
                bp.bronze_row_count,
                bp.silver_row_count,
                bp.error_class,
                bp.error_message,
                TIMESTAMPDIFF(SECOND, bp.started_at, bp.finished_at) as duration_seconds
            FROM {OPS_SCHEMA}.batch_process bp
            WHERE bp.dataset_id = '{selected_dataset_id}'
            ORDER BY bp.started_at DESC
            LIMIT 50
            """
            
            runs_df = _query_databricks(runs_query)
            
            if runs_df is not None and not runs_df.empty:
                st.subheader(f"📜 Execuções ({len(runs_df)} total)")
                
                for idx, run in runs_df.iterrows():
                    run_id = run['run_id']
                    status = run['status']
                    started = pd.to_datetime(run['started_at']).strftime('%Y-%m-%d %H:%M:%S')
                    duration = run['duration_seconds'] if pd.notna(run['duration_seconds']) else 'N/A'
                    
                    status_badge = _format_status_badge(status)
                    
                    with st.expander(f"{status_badge} | {started} | Run: {run_id[:8]}... | Duração: {duration}s"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Resumo da Execução:**")
                            st.write(f"- **Run ID:** `{run_id}`")
                            st.write(f"- **Status:** {status_badge}")
                            st.write(f"- **Iniciado:** {started}")
                            st.write(f"- **Duração:** {duration}s")
                            st.write(f"- **Bronze Rows:** {run['bronze_row_count'] or 'N/A'}")
                            st.write(f"- **Silver Rows:** {run['silver_row_count'] or 'N/A'}")
                        
                        with col2:
                            if pd.notna(run['error_message']):
                                st.write("**Erro:**")
                                st.write(f"- **Classe:** {run['error_class']}")
                                st.code(run['error_message'][:500], language='text')
                        
                        # Get steps for this run
                        steps_query = f"""
                        SELECT 
                            step_id,
                            phase,
                            step_key,
                            status,
                            message,
                            progress_current,
                            progress_total,
                            details_json,
                            started_at,
                            finished_at,
                            TIMESTAMPDIFF(SECOND, started_at, finished_at) as step_duration_seconds
                        FROM {OPS_SCHEMA}.batch_process_steps
                        WHERE run_id = '{run_id}'
                        ORDER BY started_at ASC
                        """
                        
                        steps_df = _query_databricks(steps_query)
                        
                        if steps_df is not None and not steps_df.empty:
                            st.write("---")
                            st.write("**📋 Etapas da Execução (Passo a Passo):**")
                            
                            for step_idx, step in steps_df.iterrows():
                                step_status_badge = _format_status_badge(step['status'])
                                step_started = pd.to_datetime(step['started_at']).strftime('%H:%M:%S') if pd.notna(step['started_at']) else 'N/A'
                                step_duration = step['step_duration_seconds'] if pd.notna(step['step_duration_seconds']) else 'N/A'
                                
                                progress_str = ""
                                if pd.notna(step['progress_current']) and pd.notna(step['progress_total']):
                                    progress_str = f" ({int(step['progress_current'])}/{int(step['progress_total'])})"
                                elif pd.notna(step['progress_current']):
                                    progress_str = f" ({int(step['progress_current'])})"
                                
                                st.write(f"{step_idx + 1}. **[{step['phase']}] {step['step_key']}** {step_status_badge}{progress_str}")
                                st.write(f"   - Início: {step_started} | Duração: {step_duration}s")
                                
                                if pd.notna(step['message']):
                                    st.write(f"   - Mensagem: {step['message']}")
                                
                                if pd.notna(step['details_json']):
                                    try:
                                        details = json.loads(step['details_json'])
                                        if details:
                                            st.json(details)
                                    except:
                                        pass
                        else:
                            st.info("ℹ️ Nenhuma etapa detalhada registrada para esta execução.")
            else:
                st.info("ℹ️ Nenhuma execução encontrada para este dataset.")
        else:
            st.warning("⚠️ Nenhum dataset encontrado no sistema.")

# ===== TAB 3: LOAD CONFIGURATION (ORIGINAL) =====
with tab3:
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
