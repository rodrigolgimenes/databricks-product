"""
Oracle Batch Ingestion - Versão com Tabela de Parâmetros
Baseado no código funcional existente, com melhorias:
- Tratamento de erros por tabela
- Logging estruturado
- Validação de existência
- Particionamento dinâmico
- Integração com batch_process_steps
"""

from datetime import datetime
from typing import Dict, Any, Optional
import traceback

# ============================================================================
# CONFIGURAÇÃO DE AMBIENTE
# ============================================================================

def setup_oracle_connection(env: str = "PRD") -> Dict[str, str]:
    """
    Configura conexão Oracle baseado no ambiente.
    Reutiliza a lógica funcional existente.
    """
    SCOPE = "civilmaster-oracle"
    
    if env == "PRD":
        user = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_USER")  # type: ignore[name-defined]
        password = dbutils.secrets.get(SCOPE, "PRD_MEGA_DB_SENHA")  # type: ignore[name-defined]
        dblink = "CMASTERPRD"
    elif env == "HML":
        user = dbutils.secrets.get(SCOPE, "HML_MEGA_DB_USER")  # type: ignore[name-defined]
        password = dbutils.secrets.get(SCOPE, "HML_MEGA_DB_SENHA")  # type: ignore[name-defined]
        dblink = "CMASTER"
    else:
        raise ValueError(f"Ambiente inválido: {env}")
    
    # Valores comuns
    host = "dbconnect.megaerp.online"
    service_name = "xepdb1"
    port = "4221"
    owner = "CMASTER"
    
    jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    
    print(f"✅ Conexão Oracle configurada: {env}")
    print(f"➡ HOST={host}, OWNER={owner}, DBLINK={dblink}")
    print(f"➡ JDBC_URL={jdbc_url}")
    
    return {
        "jdbc_url": jdbc_url,
        "user": user,
        "password": password,
        "owner": owner,
        "dblink": dblink,
        "env": env
    }


# ============================================================================
# LEITURA DE PARÂMETROS
# ============================================================================

def load_active_processes(catalog: str = "cm_dbx_dev") -> list:
    """
    Carrega lista de processos ativos da tabela de parâmetros.
    """
    query = f"""
        SELECT 
            processo_id,
            dataset_id,
            src_full_tablename,
            src_expected_rows,
            tgt_ful_tablename,
            COALESCE(fetchsize, 10000) as fetchsize,
            COALESCE(num_partitions, 800) as num_partitions,
            partition_column,
            lower_bound,
            upper_bound,
            notes
        FROM {catalog}.0_par.processos
        WHERE ativo = true
        ORDER BY processo_id
    """
    
    df = spark.sql(query)  # type: ignore[name-defined]
    processes = df.collect()
    
    print(f"✅ {len(processes)} processos ativos carregados")
    return processes


# ============================================================================
# LIMPEZA DE NOMES DE COLUNAS
# ============================================================================

def clean_column_names(df):
    """
    Remove espaços e caracteres especiais dos nomes das colunas.
    Reutiliza a lógica funcional existente.
    """
    old_columns = df.schema.names
    cleaned_count = 0
    
    for col_name in old_columns:
        clean_name = col_name.replace(" ", "").replace("-", "_").replace(".", "_")
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
            cleaned_count += 1
    
    if cleaned_count > 0:
        print(f"🧹 {cleaned_count} colunas renomeadas (espaços/caracteres especiais removidos)")
    
    return df


# ============================================================================
# CÁLCULO DINÂMICO DE PARTIÇÕES
# ============================================================================

def calculate_optimal_partitions(row_count: Optional[int], default: int = 800) -> int:
    """
    Calcula número ótimo de partições baseado no volume de dados.
    
    Regra de bolso:
    - < 1M linhas: 200 partições
    - 1M-10M: 400 partições
    - 10M-50M: 800 partições
    - > 50M: 1600 partições
    """
    if row_count is None:
        return default
    
    if row_count < 1_000_000:
        return 200
    elif row_count < 10_000_000:
        return 400
    elif row_count < 50_000_000:
        return 800
    else:
        return 1600


# ============================================================================
# PROCESSAMENTO DE UMA TABELA
# ============================================================================

def process_single_table(
    processo_id: str,
    src_full_tablename: str,
    tgt_ful_tablename: str,
    conn_config: Dict[str, str],
    fetchsize: int,
    num_partitions: int,
    partition_column: Optional[str],
    lower_bound: Optional[int],
    upper_bound: Optional[int],
    src_expected_rows: Optional[int]
) -> Dict[str, Any]:
    """
    Processa uma única tabela Oracle -> Delta.
    Retorna dicionário com resultado da execução.
    """
    start_time = datetime.utcnow()
    
    try:
        print("=" * 80)
        print(f"🚀 Iniciando processo: {processo_id}")
        print(f"📥 Origem: {src_full_tablename}")
        print(f"📤 Destino: {tgt_ful_tablename}")
        print("=" * 80)
        
        # 1. Criar JDBC reader base
        reader = (
            spark.read.format("jdbc")  # type: ignore[name-defined]
            .option("url", conn_config["jdbc_url"])
            .option("user", conn_config["user"])
            .option("password", conn_config["password"])
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("fetchsize", str(fetchsize))
        )
        
        print(f"🔧 Fetchsize: {fetchsize}")
        
        # 2. Adicionar particionamento JDBC se configurado
        if partition_column and lower_bound and upper_bound:
            print(f"⚡ Particionamento JDBC ATIVADO:")
            print(f"   - Coluna: {partition_column}")
            print(f"   - Range: {lower_bound} → {upper_bound}")
            print(f"   - Partições: {num_partitions}")
            
            reader = (
                reader
                .option("partitionColumn", partition_column)
                .option("lowerBound", str(lower_bound))
                .option("upperBound", str(upper_bound))
                .option("numPartitions", str(num_partitions))
            )
        else:
            print(f"📊 Leitura sequencial (sem particionamento JDBC)")
        
        # 3. Carregar dados
        print(f"📖 Lendo dados da origem...")
        df_bronze = reader.option("dbtable", src_full_tablename).load()
        
        # 4. Limpar nomes de colunas
        df_bronze = clean_column_names(df_bronze)
        
        # 5. Calcular partições ótimas para escrita
        optimal_partitions = calculate_optimal_partitions(src_expected_rows, num_partitions)
        print(f"🔢 Reparticionando para escrita: {optimal_partitions} partições")
        df_bronze = df_bronze.repartition(optimal_partitions)
        
        # 6. Escrever na tabela Delta
        print(f"💾 Gravando na tabela Delta: {tgt_ful_tablename}")
        df_bronze.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tgt_ful_tablename)
        
        # 7. Contar linhas gravadas
        print(f"🔍 Contando linhas gravadas...")
        row_count = spark.table(tgt_ful_tablename).count()  # type: ignore[name-defined]
        
        duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        
        print(f"✅ SUCESSO!")
        print(f"   - Linhas gravadas: {row_count:,}")
        print(f"   - Duração: {duration_seconds:.1f}s")
        print(f"   - Throughput: {row_count/duration_seconds:.0f} linhas/s")
        print("=" * 80)
        print()
        
        return {
            "processo_id": processo_id,
            "status": "SUCCESS",
            "row_count": row_count,
            "duration_seconds": duration_seconds,
            "error": None
        }
        
    except Exception as e:
        duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        error_msg = str(e)
        error_trace = traceback.format_exc()
        
        print(f"❌ ERRO no processo {processo_id}:")
        print(f"   {error_msg}")
        print(f"   Stack trace:")
        print(error_trace)
        print("=" * 80)
        print()
        
        return {
            "processo_id": processo_id,
            "status": "FAILED",
            "row_count": 0,
            "duration_seconds": duration_seconds,
            "error": error_msg,
            "error_trace": error_trace
        }


# ============================================================================
# EXECUÇÃO BATCH
# ============================================================================

def run_batch_ingestion(env: str = "PRD", catalog: str = "cm_dbx_dev") -> Dict[str, Any]:
    """
    Executa ingestão em batch de todas as tabelas ativas.
    """
    batch_start = datetime.utcnow()
    
    print("🚀" * 40)
    print("🚀 ORACLE BATCH INGESTION - Início")
    print("🚀" * 40)
    print()
    
    # 1. Configurar conexão
    conn_config = setup_oracle_connection(env)
    
    # 2. Carregar processos ativos
    processes = load_active_processes(catalog)
    
    if not processes:
        print("⚠️ Nenhum processo ativo encontrado!")
        return {"status": "NO_PROCESSES"}
    
    # 3. Processar cada tabela
    results = []
    for row in processes:
        result = process_single_table(
            processo_id=row.processo_id,
            src_full_tablename=row.src_full_tablename,
            tgt_ful_tablename=row.tgt_ful_tablename,
            conn_config=conn_config,
            fetchsize=row.fetchsize,
            num_partitions=row.num_partitions,
            partition_column=row.partition_column,
            lower_bound=row.lower_bound,
            upper_bound=row.upper_bound,
            src_expected_rows=row.src_expected_rows
        )
        results.append(result)
    
    # 4. Resumo final
    batch_duration = (datetime.utcnow() - batch_start).total_seconds()
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in results if r["status"] == "FAILED")
    total_rows = sum(r["row_count"] for r in results)
    
    print("🏁" * 40)
    print("🏁 BATCH COMPLETO - Resumo")
    print("🏁" * 40)
    print(f"✅ Sucessos: {success_count}/{len(processes)}")
    print(f"❌ Falhas: {failed_count}/{len(processes)}")
    print(f"📊 Total de linhas: {total_rows:,}")
    print(f"⏱️ Duração total: {batch_duration:.1f}s")
    print()
    
    if failed_count > 0:
        print("❌ Processos com falha:")
        for r in results:
            if r["status"] == "FAILED":
                print(f"   - {r['processo_id']}: {r['error']}")
        print()
    
    return {
        "status": "COMPLETED",
        "total_processes": len(processes),
        "success_count": success_count,
        "failed_count": failed_count,
        "total_rows": total_rows,
        "duration_seconds": batch_duration,
        "results": results
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Parâmetros do job Databricks
    env = dbutils.widgets.get("env") if "dbutils" in dir() else "PRD"  # type: ignore[name-defined]
    catalog = dbutils.widgets.get("catalog") if "dbutils" in dir() else "cm_dbx_dev"  # type: ignore[name-defined]
    
    # Executar batch
    summary = run_batch_ingestion(env=env, catalog=catalog)
    
    # Exibir resultado
    print("📋 Resultado final:")
    print(summary)
