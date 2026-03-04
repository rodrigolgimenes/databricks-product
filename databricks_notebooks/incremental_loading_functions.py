# Databricks notebook source
# ============================================================================
# Incremental Loading Functions - Universal Oracle → Delta Engine
# ============================================================================
# Funções para suporte completo de carga incremental inteligente:
# - Discovery automático de estratégia
# - MERGE incremental por watermark ou hash
# - Reconciliação de deletes (opt-in)
# - OPTIMIZE condicional
# - Override de watermark para reprocessamento
# ============================================================================

import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DecimalType, StringType
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================================
# HELPER: SQL String Literal
# ============================================================================

def _sql_string_literal(s: str) -> str:
    """Escape string for SQL literal."""
    return "'" + s.replace("'", "''") + "'"


# ============================================================================
# 1. DISCOVERY: Detectar Colunas Voláteis
# ============================================================================

def _detect_volatile_columns(oracle_table: str, jdbc_url: str, user: str, pwd: str) -> List[str]:
    """
    Auto-detecta colunas voláteis que devem ser excluídas do hash.
    
    Voláteis são colunas que mudam sempre sem refletir mudança real de negócio:
    - UPDATED_AT, LAST_MODIFIED, MODIFIED_AT
    - LAST_ACCESS, ACCESS_COUNT, LAST_LOGIN
    - AUDIT_*, LOG_*, METADATA_*
    
    Args:
        oracle_table: Nome da tabela no formato OWNER.TABLE ou OWNER.TABLE@DBLINK
        jdbc_url: JDBC URL do Oracle
        user: Usuário Oracle
        pwd: Senha Oracle
        
    Returns:
        Lista de nomes de colunas voláteis encontradas (uppercase)
    """
    # Patterns conhecidos de colunas voláteis
    VOLATILE_PATTERNS = [
        'UPDATED_AT', 'LAST_MODIFIED', 'MODIFIED_AT', 'LAST_UPDATE', 
        'LAST_ACCESS', 'ACCESS_COUNT', 'ACCESS_DATE', 'LAST_LOGIN',
        'AUDIT_DATE', 'AUDIT_USER', 'AUDIT_TIMESTAMP',
        'LOG_DATE', 'LOG_TIMESTAMP', 'LOG_USER',
        'METADATA_UPDATE', 'LAST_SYNC', 'SYNC_DATE'
    ]
    
    try:
        # Parse table name
        base = oracle_table.split('@')[0].strip()
        parts = [p for p in base.split('.') if p]
        if len(parts) != 2:
            return []
        
        owner, table = parts[0].upper(), parts[1].upper()
        
        # Query Oracle metadata
        query = f"""
        (SELECT column_name 
         FROM all_tab_columns 
         WHERE owner = '{owner}' AND table_name = '{table}'
         AND UPPER(column_name) IN ({','.join([f"'{p}'" for p in VOLATILE_PATTERNS])})
        ) volatile_cols
        """
        
        # type: ignore[name-defined]
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        volatile_cols = [row.COLUMN_NAME.upper() for row in df.collect()]
        print(f"[DISCOVERY] Colunas voláteis detectadas: {volatile_cols}")
        return volatile_cols
        
    except Exception as e:
        print(f"[DISCOVERY] Erro ao detectar colunas voláteis: {e}")
        return []  # Fallback: nenhuma coluna volátil


# ============================================================================
# 2. PK CANDIDATE DISCOVERY: Descobrir PK por unicidade
# ============================================================================

# Configurações do PK Discovery
_PK_SAMPLE_SIZE = 100_000          # Amostra para teste de unicidade
_PK_MAX_SINGLE_COLS = 10           # Máx colunas individuais a testar
_PK_MAX_COMBO_2 = 15               # Máx combinações de 2 colunas
_PK_MAX_COMBO_3 = 20               # Máx combinações de 3 colunas
_PK_QUERY_TIMEOUT_SEC = 30         # Timeout por query de unicidade
_PK_FULL_CHECK_THRESHOLD = 5_000_000  # Tabelas <= 5M usam full check; > 5M usam quick dup

# Separador raro para concatenação segura (Unit Separator U+241F)
_PK_SEPARATOR = '\u241f'
_PK_NULL_PLACEHOLDER = '\u2205'  # ∅

# Patterns de nomes que indicam provável PK
_PK_NAME_HEURISTIC_PATTERNS = [
    'ID', 'CODIGO', 'CD', 'NR', 'NUM', 'CODE', 'KEY', 'SEQ',
    'IN_CODIGO', 'CHAVE', 'SK', 'PK', 'IDENT',
]

# Tipos Oracle excluídos do PK discovery
_PK_EXCLUDED_TYPES = [
    'CLOB', 'BLOB', 'NCLOB', 'LONG', 'LONG RAW', 'RAW',
    'XMLTYPE', 'BFILE', 'SDO_GEOMETRY', 'ANYDATA',
]


def _discover_pk_candidates_oracle(
    jdbc_url: str,
    user: str,
    pwd: str,
    owner: str,
    table: str,
    dblink: Optional[str],
    watermark_candidates: List[str],
    num_rows: Optional[int] = None
) -> Dict[str, Any]:
    """
    Descobre PK candidata por teste de unicidade quando não há constraint declarada.
    
    Fluxo:
    0. Extrair num_rows (se não fornecido)
    1. Listar colunas elegíveis (excluir LOB, DATE/TIMESTAMP, watermark, voláteis)
    2. Ranking por cardinalidade (stats) ou heurística de nome
    3. Testar unicidade individual (amostra 100K)
    4. Testar combos 2-3 colunas
    5. Confirmar (full check ou quick duplicate search)
    6. Calcular score de confiança
    
    Returns:
        {"pk": [...], "pk_source": "CANDIDATE_DISCOVERY", "pk_confidence": 0.xx,
         "pk_discovery_details": {...}}
        ou {} se nada encontrado.
    """
    from itertools import combinations
    
    table_ref = f"{owner}.{table}" + (f"@{dblink}" if dblink else "")
    print(f"[PK_DISCOVERY] ========== Iniciando PK Candidate Discovery para {table_ref} ==========")
    
    def _jdbc_query(query_str: str) -> List:
        """Helper: executa query Oracle via JDBC com timeout."""
        try:
            df = (
                spark.read.format("jdbc")  # type: ignore[name-defined]
                .option("url", jdbc_url)
                .option("dbtable", query_str)
                .option("user", user)
                .option("password", pwd)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("queryTimeout", str(_PK_QUERY_TIMEOUT_SEC))
                .load()
            )
            return df.collect()
        except Exception as e:
            print(f"[PK_DISCOVERY] Query error: {e}")
            return []
    
    # ===================
    # ETAPA 0: num_rows
    # ===================
    if num_rows is None or num_rows <= 0:
        try:
            if not dblink:
                rows = _jdbc_query(
                    f"(SELECT num_rows FROM all_tables WHERE owner = '{owner}' AND table_name = '{table}') nr"
                )
                if rows and rows[0][0] is not None:
                    num_rows = int(rows[0][0])
        except Exception:
            pass
        if num_rows is None or num_rows <= 0:
            num_rows = 1_000_000  # Fallback
            print(f"[PK_DISCOVERY] num_rows não disponível, assumindo {num_rows:,}")
        else:
            print(f"[PK_DISCOVERY] num_rows = {num_rows:,}")
    
    # ===================
    # ETAPA 1: Colunas elegíveis
    # ===================
    print(f"[PK_DISCOVERY] Etapa 1: Buscando colunas elegíveis...")
    
    # Patterns voláteis (reutilizando de _detect_volatile_columns)
    volatile_patterns = [
        'UPDATED_AT', 'LAST_MODIFIED', 'MODIFIED_AT', 'LAST_UPDATE',
        'LAST_ACCESS', 'ACCESS_COUNT', 'ACCESS_DATE', 'LAST_LOGIN',
        'AUDIT_DATE', 'AUDIT_USER', 'AUDIT_TIMESTAMP',
        'LOG_DATE', 'LOG_TIMESTAMP', 'LOG_USER',
        'METADATA_UPDATE', 'LAST_SYNC', 'SYNC_DATE',
    ]
    
    watermark_set = {w.upper() for w in watermark_candidates}
    volatile_set = {v.upper() for v in volatile_patterns}
    excluded_types_set = {t.upper() for t in _PK_EXCLUDED_TYPES}
    
    cols_query = f"""
    (SELECT column_name, data_type, nullable
     FROM all_tab_columns
     WHERE owner = '{owner}' AND table_name = '{table}'
     ORDER BY column_id
    ) eligible_cols
    """
    
    all_cols_rows = _jdbc_query(cols_query)
    if not all_cols_rows:
        print(f"[PK_DISCOVERY] Nenhuma coluna encontrada, abortando.")
        return {}
    
    eligible_cols = []
    for row in all_cols_rows:
        col_name = str(row.COLUMN_NAME).upper()
        data_type = str(row.DATA_TYPE).upper()
        nullable = str(row.NULLABLE).upper()
        
        # Excluir tipos inelegíveis
        if any(excl in data_type for excl in excluded_types_set):
            continue
        
        # Excluir watermark candidates
        if col_name in watermark_set:
            continue
        
        # Excluir voláteis
        if col_name in volatile_set:
            continue
        
        is_date_type = 'DATE' in data_type or 'TIMESTAMP' in data_type
        is_not_null = nullable == 'N'
        
        eligible_cols.append({
            "name": col_name,
            "data_type": data_type,
            "is_not_null": is_not_null,
            "is_date_type": is_date_type,
        })
    
    # Filtrar DATE/TIMESTAMP por padrão (podem ser incluídas em combos se necessário)
    non_date_cols = [c for c in eligible_cols if not c["is_date_type"]]
    
    if not non_date_cols:
        print(f"[PK_DISCOVERY] Nenhuma coluna elegível (não-date), abortando.")
        return {}
    
    print(f"[PK_DISCOVERY] {len(non_date_cols)} colunas elegíveis (de {len(all_cols_rows)} total)")
    
    # ===================
    # ETAPA 2: Ranking
    # ===================
    print(f"[PK_DISCOVERY] Etapa 2: Ranking por cardinalidade/heurística...")
    
    # Tentar obter stats
    stats_map = {}  # col_name -> num_distinct
    if not dblink:
        col_names_sql = ",".join([f"'{c['name']}'" for c in non_date_cols])
        stats_query = f"""
        (SELECT column_name, num_distinct
         FROM all_tab_col_statistics
         WHERE owner = '{owner}' AND table_name = '{table}'
         AND column_name IN ({col_names_sql})
        ) col_stats
        """
        stats_rows = _jdbc_query(stats_query)
        for sr in stats_rows:
            if sr.NUM_DISTINCT is not None:
                stats_map[str(sr.COLUMN_NAME).upper()] = int(sr.NUM_DISTINCT)
    
    has_stats = len(stats_map) > 0
    
    def _rank_score(col_info: Dict) -> float:
        """Score para ranking (maior = melhor candidata a PK)."""
        name = col_info["name"]
        score = 0.0
        
        if has_stats and name in stats_map and num_rows and num_rows > 0:
            # Stats disponíveis: usar razão distinct/total
            ratio = stats_map[name] / num_rows
            score += ratio * 100  # 0-100
        
        # Heurística de nome
        name_upper = name.upper()
        for pattern in _PK_NAME_HEURISTIC_PATTERNS:
            if pattern in name_upper:
                score += 20
                break
        
        # Preferir NOT NULL
        if col_info["is_not_null"]:
            score += 15
        
        # Preferir NUMBER/VARCHAR2
        dtype = col_info["data_type"]
        if 'NUMBER' in dtype or 'INTEGER' in dtype:
            score += 10
        elif 'VARCHAR' in dtype or 'CHAR' in dtype:
            score += 5
        
        return score
    
    # Ordenar por score e pegar top N
    ranked_cols = sorted(non_date_cols, key=_rank_score, reverse=True)
    top_cols = ranked_cols[:_PK_MAX_SINGLE_COLS]
    
    print(f"[PK_DISCOVERY] Top {len(top_cols)} colunas para teste:")
    for c in top_cols:
        s = stats_map.get(c["name"], "N/A")
        print(f"[PK_DISCOVERY]   - {c['name']} ({c['data_type']}, NOT_NULL={c['is_not_null']}, distinct={s})")
    
    # ===================
    # ETAPA 3: Teste unicidade individual (amostra)
    # ===================
    print(f"[PK_DISCOVERY] Etapa 3: Testando unicidade individual (amostra {_PK_SAMPLE_SIZE:,})...")
    
    single_candidates = []
    
    for col_info in top_cols:
        col_name = col_info["name"]
        
        uniqueness_query = f"""
        (SELECT
           CASE WHEN COUNT(DISTINCT NVL(TO_CHAR({col_name}), '{_PK_NULL_PLACEHOLDER}')) = COUNT(*)
                THEN 1 ELSE 0 END AS is_unique,
           COUNT(*) AS sample_count
         FROM (SELECT {col_name} FROM {table_ref} WHERE ROWNUM <= {_PK_SAMPLE_SIZE})
        ) uq
        """
        
        rows = _jdbc_query(uniqueness_query)
        if rows and rows[0].IS_UNIQUE == 1:
            sample_count = int(rows[0].SAMPLE_COUNT)
            print(f"[PK_DISCOVERY]   ✓ {col_name}: ÚNICA na amostra ({sample_count:,} rows)")
            single_candidates.append({
                "cols": [col_name],
                "col_infos": [col_info],
                "sample_count": sample_count,
                "method": "single_col_sample",
            })
        else:
            print(f"[PK_DISCOVERY]   ✗ {col_name}: não única")
    
    # Se encontrou candidata individual, ir direto para confirmação
    if single_candidates:
        print(f"[PK_DISCOVERY] {len(single_candidates)} coluna(s) individual(is) candidata(s)")
        # Preferir a primeira (melhor ranqueada)
        best = single_candidates[0]
    else:
        # ===================
        # ETAPA 4: Combos 2 e 3 colunas
        # ===================
        print(f"[PK_DISCOVERY] Etapa 4: Testando combinações de 2-3 colunas...")
        best = None
        
        # Combos de 2
        combo2_cols = top_cols[:8]  # Top 8 para combinar (C(8,2)=28, limitado a 15)
        combos_2 = list(combinations(combo2_cols, 2))[:_PK_MAX_COMBO_2]
        
        print(f"[PK_DISCOVERY] Testando {len(combos_2)} combinações de 2 colunas...")
        for combo in combos_2:
            col_names = [c["name"] for c in combo]
            select_cols = ", ".join(col_names)
            
            concat_expr = " || '" + _PK_SEPARATOR + "' || ".join(
                [f"NVL(TO_CHAR({c}), '{_PK_NULL_PLACEHOLDER}')" for c in col_names]
            )
            
            combo_query = f"""
            (SELECT
               CASE WHEN COUNT(DISTINCT ({concat_expr})) = COUNT(*)
                    THEN 1 ELSE 0 END AS is_unique,
               COUNT(*) AS sample_count
             FROM (SELECT {select_cols} FROM {table_ref} WHERE ROWNUM <= {_PK_SAMPLE_SIZE})
            ) cq
            """
            
            rows = _jdbc_query(combo_query)
            if rows and rows[0].IS_UNIQUE == 1:
                sample_count = int(rows[0].SAMPLE_COUNT)
                print(f"[PK_DISCOVERY]   ✓ ({', '.join(col_names)}): ÚNICA na amostra")
                best = {
                    "cols": col_names,
                    "col_infos": list(combo),
                    "sample_count": sample_count,
                    "method": "combo_2col_sample",
                }
                break  # Parar na primeira combo única (melhor ranqueada)
        
        # Combos de 3 (se não encontrou em 2)
        if not best:
            combo3_cols = top_cols[:7]  # Top 7 para combinar (C(7,3)=35, limitado a 20)
            combos_3 = list(combinations(combo3_cols, 3))[:_PK_MAX_COMBO_3]
            
            print(f"[PK_DISCOVERY] Testando {len(combos_3)} combinações de 3 colunas...")
            for combo in combos_3:
                col_names = [c["name"] for c in combo]
                select_cols = ", ".join(col_names)
                
                concat_expr = " || '" + _PK_SEPARATOR + "' || ".join(
                    [f"NVL(TO_CHAR({c}), '{_PK_NULL_PLACEHOLDER}')" for c in col_names]
                )
                
                combo_query = f"""
                (SELECT
                   CASE WHEN COUNT(DISTINCT ({concat_expr})) = COUNT(*)
                        THEN 1 ELSE 0 END AS is_unique,
                   COUNT(*) AS sample_count
                 FROM (SELECT {select_cols} FROM {table_ref} WHERE ROWNUM <= {_PK_SAMPLE_SIZE})
                ) cq
                """
                
                rows = _jdbc_query(combo_query)
                if rows and rows[0].IS_UNIQUE == 1:
                    sample_count = int(rows[0].SAMPLE_COUNT)
                    print(f"[PK_DISCOVERY]   ✓ ({', '.join(col_names)}): ÚNICA na amostra")
                    best = {
                        "cols": col_names,
                        "col_infos": list(combo),
                        "sample_count": sample_count,
                        "method": "combo_3col_sample",
                    }
                    break
    
    if not best:
        print(f"[PK_DISCOVERY] Nenhuma PK candidata encontrada.")
        print(f"[PK_DISCOVERY] ========== PK Discovery concluído (sem resultado) ==========")
        return {}
    
    # ===================
    # ETAPA 5: Confirmação
    # ===================
    pk_cols = best["cols"]
    col_infos = best["col_infos"]
    verified_full = False
    verified_quick_dup = False
    
    print(f"[PK_DISCOVERY] Etapa 5: Confirmando PK candidata {pk_cols}...")
    
    if num_rows and num_rows <= _PK_FULL_CHECK_THRESHOLD:
        # Tabela pequena/média: full count
        print(f"[PK_DISCOVERY] Tabela <= {_PK_FULL_CHECK_THRESHOLD:,} rows, executando full check...")
        
        if len(pk_cols) == 1:
            full_query = f"""
            (SELECT
               CASE WHEN COUNT(DISTINCT NVL(TO_CHAR({pk_cols[0]}), '{_PK_NULL_PLACEHOLDER}')) = COUNT(*)
                    THEN 1 ELSE 0 END AS is_unique
             FROM {table_ref}
            ) fc
            """
        else:
            concat_expr = " || '" + _PK_SEPARATOR + "' || ".join(
                [f"NVL(TO_CHAR({c}), '{_PK_NULL_PLACEHOLDER}')" for c in pk_cols]
            )
            full_query = f"""
            (SELECT
               CASE WHEN COUNT(DISTINCT ({concat_expr})) = COUNT(*)
                    THEN 1 ELSE 0 END AS is_unique
             FROM {table_ref}
            ) fc
            """
        
        rows = _jdbc_query(full_query)
        if rows and rows[0].IS_UNIQUE == 1:
            verified_full = True
            print(f"[PK_DISCOVERY]   ✓ Full check PASSED")
        else:
            print(f"[PK_DISCOVERY]   ✗ Full check FAILED — descartando candidata")
            return {}
    else:
        # Tabela grande: quick duplicate search
        print(f"[PK_DISCOVERY] Tabela > {_PK_FULL_CHECK_THRESHOLD:,} rows, executando quick duplicate search...")
        
        group_cols = ", ".join(pk_cols)
        
        dup_query = f"""
        (SELECT 1 AS has_dup FROM (
           SELECT {group_cols}, COUNT(*) AS cnt
           FROM {table_ref}
           GROUP BY {group_cols}
           HAVING COUNT(*) > 1
        ) WHERE ROWNUM = 1
        ) dq
        """
        
        rows = _jdbc_query(dup_query)
        if not rows or len(rows) == 0:
            verified_quick_dup = True
            print(f"[PK_DISCOVERY]   ✓ Quick dup search: nenhuma duplicata encontrada")
        else:
            print(f"[PK_DISCOVERY]   ✗ Quick dup search: duplicata encontrada — descartando candidata")
            return {}
    
    # ===================
    # ETAPA 6: Score
    # ===================
    print(f"[PK_DISCOVERY] Etapa 6: Calculando score de confiança...")
    
    score = 0.60  # Base: passou na amostra
    
    all_not_null = all(c.get("is_not_null", False) for c in col_infos)
    if all_not_null:
        score += 0.15
        print(f"[PK_DISCOVERY]   +0.15 (todas NOT NULL)")
    
    # Verificar stats de alta cardinalidade
    if has_stats and num_rows and num_rows > 0:
        high_card = all(
            stats_map.get(c["name"], 0) / num_rows > 0.8
            for c in col_infos
            if c["name"] in stats_map
        )
        if high_card and any(c["name"] in stats_map for c in col_infos):
            score += 0.10
            print(f"[PK_DISCOVERY]   +0.10 (stats alta cardinalidade)")
    
    if verified_full or verified_quick_dup:
        score += 0.10
        print(f"[PK_DISCOVERY]   +0.10 (confirmado {'full' if verified_full else 'quick_dup'})")
    
    has_date_col = any(c.get("is_date_type", False) for c in col_infos)
    if has_date_col:
        score -= 0.10
        print(f"[PK_DISCOVERY]   -0.10 (inclui coluna date/timestamp)")
    
    score = min(score, 0.99)
    score = round(score, 2)
    
    method = best["method"] + f"_{_PK_SAMPLE_SIZE // 1000}k"
    
    result = {
        "pk": pk_cols,
        "pk_source": "CANDIDATE_DISCOVERY",
        "pk_confidence": score,
        "pk_discovery_details": {
            "method": method,
            "sample_size": _PK_SAMPLE_SIZE,
            "verified_full": verified_full,
            "verified_quick_dup": verified_quick_dup,
            "all_not_null": all_not_null,
            "num_rows_estimate": num_rows,
            "columns_tested_single": len(top_cols),
        },
    }
    
    print(f"[PK_DISCOVERY] ✅ PK Candidata encontrada: {pk_cols}")
    print(f"[PK_DISCOVERY] Score: {score} | Method: {method}")
    print(f"[PK_DISCOVERY] Verified: full={verified_full}, quick_dup={verified_quick_dup}")
    print(f"[PK_DISCOVERY] ========== PK Discovery concluído ==========")
    
    return result


# ============================================================================
# 3. DISCOVERY: Estratégia Incremental
# ============================================================================

def _discover_incremental_strategy(
    dataset_id: str,
    oracle_table: str,
    jdbc_url: str,
    user: str,
    pwd: str,
    catalog: str
) -> Dict[str, Any]:
    """
    Descobre automaticamente a melhor estratégia incremental para o dataset.
    
    Passos:
    1. Buscar colunas de auditoria (LAST_UPDATE_DATE, UPDATED_AT, etc)
    2. Validar incrementalidade usando stats do Oracle (PERFORMANCE CRITICAL)
    3. Buscar Primary Key / Unique Key
    4. Decidir estratégia baseado em tamanho da tabela
    
    Returns:
        {
            "strategy": "WATERMARK | HASH_MERGE | SNAPSHOT | APPEND_LOG | REQUIRES_CDC",
            "metadata": {
                "watermark_col": "LAST_UPDATE_DATE",  # se WATERMARK
                "pk": ["ID", "SUBID"],                # se encontrado
                "hash_exclude_cols": ["UPDATED_AT"],  # colunas voláteis
                "table_size_rows": 50000000,
                "reason": "...",                       # se REQUIRES_CDC
                "recommendation": "..."                # se REQUIRES_CDC
            }
        }
    """
    print(f"[DISCOVERY] ========== Iniciando Discovery para {oracle_table} ==========")
    
    # Parse table name
    base = oracle_table.split('@')[0].strip()
    dblink = oracle_table.split('@')[1] if '@' in oracle_table else None
    parts = [p for p in base.split('.') if p]
    if len(parts) != 2:
        return {"strategy": "SNAPSHOT", "metadata": {"reason": "Invalid table format"}}
    
    owner, table = parts[0].upper(), parts[1].upper()
    print(f"[DISCOVERY] Owner: {owner}, Table: {table}, DBLink: {dblink}")
    
    # ====================
    # PASSO 1: Buscar Colunas de Auditoria
    # ====================
    watermark_candidates = [
        # Patterns: UPDATE/ALTER
        'LAST_UPDATE_DATE', 'UPDATED_AT', 'DT_ALTERACAO', 'DATA_ALTERACAO', 'DATALT',
        'DT_ATUALIZACAO', 'DATA_ATUALIZACAO', 'TIMESTAMP_UPD', 'DT_MODIFICACAO', 'MODIFIED_AT',
        'DT_ULTIMA_ALTERACAO', 'DATA_ULTIMA_ALTERACAO', 'DT_ULT_ALTERACAO',
        'DATA_ULT_ATUALIZACAO', 'DT_ULTIMA_ATUALIZACAO', 'DT_ULT_ATUALIZACAO',
        'DATA_MODIFICACAO', 'DATA_ULTIMA_MODIFICACAO', 'LAST_MODIFIED_DATE',
        'LAST_UPDATED_DATE', 'UPDATE_DATE', 'MODIFY_DATE', 'CHANGED_AT', 'DT_CHANGE',
        # Patterns: INSERT/CREATE
        'DATAINSERCAO', 'DATA_INSERCAO', 'CREATED_AT', 'DT_INSERCAO', 'DT_CRIACAO',
        'INSERT_DATE', 'CREATION_DATE', 'DT_CADASTRO', 'DATA_CADASTRO',
        'DATA_INCLUSAO', 'DT_INCLUSAO',
        # Patterns: TIMESTAMP generic
        'TIMESTAMP_COL', 'TIMESTAMP', 'DT_TIMESTAMP', 'DATA_HORA',
        'DT_REGISTRO', 'DATA_REGISTRO', 'DT_PROCESSAMENTO', 'DATA_PROCESSAMENTO',
    ]
    
    watermark_col = None
    watermark_type = None
    
    try:
        query_cols = f"""
        (SELECT column_name, data_type
         FROM all_tab_columns
         WHERE owner = '{owner}' AND table_name = '{table}'
         AND UPPER(column_name) IN ({','.join([f"'{c}'" for c in watermark_candidates])})
         ORDER BY CASE 
           WHEN UPPER(column_name) LIKE '%UPDATE%' THEN 1
           WHEN UPPER(column_name) LIKE '%ALTERACAO%' THEN 2
           ELSE 3
         END
        ) wm_cols
        """
        
        # type: ignore[name-defined]
        df_cols = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query_cols)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        rows = df_cols.collect()
        if rows:
            watermark_col = rows[0].COLUMN_NAME.upper()
            watermark_type = rows[0].DATA_TYPE.upper()
            print(f"[DISCOVERY] ✓ Coluna watermark candidata: {watermark_col} ({watermark_type})")
        else:
            print(f"[DISCOVERY] ⚠ Nenhuma coluna de auditoria encontrada")
    except Exception as e:
        print(f"[DISCOVERY] Erro ao buscar colunas de auditoria: {e}")
    
    # ====================
    # PASSO 2: Validar Incrementalidade (USAR STATS DO ORACLE - PERFORMANCE CRITICAL)
    # ====================
    watermark_valid = False
    table_size_rows = None
    
    if watermark_col:
        try:
            # OPTION A: Stats do Oracle (instantâneo, preferencial)
            print(f"[DISCOVERY] Validando incrementalidade usando all_tab_col_statistics...")
            query_stats = f"""
            (SELECT num_rows, num_distinct, low_value, high_value, num_nulls
             FROM all_tab_col_statistics
             WHERE owner = '{owner}' AND table_name = '{table}' AND column_name = '{watermark_col}'
            ) stats
            """
            
            # type: ignore[name-defined]
            df_stats = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", query_stats)
                .option("user", user)
                .option("password", pwd)
                .option("driver", "oracle.jdbc.OracleDriver")
                .load()
            )
            
            stats_rows = df_stats.collect()
            if stats_rows and stats_rows[0].NUM_ROWS:
                num_rows = int(stats_rows[0].NUM_ROWS or 0)
                num_distinct = int(stats_rows[0].NUM_DISTINCT or 0)
                num_nulls = int(stats_rows[0].NUM_NULLS or 0)
                
                table_size_rows = num_rows
                print(f"[DISCOVERY] Stats Oracle: {num_rows:,} rows, {num_distinct:,} distinct values, {num_nulls} nulls")
                
                # Critérios de validação
                if num_rows > 0:
                    distinct_ratio = num_distinct / num_rows
                    null_ratio = num_nulls / num_rows
                    
                    if distinct_ratio > 0.01 and null_ratio < 0.5:
                        watermark_valid = True
                        print(f"[DISCOVERY] ✓ Coluna incremental válida (distinct_ratio={distinct_ratio:.3f}, null_ratio={null_ratio:.3f})")
                    else:
                        print(f"[DISCOVERY] ✗ Coluna não válida (distinct_ratio={distinct_ratio:.3f}, null_ratio={null_ratio:.3f})")
            else:
                print(f"[DISCOVERY] ⚠ Stats não disponíveis, tentando amostra 1%...")
                
                # OPTION B: Amostra 1% (fallback se stats não disponíveis)
                if not dblink:  # SAMPLE não funciona com DBLink
                    query_sample = f"""
                    (SELECT 
                       COUNT(*) * 100 as total_rows_estimate,
                       APPROX_COUNT_DISTINCT({watermark_col}) * 100 as distinct_estimate,
                       COUNT(CASE WHEN {watermark_col} IS NULL THEN 1 END) * 100 as null_estimate
                     FROM {owner}.{table} SAMPLE (1)
                    ) sample
                    """
                    
                    # type: ignore[name-defined]
                    df_sample = (
                        spark.read.format("jdbc")
                        .option("url", jdbc_url)
                        .option("dbtable", query_sample)
                        .option("user", user)
                        .option("password", pwd)
                        .option("driver", "oracle.jdbc.OracleDriver")
                        .load()
                    )
                    
                    sample_rows = df_sample.collect()
                    if sample_rows:
                        total = int(sample_rows[0].TOTAL_ROWS_ESTIMATE or 0)
                        distinct = int(sample_rows[0].DISTINCT_ESTIMATE or 0)
                        nulls = int(sample_rows[0].NULL_ESTIMATE or 0)
                        
                        table_size_rows = total
                        print(f"[DISCOVERY] Amostra 1%: ~{total:,} rows, ~{distinct:,} distinct, ~{nulls} nulls")
                        
                        if total > 0 and (distinct / total) > 0.01 and (nulls / total) < 0.5:
                            watermark_valid = True
                            print(f"[DISCOVERY] ✓ Coluna incremental válida (baseado em amostra)")
                        
        except Exception as e:
            print(f"[DISCOVERY] Erro ao validar incrementalidade: {e}")
    
    # ====================
    # FALLBACK: Se encontrou watermark mas não conseguiu validar (DBLink ou sem stats),
    # assumir válida se tipo é DATE ou TIMESTAMP
    # ====================
    if watermark_col and not watermark_valid and watermark_type:
        if 'DATE' in watermark_type or 'TIMESTAMP' in watermark_type:
            watermark_valid = True
            print(f"[DISCOVERY] ✓ Coluna watermark assumida como válida (tipo {watermark_type}, stats não disponíveis)")
    
    # ====================
    # PASSO 3: Buscar Primary Key
    # ====================
    pk_cols = []
    
    try:
        query_pk = f"""
        (SELECT cols.column_name, cols.position
         FROM all_constraints cons
         JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
         WHERE cons.owner = '{owner}' AND cons.table_name = '{table}'
         AND cons.constraint_type IN ('P', 'U')
         ORDER BY cols.position
        ) pk
        """
        
        # type: ignore[name-defined]
        df_pk = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query_pk)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        pk_cols = [row.COLUMN_NAME.upper() for row in df_pk.collect()]
        if pk_cols:
            print(f"[DISCOVERY] ✓ Primary Key encontrada: {pk_cols}")
        else:
            print(f"[DISCOVERY] ⚠ Nenhuma PK/UK encontrada")
    except Exception as e:
        print(f"[DISCOVERY] Erro ao buscar PK: {e}")
    
    # Track PK source and confidence
    pk_source = None
    pk_confidence = 0.0
    pk_discovery_details = {}
    
    if pk_cols:
        pk_source = "DECLARED_CONSTRAINT"
        pk_confidence = 1.0
        print(f"[DISCOVERY] PK source: DECLARED_CONSTRAINT (confidence=1.0)")
    else:
        # ====================
        # PASSO 3B: PK Candidate Discovery (quando constraint não existe)
        # ====================
        print(f"[DISCOVERY] Iniciando PK Candidate Discovery...")
        try:
            pk_result = _discover_pk_candidates_oracle(
                jdbc_url=jdbc_url,
                user=user,
                pwd=pwd,
                owner=owner,
                table=table,
                dblink=dblink,
                watermark_candidates=watermark_candidates,
                num_rows=table_size_rows,
            )
            if pk_result and pk_result.get("pk"):
                pk_cols = pk_result["pk"]
                pk_source = pk_result.get("pk_source", "CANDIDATE_DISCOVERY")
                pk_confidence = pk_result.get("pk_confidence", 0.0)
                pk_discovery_details = pk_result.get("pk_discovery_details", {})
                print(f"[DISCOVERY] ✓ PK Candidata: {pk_cols} (confidence={pk_confidence}, source={pk_source})")
            else:
                print(f"[DISCOVERY] PK Candidate Discovery: nenhuma PK encontrada")
        except Exception as pk_disc_err:
            print(f"[DISCOVERY] Erro no PK Candidate Discovery: {pk_disc_err}")
    
    # ====================
    # PASSO 4: Estimar Tamanho da Tabela (se ainda não temos)
    # ====================
    if table_size_rows is None:
        try:
            query_size = f"""
            (SELECT num_rows FROM all_tables WHERE owner = '{owner}' AND table_name = '{table}') size
            """
            
            # type: ignore[name-defined]
            df_size = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", query_size)
                .option("user", user)
                .option("password", pwd)
                .option("driver", "oracle.jdbc.OracleDriver")
                .load()
            )
            
            size_rows = df_size.collect()
            if size_rows and size_rows[0].NUM_ROWS:
                table_size_rows = int(size_rows[0].NUM_ROWS)
                print(f"[DISCOVERY] Tamanho estimado: {table_size_rows:,} rows")
        except Exception as e:
            print(f"[DISCOVERY] Erro ao estimar tamanho: {e}")
            table_size_rows = 1_000_000  # Fallback: assumir tabela média
    
    # ====================
    # PASSO 5: Decisão Final
    # ====================
    
    # Detectar colunas voláteis
    volatile_cols = _detect_volatile_columns(oracle_table, jdbc_url, user, pwd)
    
    # Decisão baseada em heurística
    if watermark_col and watermark_valid:
        # Estratégia A: WATERMARK (com ou sem PK)
        strategy = "WATERMARK"
        metadata = {
            "watermark_column": watermark_col,
            "pk": pk_cols,
            "hash_exclude_cols": volatile_cols,
            "table_size_rows": table_size_rows
        }
        print(f"[DISCOVERY] ✅ Estratégia sugerida: WATERMARK (watermark={watermark_col}, pk={pk_cols or 'N/A'})")
        
    elif pk_cols and pk_confidence >= 0.90 and table_size_rows and 10_000_000 <= table_size_rows <= 100_000_000:
        # Estratégia B: HASH_MERGE (tabelas médias 10M-100M, PK com confiança alta)
        strategy = "HASH_MERGE"
        metadata = {
            "pk": pk_cols,
            "hash_exclude_cols": volatile_cols,
            "table_size_rows": table_size_rows
        }
        print(f"[DISCOVERY] ✅ Estratégia sugerida: HASH_MERGE (pk={pk_cols}, confidence={pk_confidence}, size={table_size_rows:,})")
        
    elif pk_cols and table_size_rows and table_size_rows > 100_000_000:
        # Estratégia E: REQUIRES_CDC (tabelas muito grandes > 100M)
        strategy = "REQUIRES_CDC"
        metadata = {
            "pk": pk_cols,
            "table_size_rows": table_size_rows,
            "reason": "Table too large for full scan hash comparison",
            "recommendation": "Implement Oracle CDC/LogMiner or accept SNAPSHOT mode"
        }
        print(f"[DISCOVERY] ⚠️ Estratégia sugerida: REQUIRES_CDC (size={table_size_rows:,} > 100M)")
        
    elif pk_cols and pk_confidence < 0.90:
        # PK candidata com confiança baixa — SNAPSHOT mas com sugestão
        strategy = "SNAPSHOT"
        metadata = {
            "pk": pk_cols,
            "table_size_rows": table_size_rows,
            "reason": f"PK candidate found but confidence too low ({pk_confidence}). Validate uniqueness to enable CURRENT mode."
        }
        print(f"[DISCOVERY] ⚠️ Estratégia sugerida: SNAPSHOT (PK candidata com confiança baixa: {pk_confidence})")
        
    elif table_size_rows and table_size_rows < 1_000_000:
        # Estratégia C: SNAPSHOT (tabelas pequenas < 1M)
        strategy = "SNAPSHOT"
        metadata = {
            "table_size_rows": table_size_rows,
            "reason": "Small table, SNAPSHOT is efficient"
        }
        print(f"[DISCOVERY] ✅ Estratégia sugerida: SNAPSHOT (size={table_size_rows:,} < 1M)")
        
    else:
        # Estratégia D: APPEND_LOG (sem PK nem timestamp)
        strategy = "APPEND_LOG"
        metadata = {
            "table_size_rows": table_size_rows,
            "reason": "No PK and no reliable timestamp column"
        }
        print(f"[DISCOVERY] ✅ Estratégia sugerida: APPEND_LOG (sem PK/timestamp)")
    
    # Enriquecer metadata com informações do PK discovery
    if pk_source:
        metadata["pk_source"] = pk_source
        metadata["pk_confidence"] = pk_confidence
    if pk_discovery_details:
        metadata["pk_discovery_details"] = pk_discovery_details
    
    # ====================
    # AUDIT: strategy_decision_log
    # ====================
    # Gera entrada de log estruturada com a justificativa da decisão.
    # REQUISITO: ALTER TABLE {catalog}.ingestion_sys_ctrl.dataset_control
    #            ADD COLUMNS (strategy_decision_log STRING);
    from datetime import datetime, timezone
    decision_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "DISCOVERY_DECISION",
        "strategy": strategy,
        "inputs": {
            "watermark_col": watermark_col,
            "watermark_valid": watermark_valid,
            "pk_cols": pk_cols,
            "pk_source": pk_source,
            "pk_confidence": pk_confidence,
            "table_size_rows": table_size_rows,
        },
        "reason": metadata.get("reason", f"Auto-discovery selected {strategy}"),
    }
    metadata["_last_decision_log"] = decision_entry
    
    print(f"[DISCOVERY] ========== Discovery concluído ==========\n")
    
    return {
        "strategy": strategy,
        "metadata": metadata
    }


# ============================================================================
# 3. WATERMARK: Get Last Value (com suporte a override)
# ============================================================================

def _get_last_watermark(dataset_id: str, catalog: str) -> Optional[Any]:
    """
    Busca último valor de watermark para o dataset.
    
    PRIORITY: Verificar override_watermark_value primeiro (reprocessamento manual).
    Se NULL, usar watermark normal da tabela dataset_watermark.
    
    Returns:
        Valor tipado (TIMESTAMP ou NUMERIC), NÃO string.
        NULL se primeira execução.
    """
    ctrl_schema = f"{catalog}.ingestion_sys_ctrl"
    ops_schema = f"{catalog}.ingestion_sys_ops"
    
    try:
        # PRIORITY: Verificar override manual
        # type: ignore[name-defined]
        override_rows = spark.sql(f"""
            SELECT override_watermark_value
            FROM {ctrl_schema}.dataset_control
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
            AND override_watermark_value IS NOT NULL
        """).collect()
        
        if override_rows and override_rows[0].override_watermark_value:
            override_val = str(override_rows[0].override_watermark_value)
            print(f"[WATERMARK] ⚠️ OVERRIDE MANUAL DETECTADO: {override_val}")
            # TODO: Converter para tipo apropriado (TIMESTAMP ou NUMERIC)
            return override_val
        
        # Normal: buscar watermark da tabela dataset_watermark
        # type: ignore[name-defined]
        wm_rows = spark.sql(f"""
            SELECT watermark_value, watermark_type
            FROM {ops_schema}.dataset_watermark
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
        """).collect()
        
        if not wm_rows:
            print(f"[WATERMARK] Primeira execução (sem watermark anterior)")
            return None
        
        wm_value = wm_rows[0].watermark_value
        wm_type = wm_rows[0].watermark_type
        
        print(f"[WATERMARK] Último watermark: {wm_value} (type={wm_type})")
        return wm_value  # Já está tipado no banco
        
    except Exception as e:
        print(f"[WATERMARK] Erro ao buscar watermark: {e}")
        return None


# ============================================================================
# 4. BRONZE: Adicionar Colunas Técnicas
# ============================================================================

def _add_bronze_metadata_columns(
    df: DataFrame,
    batch_id: str,
    source_table: str,
    watermark_col: Optional[str],
    pk_cols: List[str],
    hash_exclude_cols: List[str]
) -> DataFrame:
    """
    Adiciona 8 colunas técnicas ao DataFrame da Bronze.
    
    Colunas:
    - _ingestion_ts: Timestamp da ingestão
    - _batch_id: UUID do run_id
    - _source_table: OWNER.TABLE@DBLINK
    - _op: INSERT/UPDATE/UPSERT
    - _watermark_col: Nome da coluna de watermark
    - _watermark_value: Valor do watermark (TIPADO: TIMESTAMP ou NUMERIC)
    - _row_hash: MD5 normalizado de colunas não-voláteis
    - _is_deleted: Flag de soft delete
    
    IMPORTANTE: Hash normalizado para evitar falsos positivos:
    - Decimals: formato string fixo
    - Timestamps: truncado para segundos
    - Strings: trim + upper (se case-insensitive)
    """
    from pyspark.sql.functions import current_timestamp, lit, md5, concat_ws, col, date_trunc, trim, upper
    
    # 1. _ingestion_ts e _batch_id
    df = df.withColumn("_ingestion_ts", current_timestamp())
    df = df.withColumn("_batch_id", lit(batch_id))
    df = df.withColumn("_source_table", lit(source_table))
    
    # 2. _op (por enquanto sempre UPSERT, futuramente detectar INSERT/UPDATE)
    df = df.withColumn("_op", lit("UPSERT"))
    
    # 3. _watermark_col e _watermark_value
    df = df.withColumn("_watermark_col", lit(watermark_col) if watermark_col else lit(None).cast(StringType()))
    
    if watermark_col and watermark_col in df.columns:
        # IMPORTANTE: Manter tipo original (TIMESTAMP ou NUMERIC), não converter para string
        df = df.withColumn("_watermark_value", col(watermark_col).cast(StringType()))
    else:
        df = df.withColumn("_watermark_value", lit(None).cast(StringType()))
    
    # 4. _row_hash: MD5 normalizado de colunas não-voláteis
    # Excluir: colunas técnicas (_*) + colunas voláteis + watermark
    business_cols = [
        c for c in df.columns 
        if not c.startswith('_') and c.upper() not in hash_exclude_cols and c != watermark_col
    ]
    
    if business_cols:
        # Normalizar colunas antes de hash
        normalized_cols = []
        for col_name in business_cols:
            col_obj = col(col_name)
            col_type = df.schema[col_name].dataType
            
            if isinstance(col_type, DecimalType):
                # Decimals: cast para string com precisão fixa
                normalized = col_obj.cast(StringType())
            elif isinstance(col_type, TimestampType):
                # Timestamps: truncar para segundos
                normalized = date_trunc("second", col_obj).cast(StringType())
            else:
                # Outros: cast para string
                normalized = col_obj.cast(StringType())
            
            normalized_cols.append(normalized)
        
        # Concatenar e gerar MD5
        df = df.withColumn("_row_hash", md5(concat_ws("|", *normalized_cols)))
    else:
        df = df.withColumn("_row_hash", lit(None).cast(StringType()))
    
    # 5. _is_deleted (padrão: false)
    df = df.withColumn("_is_deleted", lit(False))
    
    print(f"[METADATA] ✓ Adicionadas 8 colunas técnicas")
    print(f"[METADATA] Hash calculado a partir de {len(business_cols)} colunas de negócio")
    print(f"[METADATA] Excluídas do hash: {hash_exclude_cols}")
    
    return df


# ============================================================================
# 5. MERGE: Bronze por PK (com dedupe watermark >=)
# ============================================================================

def _merge_bronze_by_pk(
    df: DataFrame,
    bronze_table: str,
    pk_cols: List[str],
    watermark_col: Optional[str] = None
) -> int:
    """
    MERGE incremental na Bronze por Primary Key.
    
    IMPORTANTE: Dedupe por PK ANTES do merge para evitar perda de dados com watermark >=.
    - Se houver watermark: manter registro com maior valor de watermark
    - Senão: manter último registro (order by _ingestion_ts)
    
    WHEN MATCHED: UPDATE all + _op='UPDATE'
    WHEN NOT MATCHED: INSERT all + _op='INSERT'
    
    Returns:
        Número de registros processados (count do DataFrame de entrada)
    """
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number, col
    
    if not pk_cols:
        raise ValueError("pk_cols é obrigatório para MERGE por PK")
    
    print(f"[MERGE_PK] Iniciando MERGE por PK: {pk_cols}")
    print(f"[MERGE_PK] Watermark column: {watermark_col}")
    
    # DEDUPE: Manter apenas 1 registro por PK
    if watermark_col and watermark_col in df.columns:
        # Dedupe por maior watermark
        window = Window.partitionBy(*[col(c) for c in pk_cols]).orderBy(col(watermark_col).desc_nulls_last())
        print(f"[MERGE_PK] Aplicando dedupe por PK + maior watermark")
    else:
        # Dedupe por _ingestion_ts mais recente
        window = Window.partitionBy(*[col(c) for c in pk_cols]).orderBy(col("_ingestion_ts").desc())
        print(f"[MERGE_PK] Aplicando dedupe por PK + _ingestion_ts")
    
    df_deduped = (
        df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    
    deduped_count = df.count()
    final_count = df_deduped.count()
    if deduped_count != final_count:
        print(f"[MERGE_PK] ⚠️ Dedupe removeu {deduped_count - final_count} duplicatas")
    
    # Criar tabela se não existir
    if not spark.catalog.tableExists(bronze_table):  # type: ignore[name-defined]
        print(f"[MERGE_PK] Criando tabela Bronze: {bronze_table}")
        df_deduped.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        return final_count
    
    # MERGE
    dt = DeltaTable.forName(spark, bronze_table)  # type: ignore[name-defined]
    
    # Condição de join por PK
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])
    
    print(f"[MERGE_PK] Executando MERGE com condição: {merge_condition}")
    
    (
        dt.alias("target")
        .merge(
            df_deduped.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()  # UPDATE todos campos
        .whenNotMatchedInsertAll()  # INSERT novos registros
        .execute()
    )
    
    print(f"[MERGE_PK] ✓ MERGE concluído: {final_count} registros processados")
    return final_count


# ============================================================================
# 6. MERGE: Bronze por Hash (skip se hash igual)
# ============================================================================

def _merge_bronze_by_hash(
    df: DataFrame,
    bronze_table: str,
    pk_cols: List[str]
) -> int:
    """
    MERGE incremental na Bronze por Hash Comparison.
    
    Usado quando não há watermark confiável, mas existe PK.
    "Full refresh incrementalizado" - lê tudo, mas só atualiza se hash mudou.
    
    WHEN MATCHED AND hash diferente: UPDATE all + _op='UPDATE'
    WHEN MATCHED AND hash igual: SKIP (sem UPDATE - economia de I/O)
    WHEN NOT MATCHED: INSERT all + _op='INSERT'
    
    Returns:
        Número de registros processados
    """
    if not pk_cols:
        raise ValueError("pk_cols é obrigatório para MERGE por hash")
    
    print(f"[MERGE_HASH] Iniciando MERGE por hash comparison")
    print(f"[MERGE_HASH] PK: {pk_cols}")
    
    # Criar tabela se não existir
    if not spark.catalog.tableExists(bronze_table):  # type: ignore[name-defined]
        print(f"[MERGE_HASH] Criando tabela Bronze: {bronze_table}")
        df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        return df.count()
    
    # MERGE com condição de hash
    dt = DeltaTable.forName(spark, bronze_table)  # type: ignore[name-defined]
    
    # Condição de join por PK
    pk_condition = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])
    
    # Condição de hash diferente
    hash_condition = "target._row_hash <> source._row_hash OR target._row_hash IS NULL"
    
    print(f"[MERGE_HASH] Condição PK: {pk_condition}")
    print(f"[MERGE_HASH] Condição hash: {hash_condition}")
    
    (
        dt.alias("target")
        .merge(
            df.alias("source"),
            pk_condition
        )
        .whenMatchedUpdate(
            condition=hash_condition,  # Só atualiza se hash mudou
            set={c: f"source.{c}" for c in df.columns}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"[MERGE_HASH] ✓ MERGE concluído (só atualizou registros com hash diferente)")
    return df.count()


# ============================================================================
# 7. OPTIMIZE: Condicional (apenas se atingiu threshold de merges)
# ============================================================================

def _optimize_bronze_table_conditional(
    bronze_table: str,
    pk_cols: List[str],
    dataset_id: str,
    catalog: str
) -> bool:
    """
    OPTIMIZE ZORDER condicional da tabela Bronze.
    
    Só executa se:
    - merge_count_since_optimize > optimize_threshold_merges (padrão: 100)
    - OU last_optimize_at é NULL (primeira vez)
    
    Após OPTIMIZE:
    - Reseta merge_count_since_optimize = 0
    - Atualiza last_optimize_at = current_timestamp()
    
    Returns:
        True se OPTIMIZE foi executado, False se foi pulado
    """
    ctrl_schema = f"{catalog}.ingestion_sys_ctrl"
    
    try:
        # Verificar se precisa OPTIMIZE
        # type: ignore[name-defined]
        check_rows = spark.sql(f"""
            SELECT 
                merge_count_since_optimize,
                optimize_threshold_merges,
                last_optimize_at
            FROM {ctrl_schema}.dataset_control
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
        """).collect()
        
        if not check_rows:
            print(f"[OPTIMIZE] Dataset não encontrado: {dataset_id}")
            return False
        
        merge_count = int(check_rows[0].merge_count_since_optimize or 0)
        threshold = int(check_rows[0].optimize_threshold_merges or 100)
        last_optimize = check_rows[0].last_optimize_at
        
        # Decidir se executa
        should_optimize = merge_count >= threshold or last_optimize is None
        
        if not should_optimize:
            print(f"[OPTIMIZE] Pulando OPTIMIZE ({merge_count}/{threshold} merges)")
            return False
        
        print(f"[OPTIMIZE] Executando OPTIMIZE ZORDER em {bronze_table}...")
        print(f"[OPTIMIZE] Motivo: {merge_count} merges desde último OPTIMIZE (threshold={threshold})")
        
        # Executar OPTIMIZE ZORDER
        if pk_cols:
            zorder_cols = ", ".join(pk_cols)
            # type: ignore[name-defined]
            spark.sql(f"OPTIMIZE {bronze_table} ZORDER BY ({zorder_cols})")
            print(f"[OPTIMIZE] ✓ OPTIMIZE ZORDER BY ({zorder_cols}) concluído")
        else:
            # type: ignore[name-defined]
            spark.sql(f"OPTIMIZE {bronze_table}")
            print(f"[OPTIMIZE] ✓ OPTIMIZE (sem ZORDER) concluído")
        
        # Resetar contador
        # type: ignore[name-defined]
        spark.sql(f"""
            UPDATE {ctrl_schema}.dataset_control
            SET 
                merge_count_since_optimize = 0,
                last_optimize_at = current_timestamp()
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
        """)
        
        print(f"[OPTIMIZE] ✓ Contador resetado para 0")
        return True
        
    except Exception as e:
        print(f"[OPTIMIZE] Erro: {e}")
        return False


# ============================================================================
# 8. RECONCILE: Deletes com LEFT ANTI JOIN (opt-in)
# ============================================================================

def _reconcile_deletes(
    dataset_id: str,
    oracle_table: str,
    bronze_table: str,
    pk_cols: List[str],
    jdbc_url: str,
    user: str,
    pwd: str,
    catalog: str
) -> Dict[str, Any]:
    """
    Reconciliação de deletes: marca registros deletados no Oracle.
    
    PRÉ-REQUISITO: enable_reconciliation = TRUE no dataset_control.
    Recomendado APENAS para dimensões pequenas (< 1M rows).
    
    Processo:
    1. Buscar todas PKs do Oracle
    2. LEFT ANTI JOIN com Bronze (NUNCA NOT IN)
    3. Marcar registros ausentes: _is_deleted = true
    4. ALERTA se > 10% da tabela foi marcada como deletada
    
    Returns:
        {
            "deleted_count": 123,
            "total_count": 10000,
            "delete_ratio": 0.0123,
            "alert": False/True
        }
    """
    ctrl_schema = f"{catalog}.ingestion_sys_ctrl"
    
    print(f"[RECONCILE] Iniciando reconciliação de deletes para {bronze_table}")
    
    # Verificar se reconciliação está habilitada
    # type: ignore[name-defined]
    enable_rows = spark.sql(f"""
        SELECT enable_reconciliation
        FROM {ctrl_schema}.dataset_control
        WHERE dataset_id = {_sql_string_literal(dataset_id)}
    """).collect()
    
    if not enable_rows or not enable_rows[0].enable_reconciliation:
        print(f"[RECONCILE] Reconciliação DESABILITADA para este dataset")
        return {"deleted_count": 0, "total_count": 0, "delete_ratio": 0.0, "alert": False}
    
    try:
        # Parse Oracle table
        base = oracle_table.split('@')[0].strip()
        parts = [p for p in base.split('.') if p]
        if len(parts) != 2:
            raise ValueError(f"Invalid oracle_table format: {oracle_table}")
        
        owner, table = parts[0].upper(), parts[1].upper()
        
        # 1. Buscar PKs do Oracle
        pk_select = ", ".join(pk_cols)
        oracle_query = f"(SELECT {pk_select} FROM {oracle_table}) oracle_pks"
        
        print(f"[RECONCILE] Lendo PKs do Oracle: {oracle_query}")
        
        # type: ignore[name-defined]
        df_oracle_pks = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", oracle_query)
            .option("user", user)
            .option("password", pwd)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        
        # 2. LEFT ANTI JOIN com Bronze (registros na Bronze que NÃO estão no Oracle)
        df_bronze = spark.table(bronze_table)  # type: ignore[name-defined]
        
        # Condição de join por PK
        join_condition = [df_bronze[c] == df_oracle_pks[c] for c in pk_cols]
        
        print(f"[RECONCILE] Executando LEFT ANTI JOIN (NUNCA NOT IN!)")
        
        df_deleted = (
            df_bronze
            .filter(df_bronze._is_deleted == False)  # Apenas registros ativos
            .join(df_oracle_pks, on=join_condition, how="left_anti")  # Registros ausentes no Oracle
        )
        
        deleted_count = df_deleted.count()
        total_count = df_bronze.filter(df_bronze._is_deleted == False).count()
        
        delete_ratio = deleted_count / total_count if total_count > 0 else 0.0
        
        print(f"[RECONCILE] Registros a marcar como deletados: {deleted_count:,} de {total_count:,} ({delete_ratio:.2%})")
        
        # 3. Marcar como deletado (se houver registros)
        if deleted_count > 0:
            # Gerar lista de PKs para UPDATE
            pks_to_delete = df_deleted.select(*pk_cols).collect()
            
            # Construir condição WHERE
            if len(pk_cols) == 1:
                # PK simples
                pk_values = [_sql_string_literal(str(row[pk_cols[0]])) for row in pks_to_delete]
                where_clause = f"{pk_cols[0]} IN ({','.join(pk_values)})"
            else:
                # PK composta (usar OR com condições AND)
                conditions = []
                for row in pks_to_delete:
                    cond = " AND ".join([f"{c} = {_sql_string_literal(str(row[c]))}" for c in pk_cols])
                    conditions.append(f"({cond})")
                where_clause = " OR ".join(conditions)
            
            # UPDATE Bronze
            # type: ignore[name-defined]
            spark.sql(f"""
                UPDATE {bronze_table}
                SET _is_deleted = true
                WHERE {where_clause}
            """)
            
            print(f"[RECONCILE] ✓ {deleted_count} registros marcados como _is_deleted = true")
        else:
            print(f"[RECONCILE] ✓ Nenhum registro deletado detectado")
        
        # 4. ALERTA se > 10% deletado
        alert = delete_ratio > 0.10
        if alert:
            print(f"[RECONCILE] ⚠️ ALERTA: {delete_ratio:.2%} da tabela foi marcada como deletada (> 10%)!")
            print(f"[RECONCILE] Verifique se houve erro na reconciliação ou se é esperado.")
        
        # Atualizar last_reconciliation_at
        # type: ignore[name-defined]
        spark.sql(f"""
            UPDATE {ctrl_schema}.dataset_control
            SET last_reconciliation_at = current_timestamp()
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
        """)
        
        return {
            "deleted_count": deleted_count,
            "total_count": total_count,
            "delete_ratio": delete_ratio,
            "alert": alert
        }
        
    except Exception as e:
        print(f"[RECONCILE] Erro: {e}")
        return {"deleted_count": 0, "total_count": 0, "delete_ratio": 0.0, "alert": False, "error": str(e)}


# ============================================================================
# 9. ORQUESTRADOR: Load Oracle Bronze Incremental (função mãe)
# ============================================================================

def _load_oracle_bronze_incremental(
    dataset_id: str,
    dataset_name: str,
    connection_id: str,
    bronze_table: str,
    run_id: str,
    catalog: str
) -> Dict[str, Any]:
    """
    NOVA função: Carga incremental Oracle → Bronze com discovery automático.
    
    Fluxo:
    1. Carregar configuração de estratégia do dataset_control
    2. Se discovery_status = PENDING: executar discovery, salvar sugestão
    3. Se enable_incremental = FALSE: retornar None (usar fallback _load_oracle_bronze)
    4. Se enable_incremental = TRUE:
       a. Buscar último watermark
       b. Ler Oracle com filtro incremental
       c. Adicionar colunas técnicas
       d. Aplicar MERGE/APPEND conforme bronze_mode
       e. Incrementar merge_count_since_optimize
       f. Verificar se precisa OPTIMIZE
    
    Returns:
        {
            "oracle_table": "OWNER.TABLE@DBLINK",
            "bronze_row_count": 1234,
            "strategy": "WATERMARK",
            "incremental": True,
            "optimize_executed": False
        }
        OU None (se enable_incremental=FALSE, usar fallback)
    """
    ctrl_schema = f"{catalog}.ingestion_sys_ctrl"
    
    print(f"[INCREMENTAL] ========== Load Oracle Bronze Incremental ==========")
    print(f"[INCREMENTAL] Dataset: {dataset_name}")
    
    # 1. Carregar configuração
    # type: ignore[name-defined]
    config_rows = spark.sql(f"""
        SELECT 
            enable_incremental,
            incremental_strategy,
            incremental_metadata,
            bronze_mode,
            discovery_status,
            discovery_suggestion
        FROM {ctrl_schema}.dataset_control
        WHERE dataset_id = {_sql_string_literal(dataset_id)}
    """).collect()
    
    if not config_rows:
        print(f"[INCREMENTAL] Erro: Dataset não encontrado")
        return None
    
    config = config_rows[0]
    enable_incremental = bool(config.enable_incremental) if config.enable_incremental is not None else False
    strategy = config.incremental_strategy
    metadata_json = config.incremental_metadata
    bronze_mode = config.bronze_mode
    discovery_status = config.discovery_status
    
    print(f"[INCREMENTAL] enable_incremental = {enable_incremental} (type={type(enable_incremental).__name__})")
    print(f"[INCREMENTAL] strategy = {strategy}")
    print(f"[INCREMENTAL] bronze_mode = {bronze_mode}")
    print(f"[INCREMENTAL] discovery_status = {discovery_status}")
    
    # 2. Discovery (se PENDING)
    if discovery_status == "PENDING":
        print(f"[INCREMENTAL] Discovery PENDING, executando discovery...")
        
        # Buscar conexão Oracle
        # type: ignore[name-defined]
        conn_rows = spark.sql(f"""
            SELECT jdbc_url, secret_scope, secret_user_key, secret_pwd_key
            FROM {ctrl_schema}.connections_oracle
            WHERE connection_id = {_sql_string_literal(connection_id)}
        """).collect()
        
        if conn_rows:
            jdbc_url = conn_rows[0].jdbc_url
            secret_scope = conn_rows[0].secret_scope
            user = dbutils.secrets.get(secret_scope, conn_rows[0].secret_user_key)  # type: ignore[name-defined]
            pwd = dbutils.secrets.get(secret_scope, conn_rows[0].secret_pwd_key)  # type: ignore[name-defined]
            
            # Executar discovery
            discovery_result = _discover_incremental_strategy(
                dataset_id=dataset_id,
                oracle_table=dataset_name,
                jdbc_url=jdbc_url,
                user=user,
                pwd=pwd,
                catalog=catalog
            )
            
            # Salvar sugestão (NÃO ativa automaticamente)
            metadata_json_str = json.dumps(discovery_result["metadata"], ensure_ascii=False)
            
            # Append decision log (preservar histórico se coluna existe)
            decision_log_entry = discovery_result["metadata"].get("_last_decision_log", {})
            decision_log_json = json.dumps(decision_log_entry, ensure_ascii=False) if decision_log_entry else None
            
            # type: ignore[name-defined]
            # Tentar atualizar com strategy_decision_log (se coluna já existir)
            try:
                spark.sql(f"""
                    UPDATE {ctrl_schema}.dataset_control
                    SET 
                        discovery_suggestion = {_sql_string_literal(discovery_result["strategy"])},
                        incremental_metadata = {_sql_string_literal(metadata_json_str)},
                        discovery_status = 'PENDING_CONFIRMATION',
                        last_discovery_at = current_timestamp(),
                        strategy_decision_log = CASE
                            WHEN strategy_decision_log IS NULL THEN {_sql_string_literal('[' + json.dumps(decision_log_entry, ensure_ascii=False) + ']')}
                            ELSE CONCAT(
                                SUBSTRING(strategy_decision_log, 1, LENGTH(strategy_decision_log) - 1),
                                ',', {_sql_string_literal(json.dumps(decision_log_entry, ensure_ascii=False))}, ']'
                            )
                        END
                    WHERE dataset_id = {_sql_string_literal(dataset_id)}
                """)
            except Exception as log_err:
                # Fallback: coluna strategy_decision_log ainda não existe
                print(f"[INCREMENTAL] strategy_decision_log não disponível (execute ALTER TABLE): {log_err}")
                spark.sql(f"""
                    UPDATE {ctrl_schema}.dataset_control
                    SET 
                        discovery_suggestion = {_sql_string_literal(discovery_result["strategy"])},
                        incremental_metadata = {_sql_string_literal(metadata_json_str)},
                        discovery_status = 'PENDING_CONFIRMATION',
                        last_discovery_at = current_timestamp()
                    WHERE dataset_id = {_sql_string_literal(dataset_id)}
                """)
            
            print(f"[INCREMENTAL] Discovery concluído: sugestão = {discovery_result['strategy']}")
            print(f"[INCREMENTAL] Aguardando confirmação do usuário para ativar incremental")
        else:
            print(f"[INCREMENTAL] Conexão Oracle não encontrada: {connection_id}")
    
    # 3. Verificar se incremental está habilitado
    if not enable_incremental:
        print(f"[INCREMENTAL] enable_incremental = FALSE, usando fallback (_load_oracle_bronze)")
        return None  # Sinal para usar função original
    
    # 4. Executar carga incremental
    print(f"[INCREMENTAL] enable_incremental = TRUE")
    print(f"[INCREMENTAL] Estratégia: {strategy}")
    print(f"[INCREMENTAL] Bronze mode: {bronze_mode}")
    
    # Parse metadata
    metadata = json.loads(metadata_json) if metadata_json else {}
    watermark_col = metadata.get("watermark_column") or metadata.get("watermark_col")
    pk_cols = metadata.get("pk", [])
    hash_exclude_cols = metadata.get("hash_exclude_cols", [])
    
    # ===========================
    # GUARDRAIL: CURRENT sem PK → fallback SNAPSHOT com log
    # ===========================
    # Defesa em profundidade: se a API não bloqueou, o orquestrador faz fallback seguro.
    if bronze_mode == "CURRENT" and not pk_cols:
        print(f"[INCREMENTAL] ⚠️ POLICY_VIOLATION: CURRENT mode requires PK - dataset_id={dataset_id}")
        print(f"[INCREMENTAL] ⚠️ Fallback automático para SNAPSHOT (OVERWRITE) para evitar falha silenciosa")
        bronze_mode = "SNAPSHOT"  # Override local para esta execução
        # Registrar fallback no dataset_control (só se não está locked)
        try:
            # type: ignore[name-defined]
            spark.sql(f"""
                UPDATE {ctrl_schema}.dataset_control
                SET bronze_mode = 'SNAPSHOT',
                    updated_at = current_timestamp(),
                    updated_by = 'policy_enforcement'
                WHERE dataset_id = {_sql_string_literal(dataset_id)}
                  AND bronze_mode = 'CURRENT'
                  AND (strategy_locked IS NULL OR strategy_locked = FALSE)
            """)
            print(f"[INCREMENTAL] bronze_mode persistido como SNAPSHOT no dataset_control")
        except Exception as pol_err:
            print(f"[INCREMENTAL] ⚠️ Erro ao registrar policy violation: {pol_err}")
    
    # ===========================
    # AUTO-PROMOÇÃO: SNAPSHOT → WATERMARK
    # ===========================
    # Quando o usuário configura enable_incremental + bronze_mode=CURRENT + watermark_col,
    # mas a strategy ficou como SNAPSHOT (discovery auto-detectou), promovemos automaticamente.
    if strategy == "SNAPSHOT" and bronze_mode == "CURRENT" and watermark_col:
        print(f"[INCREMENTAL] ⚡ AUTO-PROMOÇÃO: strategy SNAPSHOT → WATERMARK")
        print(f"[INCREMENTAL]    Motivo: enable_incremental=True + bronze_mode=CURRENT + watermark_col={watermark_col}")
        strategy = "WATERMARK"
        # Persistir a promoção para futuras execuções
        # type: ignore[name-defined]
        spark.sql(f"""
            UPDATE {ctrl_schema}.dataset_control
            SET incremental_strategy = 'WATERMARK'
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
              AND incremental_strategy = 'SNAPSHOT'
              AND (strategy_locked IS NULL OR strategy_locked = FALSE)
        """)
        print(f"[INCREMENTAL]    Strategy atualizada no dataset_control (se não estava locked)")
    
    # Buscar conexão Oracle
    # type: ignore[name-defined]
    conn_rows = spark.sql(f"""
        SELECT jdbc_url, secret_scope, secret_user_key, secret_pwd_key
        FROM {ctrl_schema}.connections_oracle
        WHERE connection_id = {_sql_string_literal(connection_id)}
    """).collect()
    
    if not conn_rows:
        raise ValueError(f"Conexão Oracle não encontrada: {connection_id}")
    
    jdbc_url = conn_rows[0].jdbc_url
    secret_scope = conn_rows[0].secret_scope
    user = dbutils.secrets.get(secret_scope, conn_rows[0].secret_user_key)  # type: ignore[name-defined]
    pwd = dbutils.secrets.get(secret_scope, conn_rows[0].secret_pwd_key)  # type: ignore[name-defined]
    
    # 4a. Determinar filtro de leitura Oracle
    oracle_table = dataset_name
    last_watermark = None
    lookback_days = int(metadata.get("lookback_days", 3))
    watermark_cutoff = None  # Data de corte efetiva usada na leitura
    short_circuited = False  # Flag: carga pulada por ausência de dados novos
    
    if strategy == "WATERMARK" and watermark_col:
        if pk_cols:
            # -------------------------------------------------------
            # CENÁRIO A: COM PK → inclusive inequality (>=) com lookback
            # safety net para late-arriving data
            #
            # MELHORIA v2 (padronizado Fivetran/Airbyte/dbt):
            # 1. Usa >= (inclusive) — padrão de mercado para CDC/watermark
            #    MERGE idempotente absorve releitura na fronteira
            # 2. Aplica lookback_days como rede de segurança para
            #    capturar late-arriving data (dados retroativos)
            # 3. Short-circuit: verifica se há dados novos ANTES de
            #    fazer o SELECT completo (query leve com > estrito)
            # -------------------------------------------------------
            last_watermark = _get_last_watermark(dataset_id, catalog)
            print(f"[INCREMENTAL] DEBUG: _get_last_watermark returned: {last_watermark!r}")
            
            if last_watermark:
                wm_str = str(last_watermark)
                
                # MELHORIA 1+2: Cutoff = watermark - lookback_days (inclusive >=)
                # Calcula cutoff em Python para evitar problemas com INTERVAL via JDBC Oracle
                from datetime import datetime as _dt, timedelta as _td
                try:
                    _wm_dt = _dt.strptime(wm_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    _cutoff_dt = _wm_dt - _td(days=lookback_days)
                    _cutoff_str = _cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    _cutoff_str = wm_str  # Fallback: usar watermark original
                
                cutoff_expr = f"TO_TIMESTAMP('{_cutoff_str}', 'YYYY-MM-DD HH24:MI:SS')"
                print(f"[INCREMENTAL] COM PK: watermark={wm_str}, lookback_days={lookback_days}")
                print(f"[INCREMENTAL] COM PK: cutoff calculado = {_cutoff_str} (watermark - {lookback_days} dias)")
                
                # MELHORIA 3: Short-circuit check — query leve para verificar
                # se há dados NOVOS (acima do watermark salvo) antes de ler tudo
                try:
                    sc_query = f"(SELECT MAX({watermark_col}) AS max_wm FROM {oracle_table} WHERE {watermark_col} > TO_TIMESTAMP('{wm_str}', 'YYYY-MM-DD HH24:MI:SS')) sc"
                    print(f"[INCREMENTAL] SHORT-CIRCUIT: Verificando dados novos acima do watermark...")
                    sc_df = (
                        spark.read.format("jdbc")  # type: ignore[name-defined]
                        .option("url", jdbc_url)
                        .option("dbtable", sc_query)
                        .option("user", user)
                        .option("password", pwd)
                        .option("driver", "oracle.jdbc.OracleDriver")
                        .load()
                    )
                    sc_rows = sc_df.collect()
                    has_new_data = bool(sc_rows and sc_rows[0]["max_wm"] is not None)
                    sc_max = str(sc_rows[0]["max_wm"]) if has_new_data else None
                    print(f"[INCREMENTAL] SHORT-CIRCUIT: has_new_data={has_new_data}, max_wm_na_origem={sc_max}")
                except Exception as sc_err:
                    # Se o short-circuit falhar, assume que há dados (fallback seguro)
                    has_new_data = True
                    print(f"[INCREMENTAL] SHORT-CIRCUIT: Erro na verificação (prosseguindo com leitura): {sc_err}")
                
                if not has_new_data:
                    # Nenhum dado novo acima do watermark — pular carga pesada
                    # Ainda lê com lookback como safety net (late-arriving data)
                    # mas usa query otimizada
                    print(f"[INCREMENTAL] SHORT-CIRCUIT: ⚡ Nenhum dado NOVO acima do watermark")
                    print(f"[INCREMENTAL] SHORT-CIRCUIT: Verificando late-arriving data (lookback {lookback_days} dias)...")
                    short_circuited = True
                
                # Query final: >= (inclusive, padrão mercado) com lookback safety net
                query = f"(SELECT * FROM {oracle_table} WHERE {watermark_col} >= {cutoff_expr}) src"
                watermark_cutoff = f"{wm_str} - {lookback_days} dias"
                print(f"[INCREMENTAL] COM PK: WHERE {watermark_col} >= (watermark - {lookback_days} dias)")
                print(f"[INCREMENTAL] COM PK: Query efetiva: WHERE {watermark_col} >= TO_TIMESTAMP('{_cutoff_str}')")
            else:
                # Primeira execução com PK: full read
                query = f"(SELECT * FROM {oracle_table}) src"
                print(f"[INCREMENTAL] COM PK: Primeira execução (sem watermark anterior) → leitura completa")
        else:
            # -------------------------------------------------------
            # CENÁRIO B: SEM PK → lookback rolling window
            # -------------------------------------------------------
            query = f"(SELECT * FROM {oracle_table} WHERE {watermark_col} >= SYSDATE - {lookback_days}) src"
            watermark_cutoff = f"CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS"
            print(f"[INCREMENTAL] SEM PK: WHERE {watermark_col} >= SYSDATE - {lookback_days} (lookback {lookback_days} dias)")
    else:
        # HASH_MERGE, APPEND_LOG, ou sem watermark
        query = f"(SELECT * FROM {oracle_table}) src"
        print(f"[INCREMENTAL] Leitura completa (estratégia={strategy})")
    
    # type: ignore[name-defined]
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", user)
        .option("password", pwd)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("fetchsize", "10000")
        .load()
    )
    
    row_count = df.count()
    print(f"[INCREMENTAL] Registros lidos do Oracle: {row_count:,}")
    
    # SHORT-CIRCUIT: Se não há dados novos E lookback também retornou 0 → skip Bronze/Silver
    if short_circuited and row_count == 0:
        print(f"[INCREMENTAL] ⚡ SHORT-CIRCUIT ATIVADO: 0 registros lidos, pulando MERGE Bronze/Silver")
        print(f"[INCREMENTAL] ⚡ Watermark permanece em: {last_watermark}")
        print(f"[INCREMENTAL] ========== Load Incremental Concluído (short-circuit) ==========")
        return {
            "oracle_table": oracle_table,
            "bronze_row_count": 0,
            "strategy": strategy,
            "incremental": True,
            "optimize_executed": False,
            "watermark_start": str(last_watermark) if last_watermark else None,
            "watermark_end": str(last_watermark) if last_watermark else None,
            "short_circuited": True,
        }
    
    if short_circuited and row_count > 0:
        print(f"[INCREMENTAL] ⚠️ SHORT-CIRCUIT: Detectado late-arriving data! {row_count:,} registros no lookback window")
    
    # 4b.1 PRÉ-COMPUTAR max watermark ANTES do MERGE (evita re-leitura JDBC após MERGE)
    saved_watermark_end = None
    if pk_cols and watermark_col and bronze_mode == "CURRENT":
        try:
            max_wm_val = df.agg(F.max(F.col(watermark_col)).alias("max_wm")).collect()[0]["max_wm"]
            if max_wm_val is not None:
                saved_watermark_end = str(max_wm_val)
                print(f"[INCREMENTAL] 📊 Max watermark pré-computado: {watermark_col} = {saved_watermark_end}")
        except Exception as pre_wm_err:
            print(f"[INCREMENTAL] ⚠️ Erro ao pré-computar max watermark: {pre_wm_err}")
    
    # 4c. Adicionar colunas técnicas
    df_enriched = _add_bronze_metadata_columns(
        df=df,
        batch_id=run_id,
        source_table=dataset_name,
        watermark_col=watermark_col,
        pk_cols=pk_cols,
        hash_exclude_cols=hash_exclude_cols
    )
    
    # 4d. Aplicar escrita conforme bronze_mode + presença de PK
    if bronze_mode == "CURRENT":
        if pk_cols:
            # -------------------------------------------------------
            # CENÁRIO A: COM PK → MERGE por PK (upsert)
            # -------------------------------------------------------
            if strategy == "HASH_MERGE":
                _merge_bronze_by_hash(df_enriched, bronze_table, pk_cols)
            else:
                _merge_bronze_by_pk(df_enriched, bronze_table, pk_cols, watermark_col)
            print(f"[INCREMENTAL] ✓ MERGE por PK concluído: {row_count:,} registros")
        elif watermark_col:
            # -------------------------------------------------------
            # CENÁRIO B: SEM PK + watermark → replaceWhere atômico
            # Substitui apenas os registros dentro do range de datas
            # -------------------------------------------------------
            from pyspark.sql.functions import current_timestamp, expr
            
            cutoff_expr = f"{watermark_col} >= CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS"
            print(f"[INCREMENTAL] SEM PK: replaceWhere → {cutoff_expr}")
            
            (
                df_enriched.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", cutoff_expr)
                .option("mergeSchema", "true")
                .saveAsTable(bronze_table)
            )
            print(f"[INCREMENTAL] ✓ replaceWhere concluído: {row_count:,} registros substituídos no range")
        else:
            # Sem PK e sem watermark → OVERWRITE total (fallback seguro)
            print(f"[INCREMENTAL] ⚠️ Sem PK e sem watermark → OVERWRITE total")
            df_enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
            print(f"[INCREMENTAL] ✓ OVERWRITE concluído: {row_count:,} registros")
    elif bronze_mode == "APPEND_LOG":
        # APPEND: adiciona registros sem dedupe
        df_enriched.write.format("delta").mode("append").saveAsTable(bronze_table)
        print(f"[INCREMENTAL] ✓ APPEND concluído: {row_count:,} registros")
    else:
        # SNAPSHOT: substitui toda a tabela
        df_enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
        print(f"[INCREMENTAL] ✓ SNAPSHOT concluído: {row_count:,} registros")
    
    # 4e. Incrementar merge_count_since_optimize
    if bronze_mode == "CURRENT":
        # type: ignore[name-defined]
        spark.sql(f"""
            UPDATE {ctrl_schema}.dataset_control
            SET merge_count_since_optimize = merge_count_since_optimize + 1
            WHERE dataset_id = {_sql_string_literal(dataset_id)}
        """)
        print(f"[INCREMENTAL] merge_count_since_optimize incrementado")
    
    # 4f. Verificar se precisa OPTIMIZE
    optimize_executed = False
    if bronze_mode == "CURRENT" and pk_cols:
        optimize_executed = _optimize_bronze_table_conditional(
            bronze_table=bronze_table,
            pk_cols=pk_cols,
            dataset_id=dataset_id,
            catalog=catalog
        )
    
    # 4g. Salvar watermark (apenas COM PK - cenário A)
    # SEM PK usa lookback_days relativo, não precisa salvar watermark
    # saved_watermark_end já foi pré-computado em 4b.1 (antes do MERGE)
    if pk_cols and watermark_col and bronze_mode == "CURRENT" and saved_watermark_end:
        try:
            ops_schema = f"{catalog}.ingestion_sys_ops"
            # type: ignore[name-defined]
            spark.sql(f"""
                MERGE INTO {ops_schema}.dataset_watermark t
                USING (SELECT
                    {_sql_string_literal(dataset_id)} AS dataset_id,
                    'TIMESTAMP' AS watermark_type,
                    {_sql_string_literal(watermark_col)} AS watermark_column,
                    {_sql_string_literal(saved_watermark_end)} AS watermark_value,
                    {_sql_string_literal(run_id)} AS last_run_id
                ) s
                ON t.dataset_id = s.dataset_id
                WHEN MATCHED THEN UPDATE SET
                    t.watermark_type = s.watermark_type,
                    t.watermark_column = s.watermark_column,
                    t.watermark_value = s.watermark_value,
                    t.last_run_id = s.last_run_id,
                    t.last_updated_at = current_timestamp()
                WHEN NOT MATCHED THEN INSERT (
                    dataset_id, watermark_type, watermark_column, watermark_value, last_run_id, last_updated_at
                ) VALUES (
                    s.dataset_id, s.watermark_type, s.watermark_column, s.watermark_value, s.last_run_id, current_timestamp()
                )
            """)
            print(f"[INCREMENTAL] ✓ Watermark salvo: {watermark_col} = {saved_watermark_end}")
        except Exception as wm_err:
            print(f"[INCREMENTAL] ⚠️ Erro ao salvar watermark: {wm_err}")
    
    print(f"[INCREMENTAL] ========== Load Incremental Concluído ==========")
    
    # Capturar watermark_start de forma robusta (last_watermark OU watermark_cutoff)
    wm_start_value = None
    if last_watermark:
        wm_start_value = str(last_watermark)
    elif watermark_cutoff:
        wm_start_value = str(watermark_cutoff)
    
    print(f"[INCREMENTAL] DEBUG RETURN: last_watermark={last_watermark!r}, watermark_cutoff={watermark_cutoff!r}, wm_start_value={wm_start_value!r}, saved_watermark_end={saved_watermark_end!r}")
    
    return {
        "oracle_table": oracle_table,
        "bronze_row_count": row_count,
        "strategy": strategy,
        "incremental": True,
        "optimize_executed": optimize_executed,
        "watermark_start": wm_start_value,
        "watermark_end": saved_watermark_end,
    }
