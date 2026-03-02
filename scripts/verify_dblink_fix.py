"""
Verifica se a correção do DBLink está presente no código do orquestrador.
"""

import re

ORCHESTRATOR_PATH = "C:/dev/cm-databricks/databricks_notebooks/governed_ingestion_orchestrator.py"

def verify_dblink_fix():
    print("🔍 Verificando correção do DBLink no orquestrador...")
    
    with open(ORCHESTRATOR_PATH, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Check 1: Verificar se o bypass do DBLink existe em _oracle_table_exists
    check1_pattern = r"if dblink:\s+result\[\"exists\"\] = True"
    check1_found = bool(re.search(check1_pattern, content))
    
    # Check 2: Verificar se o bypass do DBLink existe em _oracle_estimate_num_rows
    check2_pattern = r"if dblink:\s+return None"
    check2_found = bool(re.search(check2_pattern, content))
    
    # Check 3: Verificar se NÃO está mais usando all_tables@dblink
    check3_pattern = r"all_tables\{suffix\}"
    check3_not_found = not bool(re.search(check3_pattern, content))
    
    print(f"\n✅ CHECK 1: Bypass DBLink em _oracle_table_exists() → {'OK' if check1_found else '❌ FALTANDO'}")
    print(f"✅ CHECK 2: Bypass DBLink em _oracle_estimate_num_rows() → {'OK' if check2_found else '❌ FALTANDO'}")
    print(f"✅ CHECK 3: Remoção de all_tables{{suffix}} → {'OK' if check3_not_found else '❌ AINDA PRESENTE'}")
    
    all_checks = check1_found and check2_found and check3_not_found
    
    if all_checks:
        print("\n🎉 SUCESSO: Todas as correções estão presentes!")
        print("📝 O orquestrador agora suporta DBLinks corretamente.")
        return True
    else:
        print("\n⚠️  ATENÇÃO: Algumas correções estão faltando!")
        print("Execute novamente os edits no orquestrador.")
        return False

if __name__ == "__main__":
    verify_dblink_fix()
