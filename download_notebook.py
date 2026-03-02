import requests
import base64

DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Baixar o notebook incremental_loading_functions
print("📥 Baixando incremental_loading_functions...")
export_url = f"{DATABRICKS_HOST}/api/2.0/workspace/export"
export_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
    "format": "SOURCE"
}

response = requests.get(export_url, headers=headers, json=export_payload)
if response.status_code == 200:
    content = base64.b64decode(response.json()["content"]).decode('utf-8')
    
    # Salvar para inspeção
    with open("downloaded_incremental_loading.py", "w", encoding="utf-8") as f:
        f.write(content)
    
    # Mostrar primeiras linhas
    lines = content.split('\n')[:20]
    print("✅ Primeiras 20 linhas do arquivo:")
    for i, line in enumerate(lines, 1):
        print(f"{i:3d}: {repr(line)}")
    
    # Verificar se tem shebang correto do Databricks
    if lines and lines[0].strip() == "# Databricks notebook source":
        print("\n✅ Notebook tem header correto do Databricks")
    else:
        print("\n⚠️ Notebook NÃO tem header do Databricks!")
        print(f"Primeira linha: {repr(lines[0] if lines else 'VAZIO')}")
else:
    print(f"❌ Erro ao baixar: {response.status_code}")
    print(response.text)

# Também verificar o orchestrator
print("\n" + "="*80)
print("📥 Baixando governed_ingestion_orchestrator...")
export_payload_orch = {
    "path": "/Workspace/Shared/incremental_loading/governed_ingestion_orchestrator",
    "format": "SOURCE"
}

response_orch = requests.get(export_url, headers=headers, json=export_payload_orch)
if response_orch.status_code == 200:
    content_orch = base64.b64decode(response_orch.json()["content"]).decode('utf-8')
    
    # Mostrar primeiras linhas com o %run
    lines_orch = content_orch.split('\n')[:15]
    print("✅ Primeiras 15 linhas:")
    for i, line in enumerate(lines_orch, 1):
        print(f"{i:3d}: {repr(line)}")
        if '%run' in line.lower():
            print(f"      ^^^^ Linha com %run encontrada!")
else:
    print(f"⚠️ Orchestrator status: {response_orch.status_code}")
