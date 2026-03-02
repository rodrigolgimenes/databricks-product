import os
import base64
import requests

# Credenciais
DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

# Ler orquestrador atualizado
orchestrator_path = r"C:\dev\cm-databricks\databricks_notebooks\governed_ingestion_orchestrator.py"
with open(orchestrator_path, 'r', encoding='utf-8') as f:
    orchestrator_content = f.read()

# Encodar em base64
encoded_content = base64.b64encode(orchestrator_content.encode('utf-8')).decode('utf-8')

# Headers
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Upload do orquestrador para /Workspace/Shared/
upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
upload_payload = {
    "path": "/Workspace/Shared/governed_ingestion_orchestrator",
    "format": "SOURCE",
    "language": "PYTHON",
    "content": encoded_content,
    "overwrite": True
}

print("Fazendo upload do orquestrador governado (com integração incremental)...")
response = requests.post(upload_url, headers=headers, json=upload_payload)

if response.status_code == 200:
    print("✅ Orquestrador enviado com sucesso!")
    print(f"📍 Path: /Workspace/Shared/governed_ingestion_orchestrator")
    print("")
    print("⚡ O orquestrador agora:")
    print("  - Importa funções incrementais de /Workspace/Shared/incremental_loading/incremental_loading_functions")
    print("  - Verifica enable_incremental flag em cada dataset")
    print("  - Chama _load_oracle_bronze_incremental() quando habilitado")
    print("  - Fallback automático para full refresh em caso de erro")
else:
    print(f"❌ Erro: {response.status_code}")
    print(response.text)
