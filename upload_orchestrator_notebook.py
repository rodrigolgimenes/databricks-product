import os
import base64
import requests

DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Verificar se orchestrator existe
print("🔍 Verificando se orchestrator existe...")
get_url = f"{DATABRICKS_HOST}/api/2.0/workspace/get-status"
get_payload = {
    "path": "/Workspace/Shared/incremental_loading/governed_ingestion_orchestrator"
}
response = requests.get(get_url, headers=headers, json=get_payload)
if response.status_code == 200:
    print("✅ Orchestrator já existe")
    print(f"   {response.json()}")
else:
    print(f"⚠️ Orchestrator não encontrado (status: {response.status_code})")

# Verificar arquivo local
orchestrator_path = r"C:\dev\cm-databricks\databricks_notebooks\governed_ingestion_orchestrator.py"
if not os.path.exists(orchestrator_path):
    print(f"❌ Arquivo local não encontrado: {orchestrator_path}")
    exit(1)

# Ler arquivo
with open(orchestrator_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Verificar header
if not content.startswith("# Databricks notebook source"):
    print("❌ Arquivo não tem header correto!")
    exit(1)

print("\n✅ Arquivo local válido")

# Verificar a linha do %run
lines = content.split('\n')
for i, line in enumerate(lines[:15], 1):
    if '%run' in line.lower():
        print(f"\nLinha {i} com %run: {repr(line)}")
        if line.strip().startswith('%run'):
            print("  ✅ %run está no início da linha")
        else:
            print("  ⚠️ %run NÃO está no início da linha!")

# Fazer upload
encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')

upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
upload_payload = {
    "path": "/Workspace/Shared/incremental_loading/governed_ingestion_orchestrator",
    "format": "SOURCE",
    "language": "PYTHON",
    "content": encoded_content,
    "overwrite": True
}

print("\n🔄 Enviando orchestrator...")
response = requests.post(upload_url, headers=headers, json=upload_payload)

if response.status_code == 200:
    print("✅ Orchestrator enviado com sucesso!")
    print("   Path: /Workspace/Shared/incremental_loading/governed_ingestion_orchestrator")
else:
    print(f"❌ Erro: {response.status_code}")
    print(response.text)
