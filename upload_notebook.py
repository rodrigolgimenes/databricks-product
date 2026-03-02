import os
import base64
import requests
import json

# Ler credenciais do .env
DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

# Ler arquivo do notebook
notebook_path = r"C:\dev\cm-databricks\databricks_notebooks\incremental_loading_functions.py"
with open(notebook_path, 'r', encoding='utf-8') as f:
    notebook_content = f.read()

# Encodar em base64
encoded_content = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')

# Criar diretório no workspace (ignorar se já existe)
mkdirs_url = f"{DATABRICKS_HOST}/api/2.0/workspace/mkdirs"
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}
mkdirs_payload = {
    "path": "/Workspace/Shared/incremental_loading"
}

print("Criando diretório no workspace...")
response = requests.post(mkdirs_url, headers=headers, json=mkdirs_payload)
if response.status_code == 200:
    print("✅ Diretório criado (ou já existe)")
else:
    print(f"⚠️ Status: {response.status_code} - {response.text}")

# Upload do notebook
upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
upload_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
    "format": "SOURCE",
    "language": "PYTHON",
    "content": encoded_content,
    "overwrite": True
}

print("\nFazendo upload do notebook...")
response = requests.post(upload_url, headers=headers, json=upload_payload)

if response.status_code == 200:
    print("✅ Notebook enviado com sucesso!")
    print(f"📍 Path: /Workspace/Shared/incremental_loading/incremental_loading_functions")
else:
    print(f"❌ Erro: {response.status_code}")
    print(response.text)
