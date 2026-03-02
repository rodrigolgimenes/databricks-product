import os
import requests
import json

DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Tentar listar arquivos no diretório
print("🔍 Verificando arquivos em /Workspace/Shared/incremental_loading/...")
list_url = f"{DATABRICKS_HOST}/api/2.0/workspace/list"
list_payload = {
    "path": "/Workspace/Shared/incremental_loading"
}

response = requests.get(list_url, headers=headers, json=list_payload)
if response.status_code == 200:
    objects = response.json().get("objects", [])
    print(f"✅ Encontrados {len(objects)} objetos:")
    for obj in objects:
        print(f"  - {obj['path']} (tipo: {obj['object_type']})")
else:
    print(f"❌ Erro ao listar: {response.status_code}")
    print(response.text)

# Verificar se existe sem extensão
print("\n🔍 Verificando incremental_loading_functions (sem .py)...")
get_url = f"{DATABRICKS_HOST}/api/2.0/workspace/get-status"
get_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions"
}
response = requests.get(get_url, headers=headers, json=get_payload)
print(f"Status sem .py: {response.status_code}")
if response.status_code == 200:
    print(f"✅ Arquivo existe: {json.dumps(response.json(), indent=2)}")
else:
    print(f"❌ Não encontrado sem .py")

# Verificar se existe com extensão .py
print("\n🔍 Verificando incremental_loading_functions.py (com .py)...")
get_payload_py = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions.py"
}
response_py = requests.get(get_url, headers=headers, json=get_payload_py)
print(f"Status com .py: {response_py.status_code}")
if response_py.status_code == 200:
    print(f"✅ Arquivo existe: {json.dumps(response_py.json(), indent=2)}")
else:
    print(f"❌ Não encontrado com .py")
