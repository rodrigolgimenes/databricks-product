import os
import base64
import requests

DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Ler arquivo local
notebook_path = r"C:\dev\cm-databricks\databricks_notebooks\incremental_loading_functions.py"
with open(notebook_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Verificar se tem o header correto
if not content.startswith("# Databricks notebook source"):
    print("❌ Arquivo não tem header correto do Databricks!")
    exit(1)

print("✅ Arquivo tem header correto")

# Verificar se tem separadores de células
if "# COMMAND ----------" not in content:
    print("⚠️ Arquivo NÃO tem separadores de células (# COMMAND ----------)")
    print("   Mas isso pode ser OK se for um arquivo de funções puro")
else:
    cell_count = content.count("# COMMAND ----------")
    print(f"✅ Arquivo tem {cell_count} separadores de células")

# Fazer upload
encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')

upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
upload_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
    "format": "SOURCE",
    "language": "PYTHON",
    "content": encoded_content,
    "overwrite": True
}

print("\n🔄 Re-enviando notebook...")
response = requests.post(upload_url, headers=headers, json=upload_payload)

if response.status_code == 200:
    print("✅ Notebook reenviado com sucesso!")
    
    # Verificar status
    get_url = f"{DATABRICKS_HOST}/api/2.0/workspace/get-status"
    get_payload = {
        "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions"
    }
    status_response = requests.get(get_url, headers=headers, json=get_payload)
    if status_response.status_code == 200:
        info = status_response.json()
        print(f"\n📊 Info do notebook:")
        print(f"   - Tipo: {info.get('object_type')}")
        print(f"   - Linguagem: {info.get('language')}")
        print(f"   - Path: {info.get('path')}")
        print(f"   - ID: {info.get('object_id')}")
else:
    print(f"❌ Erro ao enviar: {response.status_code}")
    print(response.text)

# Agora tentar exportar para confirmar
print("\n🔍 Testando export...")
export_url = f"{DATABRICKS_HOST}/api/2.0/workspace/export"
export_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
    "format": "SOURCE"
}

export_response = requests.get(export_url, headers=headers, json=export_payload)
if export_response.status_code == 200:
    print("✅ Export funcionou!")
    exported_content = base64.b64decode(export_response.json()["content"]).decode('utf-8')
    first_lines = exported_content.split('\n')[:10]
    print("\nPrimeiras 10 linhas do arquivo exportado:")
    for i, line in enumerate(first_lines, 1):
        print(f"  {i}: {line}")
else:
    print(f"⚠️ Export falhou: {export_response.status_code}")
    print(f"   {export_response.text}")
