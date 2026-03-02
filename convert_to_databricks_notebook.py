import json
import base64
import requests

# Credenciais
DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

# Ler arquivo Python
with open(r"C:\dev\cm-databricks\databricks_notebooks\incremental_loading_functions.py", 'r', encoding='utf-8') as f:
    content = f.read()

# Dividir em células por "# COMMAND ----------"
# Se não houver separadores, criar uma única célula
if "# COMMAND ----------" in content:
    cells = content.split("# COMMAND ----------")
else:
    # Adicionar separadores antes de cada comentário de seção principal
    import re
    # Inserir separador antes de cada linha "# ============"
    content_with_separators = re.sub(
        r'(# ={60,})',
        r'# COMMAND ----------\n\n\1',
        content
    )
    cells = content_with_separators.split("# COMMAND ----------")

# Limpar células
cells = [cell.strip() for cell in cells if cell.strip()]

print(f"Dividido em {len(cells)} células")

# Criar estrutura de notebook Databricks
notebook = {
    "cells": []
}

for i, cell_content in enumerate(cells):
    # Se é a primeira célula, adicionar o header
    if i == 0 and not cell_content.startswith("# Databricks notebook source"):
        cell_content = "# Databricks notebook source\n" + cell_content
    
    notebook["cells"].append({
        "cell_type": "code",
        "source": cell_content,
        "metadata": {},
        "outputs": [],
        "execution_count": None
    })

# Converter para formato JSON
notebook_json = json.dumps(notebook, indent=2, ensure_ascii=False)

# Encodar em base64
encoded_content = base64.b64encode(notebook_json.encode('utf-8')).decode('utf-8')

# Upload via Databricks API
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Deletar arquivo antigo se existir
delete_url = f"{DATABRICKS_HOST}/api/2.0/workspace/delete"
delete_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
    "recursive": False
}

print("Deletando arquivo Python antigo...")
response = requests.post(delete_url, headers=headers, json=delete_payload)
if response.status_code == 200:
    print("✅ Arquivo antigo deletado")
else:
    print(f"⚠️ Arquivo antigo não encontrado ou já deletado: {response.status_code}")

# Upload novo notebook
upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
upload_payload = {
    "path": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
    "format": "JUPYTER",
    "language": "PYTHON",
    "content": encoded_content,
    "overwrite": True
}

print("\nFazendo upload do notebook Databricks (formato JUPYTER)...")
response = requests.post(upload_url, headers=headers, json=upload_payload)

if response.status_code == 200:
    print("✅ Notebook Databricks enviado com sucesso!")
    print(f"📍 Path: /Workspace/Shared/incremental_loading/incremental_loading_functions")
    print(f"📊 Formato: JUPYTER (Databricks Notebook nativo)")
    print(f"🔢 Células: {len(cells)}")
    print("")
    print("✨ Agora o %run funcionará corretamente no orquestrador!")
else:
    print(f"❌ Erro: {response.status_code}")
    print(response.text)
