"""Upload orchestrator + incremental_loading_functions notebooks to Databricks Workspace."""
import os
import base64
import requests
import sys

DATABRICKS_HOST = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

if not DATABRICKS_TOKEN:
    print("❌ DATABRICKS_TOKEN não definido!")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

NOTEBOOKS = [
    {
        "local": r"C:\dev\cm-databricks\databricks_notebooks\incremental_loading_functions.py",
        "remote": "/Workspace/Shared/incremental_loading/incremental_loading_functions",
        "name": "incremental_loading_functions",
    },
    {
        "local": r"C:\dev\cm-databricks\databricks_notebooks\governed_ingestion_orchestrator.py",
        "remote": "/Workspace/Shared/incremental_loading/governed_ingestion_orchestrator",
        "name": "governed_ingestion_orchestrator",
    },
]

upload_url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"

for nb in NOTEBOOKS:
    print(f"\n🔄 Uploading {nb['name']}...")
    
    if not os.path.exists(nb["local"]):
        print(f"  ❌ Arquivo local não encontrado: {nb['local']}")
        continue
    
    with open(nb["local"], "r", encoding="utf-8") as f:
        content = f.read()
    
    if not content.startswith("# Databricks notebook source"):
        print(f"  ❌ Arquivo não tem header correto!")
        continue
    
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    
    payload = {
        "path": nb["remote"],
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": True,
    }
    
    resp = requests.post(upload_url, headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"  ✅ {nb['name']} → {nb['remote']}")
    else:
        print(f"  ❌ Erro {resp.status_code}: {resp.text}")
        sys.exit(1)

print("\n✅ Todos os notebooks atualizados!")
