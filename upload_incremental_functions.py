#!/usr/bin/env python3
"""
Upload incremental loading functions to Databricks Workspace
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def convert_py_to_jupyter(py_file_path):
    """Convert Python file to Databricks Jupyter format"""
    with open(py_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by function definitions and major sections
    lines = content.split('\n')
    
    cells = []
    current_cell = []
    
    # First cell: header + imports
    in_header = True
    for line in lines:
        if in_header:
            current_cell.append(line)
            # After imports, start new cell
            if line.strip().startswith('from datetime import'):
                cells.append('\n'.join(current_cell))
                current_cell = []
                in_header = False
        elif line.startswith('# ============'):
            # New section = new cell
            if current_cell:
                cells.append('\n'.join(current_cell))
            current_cell = [line]
        else:
            current_cell.append(line)
    
    # Add last cell
    if current_cell:
        cells.append('\n'.join(current_cell))
    
    # Convert to Jupyter format
    jupyter_lines = ["# Databricks notebook source"]
    for cell in cells:
        if cell.strip():
            jupyter_lines.append(cell)
            jupyter_lines.append("")
            jupyter_lines.append("# COMMAND ----------")
            jupyter_lines.append("")
    
    return '\n'.join(jupyter_lines[:-3])  # Remove last COMMAND separator

def upload_to_databricks(content, workspace_path):
    """Upload notebook to Databricks Workspace"""
    import base64
    
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Encode content to base64
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
    
    payload = {
        "path": workspace_path,
        "content": content_b64,
        "language": "PYTHON",
        "format": "SOURCE",
        "overwrite": True
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Upload concluído com sucesso!")
        print(f"📍 Path: {workspace_path}")
    else:
        print(f"❌ Erro no upload: {response.status_code}")
        print(response.text)
        raise Exception(f"Upload failed: {response.text}")

if __name__ == "__main__":
    py_file = "databricks_notebooks/incremental_loading_functions.py"
    workspace_path = "/Workspace/Shared/incremental_loading/incremental_loading_functions"
    
    print("Convertendo incremental_loading_functions.py para formato Jupyter...")
    jupyter_content = convert_py_to_jupyter(py_file)
    
    print("Fazendo upload para Databricks...")
    upload_to_databricks(jupyter_content, workspace_path)
    
    print("\n⚡ Funções incrementais atualizadas!")
    print("  - Removidos todos os # type: ignore de dentro de strings SQL")
    print("  - Agora os comentários estão ANTES das chamadas spark.sql()")
