import sqlite3

conn = sqlite3.connect(r'C:\dev\cm-databricks\governed_ingestion.db')
cursor = conn.cursor()

# Listar tabelas
print("=== TABLES ===" )
for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
    print(row[0])

print("\n=== CONNECTIONS ===" )
for row in cursor.execute('SELECT id, name, jdbc_url FROM connections').fetchall():
    print(f"ID: {row[0]}\nName: {row[1]}\nJDBC: {row[2]}\n")

print("\n=== DATASETS ===" )
for row in cursor.execute('SELECT id, dataset_name, connection_id, source_table FROM datasets LIMIT 20').fetchall():
    print(f"ID: {row[0]} | Dataset: {row[1]} | Conn ID: {row[2]} | Table: {row[3]}")

conn.close()
