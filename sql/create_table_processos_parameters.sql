-- Tabela de parâmetros para ingestão de tabelas Oracle
-- Baseada no código funcional existente

CREATE TABLE IF NOT EXISTS cm_dbx_dev.0_par.processos (
  -- Identificação
  processo_id STRING COMMENT 'ID único do processo (usar UUID)',
  dataset_id STRING COMMENT 'Referência ao dataset_control (FK)',
  
  -- Origem Oracle
  src_full_tablename STRING COMMENT 'Nome completo da tabela Oracle: SCHEMA.TABELA@DBLINK ou (SELECT ...) subq',
  src_expected_rows BIGINT COMMENT 'Número estimado de linhas (para validação)',
  
  -- Destino Databricks
  tgt_ful_tablename STRING COMMENT 'Nome completo da tabela destino: catalog.schema.table',
  
  -- Configurações de Performance
  fetchsize INT COMMENT 'Tamanho do batch JDBC (padrão: 10000)',
  num_partitions INT COMMENT 'Número de partições Spark para escrita (padrão: 800)',
  partition_column STRING COMMENT 'Coluna para particionamento JDBC paralelo (NULL = sem particionamento)',
  lower_bound BIGINT COMMENT 'Valor mínimo da partition_column',
  upper_bound BIGINT COMMENT 'Valor máximo da partition_column',
  
  -- Controle
  ativo BOOLEAN COMMENT 'Se o processo está ativo (1) ou inativo (0)',
  created_at TIMESTAMP COMMENT 'Data de criação do registro',
  updated_at TIMESTAMP COMMENT 'Data da última atualização',
  created_by STRING COMMENT 'Usuário que criou',
  notes STRING COMMENT 'Observações sobre o processo'
)
COMMENT 'Tabela de parâmetros para processos de ingestão Oracle'
LOCATION 'abfss://data@storage.dfs.core.windows.net/cm_dbx_dev/0_par/processos';

-- Exemplo de insert
INSERT INTO cm_dbx_dev.0_par.processos VALUES (
  'proc-001',
  '92fb0589-07b1-48b5-98a2-c3deadad19c1',
  'CMASTER.CMALUINTERNO@CMASTERPRD',
  120200,
  'cm_dbx_dev.bronze_mega.cmaluinterno',
  10000,
  800,
  NULL, -- sem particionamento por enquanto
  NULL,
  NULL,
  true,
  current_timestamp(),
  current_timestamp(),
  'admin',
  'Tabela de alunos internos - carga full'
);

-- Criar índice para performance
CREATE INDEX IF NOT EXISTS idx_processos_ativo 
ON cm_dbx_dev.0_par.processos (ativo);

CREATE INDEX IF NOT EXISTS idx_processos_dataset 
ON cm_dbx_dev.0_par.processos (dataset_id);
