-- Dataset registration (DEV/QA)
-- Ajuste project_id, area_id, connection_id conforme seu setup.

INSERT INTO ingestion_sys.ctrl.dataset_control (
  dataset_id,
  project_id,
  area_id,
  dataset_name,
  source_type,
  connection_id,
  execution_state,
  bronze_table,
  silver_table,
  current_schema_ver,
  created_at,
  created_by
)
VALUES (
  'glo_agentes',
  '<project_id>',
  '<area_id>',
  'MEGA ERP - GLO_AGENTES',
  'ORACLE',
  '<oracle_connection_id>',
  'ACTIVE',
  'bronze_mega.glo_agentes',
  'silver_mega.glo_agentes',
  1,
  current_timestamp(),
  'seed'
);
