-- Sample dataset registration (DEV only)

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
  'sales_orders',
  'sample_project',
  'sales',
  'Sales Orders',
  'ORACLE',
  'oracle_sales_conn',
  'ACTIVE',
  'bronze_sales.sales_orders',
  'silver_sales.sales_orders',
  1,
  current_timestamp(),
  'sample_seed'
);
