-- Enqueue: initial run for dataset 'glo_agentes'
-- Safe: no credentials included.

INSERT INTO ingestion_sys.ops.run_queue (
  queue_id,
  dataset_id,
  trigger_type,
  requested_by,
  requested_at,
  priority,
  status,
  correlation_id
)
VALUES (
  uuid(),
  'glo_agentes',
  'MANUAL',
  'seed',
  current_timestamp(),
  50,
  'PENDING',
  concat('seed-initial-', date_format(current_timestamp(), 'yyyyMMdd-HHmmss'))
);
