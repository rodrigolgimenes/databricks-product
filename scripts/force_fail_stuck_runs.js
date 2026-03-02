const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(value) {
  return `'${String(value ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const datasetId = String(process.argv[2] || '').trim();
  if (!datasetId) {
    console.error('Usage: node scripts/force_fail_stuck_runs.js <DATASET_ID>');
    process.exitCode = 2;
    return;
  }

  const message = String(process.argv[3] || 'Cancelled by operator (stuck run)').trim();

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ops = process.env.GOV_SYS_OPS_SCHEMA || `${catalog}.ingestion_sys_ops`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  // 1) Mark run_queue stuck items as FAILED
  const updRQ =
    `UPDATE ${ops}.run_queue\n` +
    `SET status = 'FAILED',\n` +
    `    finished_at = current_timestamp(),\n` +
    `    last_error_class = 'RUNTIME_ERROR',\n` +
    `    last_error_message = ${sqlStringLiteral(message)},\n` +
    `    next_retry_at = NULL\n` +
    `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
    `  AND status IN ('CLAIMED','RUNNING')`;

  await db.query(updRQ);

  // 2) Mark batch_process RUNNING rows for that dataset as FAILED
  const updBP =
    `UPDATE ${ops}.batch_process\n` +
    `SET status = 'FAILED',\n` +
    `    finished_at = current_timestamp(),\n` +
    `    error_class = 'RUNTIME_ERROR',\n` +
    `    error_message = ${sqlStringLiteral(message)}\n` +
    `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
    `  AND status = 'RUNNING'`;

  await db.query(updBP);

  const out = await db.query(
    `SELECT queue_id, status, attempt, max_retries, next_retry_at, started_at, finished_at, run_id, last_error_class, last_error_message\n` +
      `FROM ${ops}.run_queue\n` +
      `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
      `ORDER BY requested_at DESC\n` +
      `LIMIT 10`
  );

  console.log(JSON.stringify({ ok: true, run_queue_last10: db.rowsAsObjects(out) }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
