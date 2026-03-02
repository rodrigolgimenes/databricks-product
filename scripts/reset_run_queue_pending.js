const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(value) {
  return `'${String(value ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const datasetId = String(process.argv[2] || '').trim();
  if (!datasetId) {
    console.error('Usage: node scripts/reset_run_queue_pending.js <DATASET_ID>');
    process.exitCode = 2;
    return;
  }

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ops = process.env.GOV_SYS_OPS_SCHEMA || `${catalog}.ingestion_sys_ops`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  const upd =
    `UPDATE ${ops}.run_queue\n` +
    `SET next_retry_at = NULL\n` +
    `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
    `  AND status = 'PENDING'`;

  await db.query(upd);

  const out = await db.query(
    `SELECT queue_id, dataset_id, status, attempt, max_retries, next_retry_at, last_error_class, last_error_message\n` +
      `FROM ${ops}.run_queue\n` +
      `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
      `ORDER BY requested_at DESC\n` +
      `LIMIT 20`
  );

  console.log(JSON.stringify({ ok: true, opsSchema: ops, items: db.rowsAsObjects(out) }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
