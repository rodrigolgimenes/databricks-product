const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(value) {
  return `'${String(value ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const datasetId = String(process.argv[2] || '').trim();
  if (!datasetId) {
    console.error('Usage: node scripts/inspect_ops.js <DATASET_ID>');
    process.exitCode = 2;
    return;
  }

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ctrl = process.env.GOV_SYS_CTRL_SCHEMA || `${catalog}.ingestion_sys_ctrl`;
  const ops = process.env.GOV_SYS_OPS_SCHEMA || `${catalog}.ingestion_sys_ops`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  const ds = await db.query(
    `SELECT dataset_id, dataset_name, source_type, connection_id, execution_state, bronze_table, silver_table\n` +
      `FROM ${ctrl}.dataset_control\n` +
      `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
      `LIMIT 1`
  );

  const rq = await db.query(
    `SELECT queue_id, status, attempt, max_retries, next_retry_at, claimed_at, claim_owner, started_at, finished_at, run_id, last_error_class, last_error_message\n` +
      `FROM ${ops}.run_queue\n` +
      `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
      `ORDER BY requested_at DESC\n` +
      `LIMIT 10`
  );

  const bp = await db.query(
    `SELECT run_id, status, started_at, finished_at, bronze_row_count, silver_row_count, error_class, error_message\n` +
      `FROM ${ops}.batch_process\n` +
      `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
      `ORDER BY started_at DESC\n` +
      `LIMIT 10`
  );

  console.log(
    JSON.stringify(
      {
        dataset: db.rowsAsObjects(ds)[0],
        run_queue_last10: db.rowsAsObjects(rq),
        batch_process_last10: db.rowsAsObjects(bp),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
