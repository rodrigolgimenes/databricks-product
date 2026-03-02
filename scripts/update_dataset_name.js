const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(value) {
  return `'${String(value ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const datasetId = String(process.argv[2] || '').trim();
  const newDatasetName = String(process.argv[3] || '').trim();
  const updatedBy = String(process.env.PORTAL_USER || 'admin_fix').trim();

  if (!datasetId || !newDatasetName) {
    console.error('Usage: node scripts/update_dataset_name.js <DATASET_ID> <NEW_DATASET_NAME>');
    process.exitCode = 2;
    return;
  }

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ctrl = process.env.GOV_SYS_CTRL_SCHEMA || `${catalog}.ingestion_sys_ctrl`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  const upd =
    `UPDATE ${ctrl}.dataset_control\n` +
    `SET dataset_name = ${sqlStringLiteral(newDatasetName)},\n` +
    `    updated_at = current_timestamp(),\n` +
    `    updated_by = ${sqlStringLiteral(updatedBy)}\n` +
    `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`;

  await db.query(upd);

  const out = await db.query(
    `SELECT dataset_id, dataset_name, source_type, connection_id, execution_state, bronze_table, silver_table, updated_at, updated_by\n` +
      `FROM ${ctrl}.dataset_control\n` +
      `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
      `LIMIT 1`
  );

  console.log(JSON.stringify({ ok: true, dataset: db.rowsAsObjects(out)[0] }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
