const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(v) {
  return `'${String(v ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const name = String(process.argv[2] || '').trim();
  if (!name) {
    console.error('Usage: node scripts/debug_dataset.js <DATASET_NAME_OR_ID>');
    process.exitCode = 2;
    return;
  }

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));
  const ctrl = 'cm_dbx_dev.ingestion_sys_ctrl';
  const ops = 'cm_dbx_dev.ingestion_sys_ops';

  const dsSql =
    `SELECT dataset_id, dataset_name, execution_state, bronze_table, silver_table, current_schema_ver, last_success_run_id,\n` +
    `       created_at, created_by, updated_at, updated_by\n` +
    `FROM ${ctrl}.dataset_control\n` +
    `WHERE upper(dataset_name)=upper(${sqlStringLiteral(name)}) OR upper(dataset_id)=upper(${sqlStringLiteral(name)})\n` +
    `ORDER BY created_at DESC\n` +
    `LIMIT 20`;

  const ds = await db.query(dsSql);
  const matches = db.rowsAsObjects(ds);

  const runQueueCounts = [];
  const batchCounts = [];
  const stateChanges = [];

  for (const m of matches) {
    const id = String(m.dataset_id);

    const rq = await db.query(`SELECT COUNT(*) AS n FROM ${ops}.run_queue WHERE dataset_id = ${sqlStringLiteral(id)}`);
    const bp = await db.query(`SELECT COUNT(*) AS n FROM ${ops}.batch_process WHERE dataset_id = ${sqlStringLiteral(id)}`);
    const sc = await db.query(
      `SELECT new_state, COUNT(*) AS n\n` +
        `FROM ${ctrl}.dataset_state_changes\n` +
        `WHERE dataset_id = ${sqlStringLiteral(id)}\n` +
        `GROUP BY new_state\n` +
        `ORDER BY n DESC`
    );

    runQueueCounts.push({ dataset_id: id, ...db.rowsAsObjects(rq)[0] });
    batchCounts.push({ dataset_id: id, ...db.rowsAsObjects(bp)[0] });
    stateChanges.push({ dataset_id: id, states: db.rowsAsObjects(sc) });
  }

  console.log(
    JSON.stringify(
      {
        input: name,
        matches,
        run_queue_counts: runQueueCounts,
        batch_counts: batchCounts,
        state_changes: stateChanges,
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
