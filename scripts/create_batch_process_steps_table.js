const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

async function main() {
  tryLoadDefaultEnv();

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ops = process.env.GOV_SYS_OPS_SCHEMA || `${catalog}.ingestion_sys_ops`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  const ddl =
    `CREATE TABLE IF NOT EXISTS ${ops}.batch_process_steps (\n` +
    `  step_id            STRING      NOT NULL,\n` +
    `  run_id             STRING      NOT NULL,\n` +
    `  dataset_id         STRING      NOT NULL,\n` +
    `  phase              STRING      NOT NULL,\n` +
    `  step_key           STRING      NOT NULL,\n` +
    `  status             STRING      NOT NULL,\n` +
    `  message            STRING,\n` +
    `  progress_current   BIGINT,\n` +
    `  progress_total     BIGINT,\n` +
    `  details_json       STRING,\n` +
    `  started_at         TIMESTAMP   NOT NULL,\n` +
    `  updated_at         TIMESTAMP,\n` +
    `  finished_at        TIMESTAMP,\n` +
    `  CONSTRAINT pk_batch_process_steps PRIMARY KEY (step_id)\n` +
    `) USING DELTA`;

  await db.query(ddl);

  const out = await db.query(`SHOW TABLES IN ${ops}`);
  const rows = db.rowsAsObjects(out);
  const found = rows.some((r) => String(r.tableName || r.table_name || '').toLowerCase() === 'batch_process_steps');

  console.log(JSON.stringify({ ok: true, opsSchema: ops, created: found }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
