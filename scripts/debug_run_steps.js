const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(v) {
  return `'${String(v ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const runId = String(process.argv[2] || '').trim();
  if (!runId) {
    console.error('Usage: node scripts/debug_run_steps.js <RUN_ID>');
    process.exitCode = 2;
    return;
  }

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ops = process.env.GOV_SYS_OPS_SCHEMA || `${catalog}.ingestion_sys_ops`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  const q =
    `SELECT phase, step_key, status, started_at, updated_at, finished_at, progress_current, progress_total, message\n` +
    `FROM ${ops}.batch_process_steps\n` +
    `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
    `ORDER BY started_at ASC\n` +
    `LIMIT 200`;

  const out = await db.query(q);
  const rows = db.rowsAsObjects(out);
  console.log(JSON.stringify({ ok: true, opsSchema: ops, run_id: runId, steps: rows }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
