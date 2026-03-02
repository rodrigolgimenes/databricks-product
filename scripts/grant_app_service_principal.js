const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

async function main() {
  tryLoadDefaultEnv();

  const cfg = getDatabricksSqlConfigFromEnv(process.env);
  const db = createDatabricksSqlClient(cfg);

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';

  // Unity Catalog identifica service principals pelo applicationId.
  // Para o app dataload-tool: applicationId = 4b4c1c0d-c8ee-4745-940b-030a45f687c9
  const principalName = '4b4c1c0d-c8ee-4745-940b-030a45f687c9';
  const principal = `\`${principalName}\``;

  const schemasReadOnly = ['silver_mega', 'bronze_mega'];
  const schemasReadWrite = ['ingestion_sys_ctrl', 'ingestion_sys_ops'];

  const statements = [];
  statements.push(`GRANT USE CATALOG ON CATALOG ${catalog} TO ${principal}`);

  for (const s of [...schemasReadWrite, ...schemasReadOnly]) {
    statements.push(`GRANT USE SCHEMA ON SCHEMA ${catalog}.${s} TO ${principal}`);
    statements.push(`GRANT SELECT ON SCHEMA ${catalog}.${s} TO ${principal}`);
  }

  // Portal precisa escrever em ctrl/ops (criar dataset DRAFT, state changes, enqueue, approvals)
  for (const s of schemasReadWrite) {
    statements.push(`GRANT MODIFY ON SCHEMA ${catalog}.${s} TO ${principal}`);
  }

  for (const st of statements) {
    await db.query(st);
  }

  const out = await db.query(`SHOW GRANTS ON SCHEMA ${catalog}.ingestion_sys_ctrl`);
  const rows = db.rowsAsObjects(out);

  const found = rows.some((r) => {
    const p = String(r.Principal || r.principal || '');
    return p === principalName;
  });

  console.log(JSON.stringify({ ok: true, principal_application_id: principalName, sampleGrantsFound: found }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
