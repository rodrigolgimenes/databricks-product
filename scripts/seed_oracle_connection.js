const { tryLoadDefaultEnv } = require('../src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('../src/databricksSql');

function sqlStringLiteral(value) {
  return `'${String(value ?? '').replaceAll("'", "''")}'`;
}

async function main() {
  tryLoadDefaultEnv();

  const catalog = process.env.UC_CATALOG || 'cm_dbx_dev';
  const ctrl = process.env.GOV_SYS_CTRL_SCHEMA || `${catalog}.ingestion_sys_ctrl`;

  const db = createDatabricksSqlClient(getDatabricksSqlConfigFromEnv(process.env));

  // Defaults based on docs/dataset-glo-agentes-mega/env/.env.example
  const connectionId = process.env.ORACLE_CONNECTION_ID || 'mega_oracle_hml';
  const projectId = process.env.ORACLE_PROJECT_ID || 'mega_erp';
  const areaId = process.env.ORACLE_AREA_ID || 'mega';
  const jdbcUrl = process.env.JDBC_URL || 'jdbc:oracle:thin:@//dbconnect.megaerp.online:4221/xepdb1';

  const secretScope = process.env.ORACLE_SECRET_SCOPE || 'civilmaster-oracle';
  const secretUserKey = process.env.ORACLE_SECRET_USER_KEY || 'HML_MEGA_DB_USER';
  const secretPwdKey = process.env.ORACLE_SECRET_PWD_KEY || 'HML_MEGA_DB_SENHA';

  const createdBy = process.env.PORTAL_USER || 'admin_bootstrap';

  const merge =
    `MERGE INTO ${ctrl}.connections_oracle t\n` +
    `USING (SELECT\n` +
    `  ${sqlStringLiteral(connectionId)} AS connection_id,\n` +
    `  ${sqlStringLiteral(projectId)} AS project_id,\n` +
    `  ${sqlStringLiteral(areaId)} AS area_id,\n` +
    `  ${sqlStringLiteral(jdbcUrl)} AS jdbc_url,\n` +
    `  ${sqlStringLiteral(secretScope)} AS secret_scope,\n` +
    `  ${sqlStringLiteral(secretUserKey)} AS secret_user_key,\n` +
    `  ${sqlStringLiteral(secretPwdKey)} AS secret_pwd_key\n` +
    `) s\n` +
    `ON t.connection_id = s.connection_id\n` +
    `WHEN MATCHED THEN UPDATE SET\n` +
    `  t.project_id = s.project_id,\n` +
    `  t.area_id = s.area_id,\n` +
    `  t.jdbc_url = s.jdbc_url,\n` +
    `  t.secret_scope = s.secret_scope,\n` +
    `  t.secret_user_key = s.secret_user_key,\n` +
    `  t.secret_pwd_key = s.secret_pwd_key,\n` +
    `  t.approval_status = 'APPROVED',\n` +
    `  t.approved_by = ${sqlStringLiteral(createdBy)},\n` +
    `  t.approved_at = current_timestamp(),\n` +
    `  t.updated_at = current_timestamp(),\n` +
    `  t.updated_by = ${sqlStringLiteral(createdBy)}\n` +
    `WHEN NOT MATCHED THEN INSERT (\n` +
    `  connection_id, project_id, area_id, jdbc_url, secret_scope, secret_user_key, secret_pwd_key,\n` +
    `  approval_status, approved_by, approved_at,\n` +
    `  created_at, created_by, updated_at, updated_by\n` +
    `) VALUES (\n` +
    `  s.connection_id, s.project_id, s.area_id, s.jdbc_url, s.secret_scope, s.secret_user_key, s.secret_pwd_key,\n` +
    `  'APPROVED', ${sqlStringLiteral(createdBy)}, current_timestamp(),\n` +
    `  current_timestamp(), ${sqlStringLiteral(createdBy)}, current_timestamp(), ${sqlStringLiteral(createdBy)}\n` +
    `)`;

  await db.query(merge);

  const out = await db.query(
    `SELECT connection_id, project_id, area_id, approval_status, jdbc_url, secret_scope, secret_user_key, secret_pwd_key\n` +
      `FROM ${ctrl}.connections_oracle\n` +
      `WHERE approval_status = 'APPROVED'\n` +
      `ORDER BY approved_at DESC NULLS LAST, created_at DESC\n` +
      `LIMIT 10`
  );

  console.log(JSON.stringify({ ok: true, ctrlSchema: ctrl, approvedConnections: db.rowsAsObjects(out) }, null, 2));
}

main().catch((e) => {
  console.error(e.code || '', e.message);
  process.exitCode = 1;
});
