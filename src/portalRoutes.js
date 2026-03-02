const crypto = require('crypto');

const { tryLoadDefaultEnv } = require('./env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('./databricksSql');

// ===== Databricks REST API helper (Jobs, Clusters) =====

function createDatabricksRestClient({ host, token, clientId, clientSecret } = {}) {
  let oauthCache = null;

  async function getAccessToken() {
    if (token) return token;
    if (!clientId || !clientSecret) return null;

    const now = Date.now();
    if (oauthCache && oauthCache.expires_at_ms - now > 60_000) return oauthCache.access_token;

    const url = `${host}/oidc/v1/token`;
    const basic = Buffer.from(`${clientId}:${clientSecret}`, 'utf8').toString('base64');
    const body = new URLSearchParams();
    body.set('grant_type', 'client_credentials');
    body.set('scope', 'all-apis');

    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data?.error_description || `OAuth ${res.status}`);
    oauthCache = { access_token: data.access_token, expires_at_ms: now + (data.expires_in || 600) * 1000 };
    return data.access_token;
  }

  async function request(method, path, body) {
    const accessToken = await getAccessToken();
    if (!accessToken) {
      const err = new Error('Databricks REST API: sem credenciais');
      err.code = 'DATABRICKS_NOT_CONFIGURED';
      throw err;
    }
    const url = `${host}${path}`;
    const opts = {
      method,
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(url, opts);
    const text = await res.text();
    let data;
    try { data = text ? JSON.parse(text) : {}; } catch { data = { _raw: text }; }
    if (!res.ok) {
      const err = new Error(data?.message || data?.error || `HTTP ${res.status}`);
      err.httpStatus = res.status;
      err.details = data;
      throw err;
    }
    return data;
  }

  return {
    listJobs: (limit = 25, offset = 0, name) => {
      let path = `/api/2.1/jobs/list?limit=${limit}&offset=${offset}&expand_tasks=false`;
      if (name) path += `&name=${encodeURIComponent(name)}`;
      return request('GET', path);
    },
    getJob: (jobId) => request('GET', `/api/2.1/jobs/get?job_id=${jobId}`),
    runNow: (jobId, jobParams = {}) =>
      request('POST', '/api/2.1/jobs/run-now', { job_id: Number(jobId), job_parameters: jobParams }),
    getRunOutput: (runId) => request('GET', `/api/2.1/jobs/runs/get-output?run_id=${runId}`),
    getRun: (runId) => request('GET', `/api/2.1/jobs/runs/get?run_id=${runId}`),
    listRuns: (jobId, limit = 10) =>
      request('GET', `/api/2.1/jobs/runs/list?job_id=${jobId}&limit=${limit}&expand_tasks=false`),
    cancelRun: (runId) => request('POST', '/api/2.1/jobs/runs/cancel', { run_id: Number(runId) }),
  };
}

function sqlStringLiteral(value) {
  return `'${String(value ?? '').replaceAll("'", "''")}'`;
}

function parseIntStrict(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return Number.isInteger(n) ? n : null;
}

function isSafeIdentifier(name) {
  return /^[A-Za-z0-9_]+$/.test(String(name || ''));
}

function isSafeTableName(name) {
  const n = String(name || '').trim();
  if (!n) return false;
  // allow schema.table or catalog.schema.table
  const parts = n.split('.');
  if (parts.length !== 2 && parts.length !== 3) return false;
  return parts.every((p) => isSafeIdentifier(p));
}

function getPortalConfigFromEnv(env) {
  const catalog = String(env.UC_CATALOG || env.GOV_UC_CATALOG || 'cm_dbx_dev').trim();
  const ctrlSchema = String(env.GOV_SYS_CTRL_SCHEMA || `${catalog}.ingestion_sys_ctrl`).trim();
  const opsSchema = String(env.GOV_SYS_OPS_SCHEMA || `${catalog}.ingestion_sys_ops`).trim();

  return {
    catalog,
    ctrlSchema,
    opsSchema,
    defaultLimit: parseIntStrict(env.PORTAL_DEFAULT_LIMIT) || 50,
    maxLimit: parseIntStrict(env.PORTAL_MAX_LIMIT) || 200,
  };
}

function getRequestUser(req) {
  // MVP: sem auth. Permite identificar ações via header, se disponível.
  const h = req.headers['x-user'] || req.headers['x-portal-user'];
  const u = String(Array.isArray(h) ? h[0] : h || '').trim();
  return u || String(process.env.PORTAL_USER || 'portal').trim();
}

function humanizeError({ errorClass, errorMessage } = {}) {
  const cls = String(errorClass || '').toUpperCase();
  const msg = String(errorMessage || '').trim();

  if (cls === 'SCHEMA_ERROR') {
    if (/drift/i.test(msg) || /schema/i.test(msg)) {
      return 'Mudança de schema detectada. O dataset foi bloqueado e precisa de aprovação.';
    }
    if (/CAST_INVALID_INPUT/i.test(msg) || /invalid input/i.test(msg)) {
      return 'Falha ao converter dados para os tipos exigidos pelo contrato (Silver). Verifique valores inválidos/formatos.';
    }
    return 'Erro de contrato/schema na promoção para Silver.';
  }

  if (cls === 'SOURCE_ERROR') {
    return 'Falha ao ler dados na origem. Verifique conexão e disponibilidade da fonte.';
  }

  if (cls === 'RUNTIME_ERROR') {
    return 'Falha durante a execução. Tente novamente; se persistir, acione o time de engenharia.';
  }

  if (msg) return msg;
  return 'Erro desconhecido.';
}

function diffExpectSchemas(active, pending) {
  const aCols = Array.isArray(active?.columns) ? active.columns : [];
  const pCols = Array.isArray(pending?.columns) ? pending.columns : [];

  const aMap = new Map(aCols.map((c) => [c.name, c]));
  const pMap = new Map(pCols.map((c) => [c.name, c]));

  const changes = [];

  for (const [name, pc] of pMap.entries()) {
    const ac = aMap.get(name);
    if (!ac) {
      changes.push({ type: 'ADD_COLUMN', column: name, pending: pc });
      continue;
    }

    const aType = String(ac.type || '').toLowerCase();
    const pType = String(pc.type || '').toLowerCase();
    if (aType !== pType) {
      changes.push({ type: 'TYPE_CHANGE', column: name, active: ac, pending: pc });
    }

    const aNull = Boolean(ac.nullable);
    const pNull = Boolean(pc.nullable);
    if (aNull !== pNull) {
      changes.push({ type: 'NULLABILITY_CHANGE', column: name, active: ac, pending: pc });
    }
  }

  for (const [name, ac] of aMap.entries()) {
    if (!pMap.has(name)) {
      changes.push({ type: 'REMOVE_COLUMN', column: name, active: ac });
    }
  }

  return changes;
}

function registerPortalRoutes(app) {
  console.log('[PORTAL] Registrando rotas do portal...');
  
  // Local dev convenience (does not override real env)
  const envResult = tryLoadDefaultEnv();
  console.log('[PORTAL] Carregamento de .env:', envResult);
  
  console.log('[PORTAL] Variáveis relevantes do ambiente:');
  console.log('  - DATABRICKS_HOST:', process.env.DATABRICKS_HOST ? '✓' : '✗');
  console.log('  - DATABRICKS_TOKEN:', process.env.DATABRICKS_TOKEN ? `✓ (${process.env.DATABRICKS_TOKEN.length} chars)` : '✗');
  console.log('  - DATABRICKS_SQL_WAREHOUSE_ID:', process.env.DATABRICKS_SQL_WAREHOUSE_ID || '(VAZIO!)');
  console.log('  - UC_CATALOG:', process.env.UC_CATALOG || '(padrão)');
  
  const portalCfg = getPortalConfigFromEnv(process.env);
  console.log('[PORTAL] Portal config:', portalCfg);
  
  const dbCfg = getDatabricksSqlConfigFromEnv(process.env);
  const db = createDatabricksSqlClient(dbCfg);

  // Databricks REST API client (for Jobs API)
  const dbHost = String(process.env.DATABRICKS_HOST || process.env.DATABRICKS_WORKSPACE_HOST || '').replace(/\/$/, '');
  const dbToken = String(process.env.DATABRICKS_TOKEN || '').trim();
  const dbClientId = String(process.env.DATABRICKS_CLIENT_ID || '').trim();
  const dbClientSecret = String(process.env.DATABRICKS_CLIENT_SECRET || '').trim();
  const dbRest = (dbHost && (dbToken || (dbClientId && dbClientSecret)))
    ? createDatabricksRestClient({ host: dbHost.startsWith('http') ? dbHost : `https://${dbHost}`, token: dbToken, clientId: dbClientId, clientSecret: dbClientSecret })
    : null;

  const ORCHESTRATOR_JOB_ID = String(process.env.DATABRICKS_ORCHESTRATOR_JOB_ID || '').trim() || null;
  console.log('[PORTAL] Databricks REST API:', dbRest ? '✓ configurado' : '✗ não configurado');
  console.log('[PORTAL] ORCHESTRATOR_JOB_ID:', ORCHESTRATOR_JOB_ID || '(não definido - configure DATABRICKS_ORCHESTRATOR_JOB_ID)');

  // Helper: trigger orchestrator job for a specific dataset
  async function triggerOrchestratorJob(datasetId) {
    if (!dbRest || !ORCHESTRATOR_JOB_ID) return null;
    try {
      console.log(`[TRIGGER] Disparando job ${ORCHESTRATOR_JOB_ID} com target_dataset_id=${datasetId}`);
      const result = await dbRest.runNow(ORCHESTRATOR_JOB_ID, {
        target_dataset_id: datasetId,
        max_items: '1',
      });
      console.log(`[TRIGGER] ✓ Job disparado: run_id=${result.run_id}`);
      return result;
    } catch (e) {
      console.error(`[TRIGGER] ✗ Erro ao disparar job: ${e.message}`);
      return { error: e.message };
    }
  }

  // ===== BATCH TRACKING (in-memory) =====
  const batchStore = new Map(); // batchId -> BatchState

  // Cleanup batches older than 1 hour
  setInterval(() => {
    const cutoff = Date.now() - 60 * 60 * 1000;
    for (const [id, batch] of batchStore.entries()) {
      if (batch.started_at_ms < cutoff) batchStore.delete(id);
    }
  }, 5 * 60 * 1000);

  function clampLimit(n, fallback) {
    const x = parseIntStrict(n);
    if (x == null) return fallback;
    return Math.max(1, Math.min(portalCfg.maxLimit, x));
  }

  async function sqlQueryObjects(sql) {
    const r = await db.query(sql);
    return db.rowsAsObjects(r);
  }

  function notConfiguredResponse(err) {
    return {
      ok: false,
      error: err.code || 'DATABRICKS_NOT_CONFIGURED',
      message: err.message,
      hint: 'Configure DATABRICKS_HOST, DATABRICKS_TOKEN e DATABRICKS_SQL_WAREHOUSE_ID (ou DATABRICKS_HTTP_PATH).',
    };
  }

  app.get('/api/portal/health', async (req, res) => {
    try {
      const out = await db.query('SELECT 1 AS ok');
      const rows = db.rowsAsObjects(out);
      return res.json({ ok: true, databricks: true, rows });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(200).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/orchestrator/status', async (req, res) => {
    try {
      // Conta jobs pendentes
      const pendingResult = await sqlQueryObjects(
        `SELECT COUNT(*) AS pending_count\n` +
          `FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE status = 'PENDING' AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp())`
      );
      
      // Verifica jobs em execução (CLAIMED/RUNNING)
      const runningResult = await sqlQueryObjects(
        `SELECT COUNT(*) AS running_count\n` +
          `FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE status IN ('CLAIMED', 'RUNNING')`
      );

      // Busca últimos jobs processados (para verificar se orchestrator está ativo)
      const recentProcessed = await sqlQueryObjects(
        `SELECT queue_id, dataset_id, status, claim_owner, claimed_at, started_at, finished_at\n` +
          `FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE status IN ('SUCCEEDED', 'FAILED') AND finished_at IS NOT NULL\n` +
          `ORDER BY finished_at DESC\n` +
          `LIMIT 5`
      );

      // Busca jobs mais antigos ainda pendentes
      const oldestPending = await sqlQueryObjects(
        `SELECT queue_id, dataset_id, requested_at, requested_by, attempt, next_retry_at\n` +
          `FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE status = 'PENDING' AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp())\n` +
          `ORDER BY requested_at ASC\n` +
          `LIMIT 10`
      );

      const pendingCount = Number(pendingResult[0]?.pending_count || 0);
      const runningCount = Number(runningResult[0]?.running_count || 0);
      
      // Heurística: se não há jobs em execução e há pendentes, orchestrator pode estar parado
      const lastProcessed = recentProcessed.length > 0 ? recentProcessed[0] : null;
      const orchestratorActive = runningCount > 0 || (lastProcessed && new Date(lastProcessed.finished_at) > new Date(Date.now() - 5 * 60 * 1000)); // Processou algo nos últimos 5min

      return res.json({
        ok: true,
        orchestrator_status: {
          likely_active: orchestratorActive,
          warning: pendingCount > 0 && !orchestratorActive ? 'Há jobs pendentes mas nenhum orchestrator parece estar ativo nos últimos 5 minutos' : null,
        },
        queue_stats: {
          pending: pendingCount,
          running: runningCount,
        },
        recent_activity: {
          last_processed: lastProcessed,
          processed_in_last_5_jobs: recentProcessed.length,
        },
        oldest_pending: oldestPending,
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/meta', (req, res) => {
    return res.json({
      ok: true,
      portal: {
        catalog: portalCfg.catalog,
        ctrlSchema: portalCfg.ctrlSchema,
        opsSchema: portalCfg.opsSchema,
      },
      databricks: {
        host: db.cfg.host || null,
        warehouseId: db.cfg.warehouseId ? `...${db.cfg.warehouseId.slice(-4)}` : null,
        configured: Boolean(db.cfg.host && db.cfg.token && db.cfg.warehouseId),
      },
    });
  });

  // Dashboard summary for UX (counts + recent failures)
  app.get('/api/portal/dashboard/summary', async (req, res) => {
    const limit = clampLimit(req.query.limit, 50);

    try {
      const [datasetStates, rqStates, bpStates, recentFailures] = await Promise.all([
        sqlQueryObjects(
          `SELECT execution_state, COUNT(*) AS n\n` +
            `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
            `GROUP BY execution_state\n` +
            `ORDER BY n DESC`
        ),
        sqlQueryObjects(
          `SELECT status, COUNT(*) AS n\n` +
            `FROM ${portalCfg.opsSchema}.run_queue\n` +
            `GROUP BY status\n` +
            `ORDER BY n DESC`
        ),
        sqlQueryObjects(
          `SELECT status, COUNT(*) AS n\n` +
            `FROM ${portalCfg.opsSchema}.batch_process\n` +
            `GROUP BY status\n` +
            `ORDER BY n DESC`
        ),
        sqlQueryObjects(
          `SELECT run_id, dataset_id, status, started_at, finished_at, error_class, error_message\n` +
            `FROM ${portalCfg.opsSchema}.batch_process\n` +
            `WHERE status = 'FAILED'\n` +
            `ORDER BY COALESCE(finished_at, started_at) DESC\n` +
            `LIMIT ${limit}`
        ),
      ]);

      return res.json({
        ok: true,
        dataset_states: datasetStates,
        run_queue_states: rqStates,
        batch_process_states: bpStates,
        recent_failures: recentFailures,
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Lookup run_queue item (used for polling until run_id is assigned)
  app.get('/api/portal/run-queue/:queueId', async (req, res) => {
    const queueId = String(req.params.queueId || '').trim();

    try {
      const rows = await sqlQueryObjects(
        `SELECT queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, claim_owner, claimed_at,\n` +
          `       attempt, max_retries, next_retry_at, last_error_class, last_error_message, started_at, finished_at, correlation_id, run_id\n` +
          `FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE queue_id = ${sqlStringLiteral(queueId)}\n` +
          `LIMIT 1`
      );
      if (!rows.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      return res.json({ ok: true, item: rows[0] });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/projects', async (req, res) => {
    try {
      const rows = await sqlQueryObjects(
        `SELECT project_id, project_name, description, is_active, created_at, created_by\n` +
          `FROM ${portalCfg.ctrlSchema}.projects\n` +
          `ORDER BY project_name ASC`
      );
      return res.json({ ok: true, items: rows });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/areas', async (req, res) => {
    const projectId = String(req.query.project_id || '').trim();
    try {
      const where = projectId ? `WHERE project_id = ${sqlStringLiteral(projectId)}` : '';
      const rows = await sqlQueryObjects(
        `SELECT area_id, project_id, area_name, description, is_active, created_at, created_by\n` +
          `FROM ${portalCfg.ctrlSchema}.areas\n` +
          `${where}\n` +
          `ORDER BY area_name ASC`
      );
      return res.json({ ok: true, items: rows });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Update project name
  app.put('/api/portal/projects/:projectId', async (req, res) => {
    const projectId = String(req.params.projectId || '').trim();
    const newName = String(req.body?.project_name || '').trim();
    const user = getRequestUser(req);

    if (!projectId || !newName) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'project_id e project_name são obrigatórios' });
    }

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.projects\n` +
          `SET project_name = ${sqlStringLiteral(newName)},\n` +
          `    updated_at = TIMESTAMP ${sqlStringLiteral(now)},\n` +
          `    updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE project_id = ${sqlStringLiteral(projectId)}`
      );

      return res.json({ ok: true, updated: true, project_id: projectId, project_name: newName });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Update area name
  app.put('/api/portal/areas/:areaId', async (req, res) => {
    const areaId = String(req.params.areaId || '').trim();
    const newName = String(req.body?.area_name || '').trim();
    const user = getRequestUser(req);

    if (!areaId || !newName) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'area_id e area_name são obrigatórios' });
    }

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.areas\n` +
          `SET area_name = ${sqlStringLiteral(newName)},\n` +
          `    updated_at = TIMESTAMP ${sqlStringLiteral(now)},\n` +
          `    updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE area_id = ${sqlStringLiteral(areaId)}`
      );

      return res.json({ ok: true, updated: true, area_id: areaId, area_name: newName });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/connections/oracle', async (req, res) => {
    const projectId = String(req.query.project_id || '').trim();
    const areaId = String(req.query.area_id || '').trim();

    try {
      const wh = [];
      if (projectId) wh.push(`project_id = ${sqlStringLiteral(projectId)}`);
      if (areaId) wh.push(`area_id = ${sqlStringLiteral(areaId)}`);
      wh.push("approval_status = 'APPROVED'");

      const where = wh.length ? `WHERE ${wh.join(' AND ')}` : '';

      const rows = await sqlQueryObjects(
        `SELECT connection_id, project_id, area_id, jdbc_url, secret_scope, secret_user_key, secret_pwd_key, approval_status, approved_by, approved_at\n` +
          `FROM ${portalCfg.ctrlSchema}.connections_oracle\n` +
          `${where}\n` +
          `ORDER BY approved_at DESC NULLS LAST, created_at DESC`
      );

      // Não retornamos segredos, apenas metadados.
      return res.json({ ok: true, items: rows });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/datasets', async (req, res) => {
    // Pagination
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const pageSize = Math.min(200, Math.max(1, parseInt(req.query.page_size, 10) || 50));
    const offset = (page - 1) * pageSize;

    // Search
    const search = String(req.query.search || '').trim();

    // Filters
    const filterStatus = String(req.query.status || '').trim().toUpperCase();
    const filterSourceType = String(req.query.source_type || '').trim().toUpperCase();
    const filterProjectId = String(req.query.project_id || '').trim();
    const filterAreaId = String(req.query.area_id || '').trim();
    
    // DataOps Filters (Governance)
    const filterLoadType = String(req.query.load_type || '').trim().toUpperCase(); // FULL, INCREMENTAL, SNAPSHOT
    const filterHasWatermark = req.query.has_watermark; // 'true', 'false', undefined
    const filterStaleDays = parseInt(req.query.stale_days, 10) || 0; // datasets sem exec há X dias

    // Sort (whitelist to prevent SQL injection)
    const SORT_WHITELIST = {
      dataset_name: 'dataset_name',
      execution_state: 'execution_state',
      source_type: 'source_type',
      project_id: 'project_id',
      area_id: 'area_id',
      created_at: 'created_at',
      updated_at: 'updated_at',
      bronze_table: 'bronze_table',
      silver_table: 'silver_table',
      current_schema_ver: 'current_schema_ver',
      last_success_at: 'last_success_at',
    };
    const sortByRaw = String(req.query.sort_by || '').trim().toLowerCase();
    const sortDir = String(req.query.sort_dir || '').trim().toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const sortColumn = SORT_WHITELIST[sortByRaw] || null;

    try {
      // Build WHERE clauses
      const conditions = [];

      if (search) {
        const term = sqlStringLiteral(`%${search}%`);
        conditions.push(
          `(LOWER(dataset_name) LIKE LOWER(${term})` +
          ` OR LOWER(bronze_table) LIKE LOWER(${term})` +
          ` OR LOWER(silver_table) LIKE LOWER(${term})` +
          ` OR LOWER(source_type) LIKE LOWER(${term})` +
          ` OR LOWER(project_id) LIKE LOWER(${term})` +
          ` OR LOWER(area_id) LIKE LOWER(${term}))`
        );
      }

      if (filterStatus) conditions.push(`dc.execution_state = ${sqlStringLiteral(filterStatus)}`);
      if (filterSourceType) conditions.push(`dc.source_type = ${sqlStringLiteral(filterSourceType)}`);
      if (filterProjectId) conditions.push(`dc.project_id = ${sqlStringLiteral(filterProjectId)}`);
      if (filterAreaId) conditions.push(`dc.area_id = ${sqlStringLiteral(filterAreaId)}`);
      
      // DataOps Governance Filters
      if (filterLoadType === 'FULL') {
        conditions.push('(dc.enable_incremental = FALSE OR dc.enable_incremental IS NULL)');
      } else if (filterLoadType === 'INCREMENTAL') {
        conditions.push('dc.enable_incremental = TRUE AND COALESCE(dc.incremental_strategy, \'\') != \'SNAPSHOT\'');
      } else if (filterLoadType === 'SNAPSHOT') {
        conditions.push('dc.incremental_strategy = \'SNAPSHOT\'');
      }
      
      if (filterHasWatermark === 'false') {
        conditions.push('(dc.incremental_metadata IS NULL OR dc.incremental_metadata NOT LIKE \'%watermark_column%\')');
      } else if (filterHasWatermark === 'true') {
        conditions.push('dc.incremental_metadata LIKE \'%watermark_column%\'');
      }
      
      if (filterStaleDays > 0) {
        conditions.push(`COALESCE(lr.last_success_at, dc.created_at) < CURRENT_TIMESTAMP() - INTERVAL ${filterStaleDays} DAYS`);
      }

      const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

      const orderBy = sortColumn
        ? `ORDER BY ${sortColumn} ${sortDir} NULLS LAST`
        : `ORDER BY COALESCE(dc.updated_at, dc.created_at) DESC`;

      // Count total with LEFT JOIN
      const countResult = await sqlQueryObjects(
        `SELECT COUNT(DISTINCT dc.dataset_id) AS cnt \n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control dc\n` +
          `LEFT JOIN (\n` +
          `  SELECT dataset_id, \n` +
          `         MAX(CASE WHEN status='SUCCEEDED' THEN finished_at END) as last_success_at\n` +
          `  FROM ${portalCfg.opsSchema}.batch_process\n` +
          `  GROUP BY dataset_id\n` +
          `) lr ON dc.dataset_id = lr.dataset_id\n` +
          `${whereClause}`
      );
      const total = parseInt(countResult[0]?.cnt || '0', 10);
      const totalPages = Math.max(1, Math.ceil(total / pageSize));

      // Fetch page with enriched data
      const rows = await sqlQueryObjects(
        `SELECT dc.dataset_id, dc.project_id, dc.area_id, dc.dataset_name, dc.source_type, dc.connection_id, dc.execution_state,\n` +
          `       dc.bronze_table, dc.silver_table, dc.current_schema_ver, dc.last_success_run_id, \n` +
          `       dc.created_at, dc.created_by, dc.updated_at, dc.updated_by,\n` +
          `       dc.enable_incremental, dc.incremental_strategy, dc.bronze_mode, dc.incremental_metadata,\n` +
          `       lr.last_success_at\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control dc\n` +
          `LEFT JOIN (\n` +
          `  SELECT dataset_id, \n` +
          `         MAX(CASE WHEN status='SUCCEEDED' THEN finished_at END) as last_success_at\n` +
          `  FROM ${portalCfg.opsSchema}.batch_process\n` +
          `  GROUP BY dataset_id\n` +
          `) lr ON dc.dataset_id = lr.dataset_id\n` +
          `${whereClause}\n` +
          `${orderBy}\n` +
          `LIMIT ${pageSize} OFFSET ${offset}`
      );
      
      // Enrich rows with derived fields
      const enrichedRows = rows.map(row => {
        let metadata = {};
        try {
          metadata = row.incremental_metadata ? JSON.parse(row.incremental_metadata) : {};
        } catch {}
        
        return {
          ...row,
          // Derived fields for frontend
          watermark_column: metadata.watermark_column || null,
          lookback_days: metadata.lookback_days || null,
          load_type: row.enable_incremental 
            ? (row.incremental_strategy === 'SNAPSHOT' ? 'SNAPSHOT' : 'INCREMENTAL')
            : 'FULL'
        };
      });

      return res.json({ ok: true, items: enrichedRows, total, page, page_size: pageSize, total_pages: totalPages });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/datasets/:datasetId', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();

    try {
      const ds = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.dataset_control WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!ds.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      return res.json({ ok: true, item: ds[0] });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/datasets/:datasetId/state-changes', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const limit = clampLimit(req.query.limit, portalCfg.defaultLimit);

    try {
      const rows = await sqlQueryObjects(
        `SELECT change_id, dataset_id, old_state, new_state, reason, changed_at, changed_by\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_state_changes\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
          `ORDER BY changed_at DESC\n` +
          `LIMIT ${limit}`
      );
      return res.json({ ok: true, items: rows });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/datasets/:datasetId/runs', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const limit = clampLimit(req.query.limit, portalCfg.defaultLimit);

    try {
      const runQueue = await sqlQueryObjects(
        `SELECT queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, claim_owner, claimed_at,\n` +
          `       attempt, max_retries, next_retry_at, last_error_class, last_error_message, started_at, finished_at, correlation_id, run_id\n` +
          `FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
          `ORDER BY requested_at DESC\n` +
          `LIMIT ${limit}`
      );

      const batch = await sqlQueryObjects(
        `SELECT run_id, dataset_id, queue_id, execution_mode, status, started_at, finished_at,\n` +
          `       bronze_row_count, silver_row_count, error_class, error_message, error_stacktrace\n` +
          `FROM ${portalCfg.opsSchema}.batch_process\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
          `ORDER BY started_at DESC\n` +
          `LIMIT ${limit}`
      );

      const latestFailed = batch.find((b) => String(b.status || '').toUpperCase() === 'FAILED') || null;
      const actionableError = latestFailed
        ? {
            human: humanizeError({ errorClass: latestFailed.error_class, errorMessage: latestFailed.error_message }),
            technical: {
              error_class: latestFailed.error_class || null,
              error_message: latestFailed.error_message || null,
            },
            debug: {
              run_id: latestFailed.run_id,
              stacktrace: latestFailed.error_stacktrace || null,
            },
          }
        : null;

      return res.json({ ok: true, run_queue: runQueue, batch_process: batch, last_error: actionableError });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/runs/:runId', async (req, res) => {
    const runId = String(req.params.runId || '').trim();

    try {
      const bp = await sqlQueryObjects(
        `SELECT run_id, dataset_id, queue_id, execution_mode, status, started_at, finished_at,\n` +
          `       orchestrator_job_id, orchestrator_run_id, orchestrator_task,\n` +
          `       bronze_row_count, silver_row_count, error_class, error_message, error_stacktrace,\n` +
          `       load_type, incremental_rows_read, watermark_start, watermark_end\n` +
          `FROM ${portalCfg.opsSchema}.batch_process\n` +
          `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
          `LIMIT 1`
      );
      if (!bp.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      const details = await sqlQueryObjects(
        `SELECT detail_id, run_id, dataset_id, layer, table_name, operation, started_at, finished_at,\n` +
          `       row_count, inserted_count, updated_count, deleted_count, status, error_message\n` +
          `FROM ${portalCfg.opsSchema}.batch_process_table_details\n` +
          `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
          `ORDER BY started_at ASC`
      );

      let rq = [];
      if (bp[0].queue_id) {
        rq = await sqlQueryObjects(
          `SELECT queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, claim_owner, claimed_at,\n` +
            `       attempt, max_retries, next_retry_at, last_error_class, last_error_message, started_at, finished_at, correlation_id, run_id\n` +
            `FROM ${portalCfg.opsSchema}.run_queue\n` +
            `WHERE queue_id = ${sqlStringLiteral(bp[0].queue_id)}\n` +
            `LIMIT 1`
        );
      } else {
        rq = await sqlQueryObjects(
          `SELECT queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, claim_owner, claimed_at,\n` +
            `       attempt, max_retries, next_retry_at, last_error_class, last_error_message, started_at, finished_at, correlation_id, run_id\n` +
            `FROM ${portalCfg.opsSchema}.run_queue\n` +
            `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
            `ORDER BY requested_at DESC\n` +
            `LIMIT 1`
        );
      }

      // ── Enrich: dataset_context (governance diagnostics) ──
      let dataset_context = null;
      let previous_run = null;
      const datasetId = bp[0].dataset_id;
      if (datasetId) {
        try {
          const [dsRows, prevRows] = await Promise.all([
            sqlQueryObjects(
              `SELECT incremental_strategy, incremental_metadata, discovery_status,\n` +
                `       discovery_suggestion, enable_incremental, strategy_decision_log\n` +
                `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
                `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
                `LIMIT 1`
            ),
            sqlQueryObjects(
              `SELECT run_id, silver_row_count, bronze_row_count, finished_at, started_at,\n` +
                `       CASE WHEN finished_at IS NOT NULL AND started_at IS NOT NULL\n` +
                `            THEN CAST(TIMESTAMPDIFF(SECOND, started_at, finished_at) AS BIGINT)\n` +
                `            ELSE NULL END AS duration_seconds\n` +
                `FROM ${portalCfg.opsSchema}.batch_process\n` +
                `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
                `  AND status = 'SUCCEEDED'\n` +
                `  AND run_id != ${sqlStringLiteral(runId)}\n` +
                `ORDER BY finished_at DESC\n` +
                `LIMIT 1`
            ),
          ]);
          if (dsRows.length) {
            const ds = dsRows[0];
            // Parse JSON fields defensively
            let incrementalMetadata = ds.incremental_metadata;
            if (typeof incrementalMetadata === 'string') {
              try { incrementalMetadata = JSON.parse(incrementalMetadata); } catch { /* keep as string */ }
            }
            let strategyDecisionLog = ds.strategy_decision_log;
            if (typeof strategyDecisionLog === 'string') {
              try { strategyDecisionLog = JSON.parse(strategyDecisionLog); } catch { /* keep as string */ }
            }
            dataset_context = {
              incremental_strategy: ds.incremental_strategy || null,
              incremental_metadata: incrementalMetadata || null,
              discovery_status: ds.discovery_status || null,
              discovery_suggestion: ds.discovery_suggestion || null,
              enable_incremental: ds.enable_incremental ?? false,
              strategy_decision_log: strategyDecisionLog || null,
            };
          }
          if (prevRows.length) {
            const pr = prevRows[0];
            previous_run = {
              run_id: pr.run_id,
              silver_row_count: pr.silver_row_count != null ? Number(pr.silver_row_count) : null,
              bronze_row_count: pr.bronze_row_count != null ? Number(pr.bronze_row_count) : null,
              duration_seconds: pr.duration_seconds != null ? Number(pr.duration_seconds) : null,
              finished_at: pr.finished_at || null,
            };
          }
        } catch (enrichErr) {
          // Non-fatal: log and continue without enrichment
          console.warn('[PORTAL] Enrichment failed for run', runId, enrichErr.message);
        }
      }

      return res.json({ ok: true, batch_process: bp[0], run_queue: rq[0] || null, table_details: details, dataset_context, previous_run });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Lightweight status endpoint for real-time UI polling
  app.get('/api/portal/runs/:runId/status', async (req, res) => {
    const runId = String(req.params.runId || '').trim();

    try {
      const bp = await sqlQueryObjects(
        `SELECT run_id, dataset_id, queue_id, status, started_at, finished_at, bronze_row_count, silver_row_count, error_class, error_message\n` +
          `FROM ${portalCfg.opsSchema}.batch_process\n` +
          `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
          `LIMIT 1`
      );
      if (!bp.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      return res.json({ ok: true, batch_process: bp[0] });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Monitor endpoint (optimized for Portal Pro): batch_process + related run_queue (no table_details)
  app.get('/api/portal/runs/:runId/monitor', async (req, res) => {
    const runId = String(req.params.runId || '').trim();

    try {
      const bp = await sqlQueryObjects(
        `SELECT run_id, dataset_id, queue_id, execution_mode, status, started_at, finished_at,\n` +
          `       orchestrator_job_id, orchestrator_run_id, orchestrator_task,\n` +
          `       bronze_row_count, silver_row_count, error_class, error_message, error_stacktrace\n` +
          `FROM ${portalCfg.opsSchema}.batch_process\n` +
          `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
          `LIMIT 1`
      );
      if (!bp.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      let rq = [];
      if (bp[0].queue_id) {
        rq = await sqlQueryObjects(
          `SELECT queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, claim_owner, claimed_at,\n` +
            `       attempt, max_retries, next_retry_at, last_error_class, last_error_message, started_at, finished_at, correlation_id, run_id\n` +
            `FROM ${portalCfg.opsSchema}.run_queue\n` +
            `WHERE queue_id = ${sqlStringLiteral(bp[0].queue_id)}\n` +
            `LIMIT 1`
        );
      } else {
        rq = await sqlQueryObjects(
          `SELECT queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, claim_owner, claimed_at,\n` +
            `       attempt, max_retries, next_retry_at, last_error_class, last_error_message, started_at, finished_at, correlation_id, run_id\n` +
            `FROM ${portalCfg.opsSchema}.run_queue\n` +
            `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
            `ORDER BY requested_at DESC\n` +
            `LIMIT 1`
        );
      }

      return res.json({ ok: true, batch_process: bp[0], run_queue: rq[0] || null });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== MONITOR ENDPOINTS FOR V2.HTML =====
  
  // Queue statistics for KPIs (enriched with 24h metrics)
  app.get('/api/portal/monitor/queue/stats', async (req, res) => {
    try {
      const [queueStats, batchStats24h] = await Promise.all([
        sqlQueryObjects(
          `SELECT status, COUNT(*) AS count\n` +
            `FROM ${portalCfg.opsSchema}.run_queue\n` +
            `GROUP BY status`
        ),
        sqlQueryObjects(
          `SELECT\n` +
            `  COUNT(*) AS total_24h,\n` +
            `  COUNT(CASE WHEN status = 'SUCCEEDED' THEN 1 END) AS success_24h,\n` +
            `  COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_24h,\n` +
            `  ROUND(AVG(CASE WHEN status = 'SUCCEEDED' AND finished_at IS NOT NULL THEN CAST(TIMESTAMPDIFF(SECOND, started_at, finished_at) AS DOUBLE) END), 0) AS avg_duration_sec\n` +
            `FROM ${portalCfg.opsSchema}.batch_process\n` +
            `WHERE started_at >= current_timestamp() - INTERVAL 24 HOURS`
        ).catch(() => [{}]),
      ]);
      
      const statsObj = {};
      queueStats.forEach(s => {
        statsObj[s.status] = Number(s.count || 0);
      });

      const b = batchStats24h[0] || {};
      const total24h = Number(b.total_24h || 0);
      const success24h = Number(b.success_24h || 0);
      const failed24h = Number(b.failed_24h || 0);
      const avgDurationSec = Number(b.avg_duration_sec || 0);
      const successRate = total24h > 0 ? Math.round((success24h / total24h) * 1000) / 10 : 0;
      
      return res.json({
        ok: true,
        stats: statsObj,
        metrics_24h: {
          total: total24h,
          success: success24h,
          failed: failed24h,
          avg_duration_sec: avgDurationSec,
          success_rate: successRate,
        },
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // SRE dashboard (views + transition audit)
  app.get('/api/portal/monitor/sre-dashboard', async (req, res) => {
    const limit = clampLimit(req.query.limit, 100);
    try {
      const [realtimeRows, summaryRows, latencyRows, transitionRows, staleRows] = await Promise.all([
        sqlQueryObjects(
          `
SELECT snapshot_at, status, item_count, stale_claimed_count, delayed_retry_count, pending_over_60m_count
FROM ${portalCfg.opsSchema}.vw_run_queue_sre_realtime
ORDER BY status
          `.trim()
        ),
        sqlQueryObjects(
          `
SELECT
  COUNT(*) AS total_items,
  SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
  SUM(CASE WHEN status = 'CLAIMED' THEN 1 ELSE 0 END) AS claimed_count,
  SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) AS running_count,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
  SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
  SUM(CASE WHEN status = 'CLAIMED'
            AND claim_timestamp IS NOT NULL
            AND claim_timestamp < current_timestamp() - INTERVAL 30 MINUTES
           THEN 1
           ELSE 0
      END) AS stale_claimed_count
FROM ${portalCfg.opsSchema}.run_queue
          `.trim()
        ),
        sqlQueryObjects(
          `
SELECT
  percentile_approx(time_to_first_claim_ms, 0.50) AS p50_time_to_claim_ms,
  percentile_approx(time_to_first_claim_ms, 0.95) AS p95_time_to_claim_ms,
  percentile_approx(time_claimed_to_running_ms, 0.50) AS p50_time_claimed_to_running_ms,
  percentile_approx(time_claimed_to_running_ms, 0.95) AS p95_time_claimed_to_running_ms
FROM ${portalCfg.opsSchema}.vw_run_queue_sre_history
WHERE requested_at >= current_timestamp() - INTERVAL 24 HOURS
          `.trim()
        ),
        sqlQueryObjects(
          `
SELECT
  date_trunc('minute', created_at) AS ts_min,
  SUM(CASE WHEN new_status = 'CLAIMED' THEN 1 ELSE 0 END) AS claim_success_count,
  SUM(CASE WHEN new_status = 'RUNNING' THEN 1 ELSE 0 END) AS running_transition_count,
  SUM(CASE WHEN new_status IN ('SUCCEEDED','FAILED','CANCELLED') THEN 1 ELSE 0 END) AS terminal_transition_count
FROM ${portalCfg.opsSchema}.run_queue_transitions
WHERE created_at >= current_timestamp() - INTERVAL 6 HOURS
GROUP BY date_trunc('minute', created_at)
ORDER BY ts_min DESC
LIMIT ${limit}
          `.trim()
        ),
        sqlQueryObjects(
          `
SELECT queue_id, dataset_id, status, claim_owner, claim_timestamp,
       CAST(TIMESTAMPDIFF(SECOND, claim_timestamp, current_timestamp()) AS BIGINT) AS claim_age_seconds
FROM ${portalCfg.opsSchema}.run_queue
WHERE status = 'CLAIMED' AND claim_timestamp IS NOT NULL
ORDER BY claim_timestamp ASC
LIMIT ${limit}
          `.trim()
        ),
      ]);

      return res.json({
        ok: true,
        realtime: realtimeRows || [],
        summary: summaryRows?.[0] || {},
        latency: latencyRows?.[0] || {},
        transition_timeline: transitionRows || [],
        stale_claim_candidates: staleRows || [],
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Dataset executions with duration
  app.get('/api/portal/monitor/datasets/:datasetId/executions', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const limit = clampLimit(req.query.limit, 50);
    
    try {
      const rows = await sqlQueryObjects(
        `SELECT bp.run_id, bp.dataset_id, bp.status, bp.started_at, bp.finished_at,\n` +
          `       bp.bronze_row_count, bp.silver_row_count, bp.error_class, bp.error_message,\n` +
          `       CAST(TIMESTAMPDIFF(SECOND, bp.started_at, bp.finished_at) AS BIGINT) AS duration_seconds,\n` +
          `       dc.dataset_name\n` +
          `FROM ${portalCfg.opsSchema}.batch_process bp\n` +
          `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON bp.dataset_id = dc.dataset_id\n` +
          `WHERE bp.dataset_id = ${sqlStringLiteral(datasetId)}\n` +
          `ORDER BY bp.started_at DESC\n` +
          `LIMIT ${limit}`
      );
      
      const datasetName = rows.length > 0 ? rows[0].dataset_name : datasetId;
      
      return res.json({ ok: true, items: rows, dataset_name: datasetName });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Execution steps with duration
  app.get('/api/portal/monitor/executions/:runId/steps', async (req, res) => {
    const runId = String(req.params.runId || '').trim();
    const limit = clampLimit(req.query.limit, 200);
    
    try {
      const steps = await sqlQueryObjects(
        `SELECT step_id, run_id, dataset_id, phase, step_key, status, message,\n` +
          `       progress_current, progress_total, started_at, updated_at, finished_at, details_json,\n` +
          `       CAST(TIMESTAMPDIFF(SECOND, started_at, finished_at) AS BIGINT) AS step_duration_seconds\n` +
          `FROM ${portalCfg.opsSchema}.batch_process_steps\n` +
          `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
          `ORDER BY started_at ASC\n` +
          `LIMIT ${limit}`
      );
      
      return res.json({ ok: true, items: steps });
    } catch (e) {
      const msg = String(e.message || '');
      if (/not found|TABLE_OR_VIEW_NOT_FOUND|batch_process_steps/i.test(msg)) {
        return res.json({ ok: true, items: [], warning: 'batch_process_steps não existe' });
      }
      
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Queue table (enhanced with dataset details + pagination)
  app.get('/api/portal/monitor/queue', async (req, res) => {
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const pageSize = Math.min(200, Math.max(1, parseInt(req.query.page_size, 10) || 25));
    const offset = (page - 1) * pageSize;
    const statusFilter = String(req.query.status || '').trim().toUpperCase();
    const search = String(req.query.search || '').trim();
    
    try {
      const conditions = [];
      if (statusFilter) conditions.push(`rq.status = ${sqlStringLiteral(statusFilter)}`);
      if (search) {
        const term = sqlStringLiteral(`%${search}%`);
        conditions.push(`(LOWER(dc.dataset_name) LIKE LOWER(${term}) OR LOWER(rq.dataset_id) LIKE LOWER(${term}))`);
      }
      const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

      const [countResult, rows] = await Promise.all([
        sqlQueryObjects(
          `SELECT COUNT(*) AS cnt\n` +
            `FROM ${portalCfg.opsSchema}.run_queue rq\n` +
            `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON rq.dataset_id = dc.dataset_id\n` +
            `${whereClause}`
        ),
        sqlQueryObjects(
          `SELECT rq.queue_id, rq.dataset_id, rq.status, rq.trigger_type, rq.requested_by, rq.requested_at,\n` +
            `       rq.priority, rq.attempt, rq.max_retries, rq.next_retry_at, rq.last_error_class, rq.last_error_message,\n` +
            `       rq.claim_owner, rq.claimed_at, rq.started_at, rq.finished_at, rq.run_id,\n` +
            `       dc.dataset_name, dc.source_type, dc.connection_id, dc.execution_state,\n` +
            `       dc.bronze_table, dc.silver_table\n` +
            `FROM ${portalCfg.opsSchema}.run_queue rq\n` +
            `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON rq.dataset_id = dc.dataset_id\n` +
            `${whereClause}\n` +
            `ORDER BY rq.requested_at DESC\n` +
            `LIMIT ${pageSize} OFFSET ${offset}`
        ),
      ]);

      const total = parseInt(countResult[0]?.cnt || '0', 10);
      const totalPages = Math.max(1, Math.ceil(total / pageSize));
      
      return res.json({ ok: true, items: rows, total, page, page_size: pageSize, total_pages: totalPages });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Recent batch processes (with pagination + filters)
  app.get('/api/portal/monitor/batch-processes/recent', async (req, res) => {
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const pageSize = Math.min(200, Math.max(1, parseInt(req.query.page_size, 10) || 25));
    const offset = (page - 1) * pageSize;
    const search = String(req.query.search || '').trim();
    const statusFilter = String(req.query.status || '').trim().toUpperCase();
    const period = String(req.query.period || '').trim();
    
    try {
      const conditions = [];
      if (search) {
        const term = sqlStringLiteral(`%${search}%`);
        conditions.push(`(LOWER(dc.dataset_name) LIKE LOWER(${term}) OR LOWER(bp.dataset_id) LIKE LOWER(${term}))`);
      }
      if (statusFilter) conditions.push(`bp.status = ${sqlStringLiteral(statusFilter)}`);
      if (period === '24h') conditions.push(`bp.started_at >= current_timestamp() - INTERVAL 24 HOURS`);
      else if (period === '7d') conditions.push(`bp.started_at >= current_timestamp() - INTERVAL 7 DAYS`);
      else if (period === '30d') conditions.push(`bp.started_at >= current_timestamp() - INTERVAL 30 DAYS`);

      const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

      const [countResult, rows] = await Promise.all([
        sqlQueryObjects(
          `SELECT COUNT(*) AS cnt\n` +
            `FROM ${portalCfg.opsSchema}.batch_process bp\n` +
            `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON bp.dataset_id = dc.dataset_id\n` +
            `${whereClause}`
        ),
        sqlQueryObjects(
          `SELECT bp.run_id, bp.dataset_id, bp.status, bp.started_at, bp.finished_at,\n` +
            `       bp.bronze_row_count, bp.silver_row_count, bp.error_class, bp.error_message,\n` +
            `       bp.load_type, bp.incremental_rows_read, bp.watermark_start, bp.watermark_end,\n` +
            `       CAST(TIMESTAMPDIFF(SECOND, bp.started_at, bp.finished_at) AS BIGINT) AS duration_seconds,\n` +
            `       dc.dataset_name, dc.incremental_strategy, dc.enable_incremental,\n` +
            `       td.inserted_count AS bronze_inserted_count, td.updated_count AS bronze_updated_count, td.operation AS bronze_operation\n` +
            `FROM ${portalCfg.opsSchema}.batch_process bp\n` +
            `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON bp.dataset_id = dc.dataset_id\n` +
            `LEFT JOIN ${portalCfg.opsSchema}.batch_process_table_details td ON td.run_id = bp.run_id AND td.layer = 'BRONZE'\n` +
            `${whereClause}\n` +
            `ORDER BY bp.started_at DESC\n` +
            `LIMIT ${pageSize} OFFSET ${offset}`
        ),
      ]);

      const total = parseInt(countResult[0]?.cnt || '0', 10);
      const totalPages = Math.max(1, Math.ceil(total / pageSize));
      
      return res.json({ ok: true, items: rows, total, page, page_size: pageSize, total_pages: totalPages });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Failed jobs (with pagination)
  app.get('/api/portal/monitor/queue/failed', async (req, res) => {
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const pageSize = Math.min(200, Math.max(1, parseInt(req.query.page_size, 10) || 25));
    const offset = (page - 1) * pageSize;
    const search = String(req.query.search || '').trim();
    
    try {
      const conditions = ["rq.status = 'FAILED'"];
      if (search) {
        const term = sqlStringLiteral(`%${search}%`);
        conditions.push(`(LOWER(dc.dataset_name) LIKE LOWER(${term}) OR LOWER(rq.dataset_id) LIKE LOWER(${term}))`);
      }
      const whereClause = `WHERE ${conditions.join(' AND ')}`;

      const [countResult, rows] = await Promise.all([
        sqlQueryObjects(
          `SELECT COUNT(*) AS cnt\n` +
            `FROM ${portalCfg.opsSchema}.run_queue rq\n` +
            `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON rq.dataset_id = dc.dataset_id\n` +
            `${whereClause}`
        ),
        sqlQueryObjects(
          `SELECT rq.queue_id, rq.dataset_id, rq.status, rq.attempt, rq.max_retries,\n` +
            `       rq.last_error_class, rq.last_error_message, rq.requested_at, rq.next_retry_at,\n` +
            `       rq.started_at, rq.finished_at, rq.run_id,\n` +
            `       dc.dataset_name\n` +
            `FROM ${portalCfg.opsSchema}.run_queue rq\n` +
            `LEFT JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON rq.dataset_id = dc.dataset_id\n` +
            `${whereClause}\n` +
            `ORDER BY rq.finished_at DESC NULLS LAST, rq.requested_at DESC\n` +
            `LIMIT ${pageSize} OFFSET ${offset}`
        ),
      ]);

      const total = parseInt(countResult[0]?.cnt || '0', 10);
      const totalPages = Math.max(1, Math.ceil(total / pageSize));
      
      return res.json({ ok: true, items: rows, total, page, page_size: pageSize, total_pages: totalPages });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Step timeline endpoint (batch_process_steps)
  app.get('/api/portal/runs/:runId/steps', async (req, res) => {
    const runId = String(req.params.runId || '').trim();
    const limit = clampLimit(req.query.limit, 200);

    try {
      const steps = await sqlQueryObjects(
        `SELECT step_id, run_id, dataset_id, phase, step_key, status, message, progress_current, progress_total, started_at, updated_at, finished_at, details_json\n` +
          `FROM ${portalCfg.opsSchema}.batch_process_steps\n` +
          `WHERE run_id = ${sqlStringLiteral(runId)}\n` +
          `ORDER BY started_at ASC\n` +
          `LIMIT ${limit}`
      );

      // best-effort parse of details_json
      const items = steps.map((s) => {
        let details = null;
        try {
          details = s.details_json ? JSON.parse(s.details_json) : null;
        } catch {
          details = null;
        }
        return { ...s, details };
      });

      return res.json({ ok: true, items });
    } catch (e) {
      // If table doesn't exist yet, keep UI functional.
      const msg = String(e.message || '');
      if (/not found|TABLE_OR_VIEW_NOT_FOUND|batch_process_steps/i.test(msg)) {
        return res.json({ ok: true, items: [], warning: 'batch_process_steps não existe (migre o schema/rode o orquestrador novo).' });
      }

      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/approvals/pending', async (req, res) => {
    const limit = clampLimit(req.query.limit, 50);

    try {
      const rows = await sqlQueryObjects(
        `SELECT dc.dataset_id, dc.dataset_name, dc.project_id, dc.area_id, dc.execution_state, dc.current_schema_ver,\n` +
          `       p.schema_version AS pending_schema_version, p.created_at AS pending_created_at, p.created_by AS pending_created_by, p.schema_fingerprint AS pending_fingerprint, p.expect_schema_json AS pending_json,\n` +
          `       a.schema_version AS active_schema_version, a.schema_fingerprint AS active_fingerprint, a.expect_schema_json AS active_json\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control dc\n` +
          `JOIN ${portalCfg.ctrlSchema}.schema_versions p ON p.dataset_id = dc.dataset_id AND p.status = 'PENDING'\n` +
          `LEFT JOIN ${portalCfg.ctrlSchema}.schema_versions a ON a.dataset_id = dc.dataset_id AND a.status = 'ACTIVE'\n` +
          `ORDER BY p.created_at DESC\n` +
          `LIMIT ${limit}`
      );

      const items = rows.map((r) => {
        let activeJson = null;
        let pendingJson = null;
        try {
          activeJson = r.active_json ? JSON.parse(r.active_json) : null;
        } catch {
          activeJson = null;
        }
        try {
          pendingJson = r.pending_json ? JSON.parse(r.pending_json) : null;
        } catch {
          pendingJson = null;
        }

        const diff = activeJson && pendingJson ? diffExpectSchemas(activeJson, pendingJson) : [];
        const diffPreview = diff.slice(0, 20);

        return {
          dataset_id: r.dataset_id,
          dataset_name: r.dataset_name,
          project_id: r.project_id,
          area_id: r.area_id,
          execution_state: r.execution_state,
          current_schema_ver: r.current_schema_ver,
          pending: {
            schema_version: r.pending_schema_version,
            created_at: r.pending_created_at,
            created_by: r.pending_created_by,
            fingerprint: r.pending_fingerprint,
          },
          active: r.active_schema_version
            ? {
                schema_version: r.active_schema_version,
                fingerprint: r.active_fingerprint,
              }
            : null,
          diff_summary: {
            total: diff.length,
            add: diff.filter((d) => d.type === 'ADD_COLUMN').length,
            remove: diff.filter((d) => d.type === 'REMOVE_COLUMN').length,
            type_change: diff.filter((d) => d.type === 'TYPE_CHANGE').length,
            nullability_change: diff.filter((d) => d.type === 'NULLABILITY_CHANGE').length,
          },
          diff_preview: diffPreview,
        };
      });

      return res.json({ ok: true, items });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/datasets/:datasetId/schema', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();

    try {
      const versions = await sqlQueryObjects(
        `SELECT dataset_id, schema_version, schema_fingerprint, status, created_at, created_by, expect_schema_json\n` +
          `FROM ${portalCfg.ctrlSchema}.schema_versions\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}\n` +
          `ORDER BY schema_version DESC`
      );

      const active = versions.find((v) => String(v.status || '').toUpperCase() === 'ACTIVE') || null;
      const pending = versions.find((v) => String(v.status || '').toUpperCase() === 'PENDING') || null;

      let activeJson = null;
      let pendingJson = null;
      try {
        activeJson = active?.expect_schema_json ? JSON.parse(active.expect_schema_json) : null;
      } catch {
        activeJson = null;
      }
      try {
        pendingJson = pending?.expect_schema_json ? JSON.parse(pending.expect_schema_json) : null;
      } catch {
        pendingJson = null;
      }

      const diff = activeJson && pendingJson ? diffExpectSchemas(activeJson, pendingJson) : [];

      return res.json({ ok: true, versions, active, pending, diff });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.get('/api/portal/datasets/:datasetId/preview', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const limit = clampLimit(req.query.limit, 10);

    try {
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, silver_table, current_schema_ver, last_success_run_id\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!dsArr.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      const ds = dsArr[0];

      const bp = await sqlQueryObjects(
        `SELECT run_id, status, started_at, finished_at\n` +
          `FROM ${portalCfg.opsSchema}.batch_process\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND status = 'SUCCEEDED'\n` +
          `ORDER BY COALESCE(finished_at, started_at) DESC\n` +
          `LIMIT 1`
      );
      if (!bp.length) {
        return res.status(409).json({ ok: false, error: 'NO_SUCCESS_RUN', message: 'Não existe execução com sucesso para este dataset.' });
      }

      const v = await sqlQueryObjects(
        `SELECT expect_schema_json\n` +
          `FROM ${portalCfg.ctrlSchema}.schema_versions\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND schema_version = ${Number(ds.current_schema_ver || 0)}\n` +
          `LIMIT 1`
      );

      let orderBy = '';
      try {
        const js = v[0]?.expect_schema_json ? JSON.parse(v[0].expect_schema_json) : null;
        const oc = String(js?.order_column || '').trim();
        if (oc && isSafeIdentifier(oc)) {
          orderBy = ` ORDER BY ${oc} DESC`;
        }
      } catch {
        orderBy = '';
      }

      let silverTable = String(ds.silver_table || '').trim();
      if (!silverTable) return res.status(400).json({ ok: false, error: 'MISSING_SILVER_TABLE' });

      // suporta valores salvos como schema.table (prefixa com o catálogo do portal)
      const parts = silverTable.split('.');
      if (parts.length === 2) {
        silverTable = `${portalCfg.catalog}.${silverTable}`;
      }

      if (!isSafeTableName(silverTable)) {
        return res.status(400).json({ ok: false, error: 'INVALID_TABLE_NAME', message: 'silver_table inválida para preview.' });
      }

      const q = `SELECT * FROM ${silverTable}${orderBy} LIMIT ${limit}`;
      const out = await db.query(q);
      const columns = out.columns;
      const rows = out.rows;

      return res.json({
        ok: true,
        dataset_id: datasetId,
        silver_table: silverTable,
        schema_version: ds.current_schema_ver || null,
        run_id: bp[0].run_id,
        columns,
        rows,
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== PREVIEW NAMING (before creating dataset) =====
  
  app.post('/api/portal/datasets/naming-preview', async (req, res) => {
    const body = req.body || {};
    const areaId = String(body.area_id || '').trim();
    const datasetName = String(body.dataset_name || '').trim();
    const namingVersion = body.naming_version ? parseIntStrict(body.naming_version) : null;
    
    if (!areaId || !datasetName) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'area_id e dataset_name são obrigatórios.' });
    }
    
    try {
      // Load naming convention (specific version or active)
      let whereClause = '';
      if (namingVersion !== null) {
        whereClause = `WHERE naming_version = ${namingVersion}`;
      } else {
        whereClause = `WHERE is_active = true`;
      }
      
      const namingArr = await sqlQueryObjects(
        `SELECT naming_version, bronze_pattern, silver_pattern\n` +
          `FROM ${portalCfg.ctrlSchema}.naming_conventions\n` +
          `${whereClause}\n` +
          `ORDER BY naming_version DESC\n` +
          `LIMIT 1`
      );
      if (!namingArr.length) {
        return res.status(400).json({ ok: false, error: 'NO_NAMING', message: namingVersion ? `Naming convention v${namingVersion} não encontrada.` : 'Sem naming_conventions ativa.' });
      }
      
      const naming = namingArr[0];
      const bronzePattern = String(naming.bronze_pattern || '').trim();
      const silverPattern = String(naming.silver_pattern || '').trim();
      
      // Sanitize dataset name (same logic as creation)
      const sanitizedDatasetName = datasetName
        .split('@')[0]  // Remove @DBLINK
        .trim()
        .replace(/\./g, '_');  // Replace dots with underscores
      
      const bronzeShort = bronzePattern.replaceAll('{area}', areaId).replaceAll('{dataset}', sanitizedDatasetName);
      const silverShort = silverPattern.replaceAll('{area}', areaId).replaceAll('{dataset}', sanitizedDatasetName);
      
      const bronzeTable = `${portalCfg.catalog}.${bronzeShort}`;
      const silverTable = `${portalCfg.catalog}.${silverShort}`;
      
      // Parse table names for display
      const parseBronze = bronzeTable.split('.');
      const parseSilver = silverTable.split('.');
      
      return res.json({
        ok: true,
        preview: {
          bronze_table: bronzeTable,
          silver_table: silverTable,
          bronze_parts: {
            catalog: parseBronze[0] || '',
            schema: parseBronze[1] || '',
            table: parseBronze[2] || ''
          },
          silver_parts: {
            catalog: parseSilver[0] || '',
            schema: parseSilver[1] || '',
            table: parseSilver[2] || ''
          },
          sanitized_dataset_name: sanitizedDatasetName
        }
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  app.post('/api/portal/datasets', async (req, res) => {
    const body = req.body || {};

    const projectId = String(body.project_id || '').trim();
    const areaId = String(body.area_id || '').trim();
    const datasetName = String(body.dataset_name || '').trim();
    const sourceType = String(body.source_type || '').trim().toUpperCase();
    const connectionId = String(body.connection_id || '').trim();
    
    // NOVO: Custom table names (opcionais)
    const customBronzeTable = body.custom_bronze_table ? String(body.custom_bronze_table).trim() : null;
    const customSilverTable = body.custom_silver_table ? String(body.custom_silver_table).trim() : null;
    const namingVersion = body.naming_version != null ? parseIntStrict(body.naming_version) : null;

    const errors = [];
    if (!projectId) errors.push('project_id é obrigatório');
    if (!areaId) errors.push('area_id é obrigatório');
    if (!datasetName) errors.push('dataset_name é obrigatório');
    
    // Validação do dataset_name:
    // - ORACLE: permite OWNER.TABLE@DBLINK (ex: CMASTER.CMALU@CMASTERPRD) ou TABLE simples
    // - SHAREPOINT: apenas A-Z, 0-9 e '_'
    if (datasetName) {
      if (sourceType === 'ORACLE') {
        // Oracle: permite alphanumeric + underscore + dot + @ (para schema.table@dblink)
        if (!/^[A-Za-z0-9_@.]+$/.test(datasetName)) {
          errors.push("dataset_name inválido para ORACLE (use: SCHEMA.TABELA@DBLINK ou TABELA)");
        }
      } else {
        // Outros source_types: apenas alphanumeric + underscore
        if (!/^[A-Za-z0-9_]+$/.test(datasetName)) {
          errors.push("dataset_name inválido (use apenas A-Z, 0-9 e '_')");
        }
      }
    }
    
    if (!['ORACLE', 'SHAREPOINT'].includes(sourceType)) errors.push('source_type inválido (ORACLE|SHAREPOINT)');
    if (!connectionId) errors.push('connection_id é obrigatório');

    if (errors.length) return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', errors });

    const createdBy = getRequestUser(req);
    const datasetId = crypto.randomUUID();

    try {
      let bronzeTable, silverTable;
      
      // NOVO: Se custom names foram informados, usar eles; senão aplicar naming_conventions
      if (customBronzeTable && customSilverTable) {
        // Validar formato: catalog.schema.table
        if (!isSafeTableName(customBronzeTable)) {
          return res.status(400).json({ ok: false, error: 'INVALID_BRONZE_TABLE', message: 'custom_bronze_table inválida (use: catalog.schema.table)' });
        }
        if (!isSafeTableName(customSilverTable)) {
          return res.status(400).json({ ok: false, error: 'INVALID_SILVER_TABLE', message: 'custom_silver_table inválida (use: catalog.schema.table)' });
        }
        
        bronzeTable = customBronzeTable;
        silverTable = customSilverTable;
        console.log(`[CREATE] Usando nomes customizados: bronze=${bronzeTable}, silver=${silverTable}`);
      } else {
        // Aplicar naming_conventions (versão específica ou ativa)
        const namingWhere = namingVersion != null
          ? `WHERE naming_version = ${namingVersion}`
          : `WHERE is_active = true`;
        const namingArr = await sqlQueryObjects(
          `SELECT naming_version, bronze_pattern, silver_pattern\n` +
            `FROM ${portalCfg.ctrlSchema}.naming_conventions\n` +
            `${namingWhere}\n` +
            `ORDER BY naming_version DESC\n` +
            `LIMIT 1`
        );
        if (!namingArr.length) return res.status(400).json({ ok: false, error: 'NO_NAMING', message: namingVersion != null ? `Naming convention v${namingVersion} não encontrada.` : 'Sem naming_conventions ativa.' });

        const naming = namingArr[0];
        const bronzePattern = String(naming.bronze_pattern || '').trim();
        const silverPattern = String(naming.silver_pattern || '').trim();

        // Sanitize dataset name for table naming:
        // 1. Remove @DBLINK suffix (Oracle DBLink): "CMASTER.GLO_AGENTES@CMASTERPRD" → "CMASTER.GLO_AGENTES"
        // 2. Replace dots with underscores to avoid multi-level table names: "CMASTER.GLO_AGENTES" → "CMASTER_GLO_AGENTES"
        const sanitizedDatasetName = datasetName
          .split('@')[0]  // Remove @DBLINK
          .trim()
          .replace(/\./g, '_');  // Replace dots with underscores

        const bronzeShort = bronzePattern.replaceAll('{area}', areaId).replaceAll('{dataset}', sanitizedDatasetName);
        const silverShort = silverPattern.replaceAll('{area}', areaId).replaceAll('{dataset}', sanitizedDatasetName);

        bronzeTable = `${portalCfg.catalog}.${bronzeShort}`;
        silverTable = `${portalCfg.catalog}.${silverShort}`;
        console.log(`[CREATE] Aplicando naming_conventions: bronze=${bronzeTable}, silver=${silverTable}`);
      }

      // Auto-create schemas if they don't exist
      const schemasToEnsure = new Set();
      for (const tbl of [bronzeTable, silverTable]) {
        const parts = tbl.split('.');
        if (parts.length === 3) schemasToEnsure.add(`${parts[0]}.${parts[1]}`);
      }
      for (const schema of schemasToEnsure) {
        try {
          await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
          console.log(`[CREATE] Schema garantido: ${schema}`);
        } catch (e) {
          console.warn(`[CREATE] Erro ao criar schema ${schema}: ${e.message}`);
        }
      }

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Dataset nasce ACTIVE (pronto para execução imediata)
      const insert =
        `INSERT INTO ${portalCfg.ctrlSchema}.dataset_control\n` +
        `(dataset_id, project_id, area_id, dataset_name, source_type, connection_id, execution_state, bronze_table, silver_table, current_schema_ver, last_success_run_id, created_at, created_by)\n` +
        `VALUES (\n` +
        `  ${sqlStringLiteral(datasetId)},\n` +
        `  ${sqlStringLiteral(projectId)},\n` +
        `  ${sqlStringLiteral(areaId)},\n` +
        `  ${sqlStringLiteral(datasetName)},\n` +
        `  ${sqlStringLiteral(sourceType)},\n` +
        `  ${sqlStringLiteral(connectionId)},\n` +
        `  'ACTIVE',\n` +
        `  ${sqlStringLiteral(bronzeTable)},\n` +
        `  ${sqlStringLiteral(silverTable)},\n` +
        `  NULL,\n` +
        `  NULL,\n` +
        `  TIMESTAMP ${sqlStringLiteral(now)},\n` +
        `  ${sqlStringLiteral(createdBy)}\n` +
        `)`;

      await db.query(insert);

      const changeId = crypto.randomUUID();
      const audit =
        `INSERT INTO ${portalCfg.ctrlSchema}.dataset_state_changes\n` +
        `(change_id, dataset_id, old_state, new_state, reason, changed_at, changed_by)\n` +
        `VALUES (\n` +
        `  ${sqlStringLiteral(changeId)},\n` +
        `  ${sqlStringLiteral(datasetId)},\n` +
        `  'NONE',\n` +
        `  'ACTIVE',\n` +
        `  'CREATED_BY_PORTAL',\n` +
        `  TIMESTAMP ${sqlStringLiteral(now)},\n` +
        `  ${sqlStringLiteral(createdBy)}\n` +
        `)`;
      await db.query(audit);

      return res.status(201).json({
        ok: true,
        item: {
          dataset_id: datasetId,
          project_id: projectId,
          area_id: areaId,
          dataset_name: datasetName,
          source_type: sourceType,
          connection_id: connectionId,
          execution_state: 'ACTIVE',
          bronze_table: bronzeTable,
          silver_table: silverTable,
        },
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== BULK CREATE DATASETS =====

  app.post('/api/portal/datasets/bulk/validate', async (req, res) => {
    const body = req.body || {};
    const projectId = String(body.project_id || '').trim();
    const areaId = String(body.area_id || '').trim();
    const sourceType = String(body.source_type || '').trim().toUpperCase();
    const connectionId = String(body.connection_id || '').trim();
    const rawNames = Array.isArray(body.dataset_names) ? body.dataset_names : [];

    if (!projectId || !areaId || !sourceType || !connectionId) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'project_id, area_id, source_type e connection_id são obrigatórios.' });
    }

    try {
      // Parse and dedupe
      const parsed = rawNames
        .map(n => String(n || '').trim())
        .filter(n => n.length > 0);

      const seen = new Map(); // name -> first index
      const items = [];

      for (let i = 0; i < parsed.length; i++) {
        const name = parsed[i];
        const item = { index: i, dataset_name: name, status: 'VALID', message: '' };

        // Format validation
        if (sourceType === 'ORACLE') {
          if (!/^[A-Za-z0-9_@.$#]+$/.test(name)) {
            item.status = 'ERROR';
            item.message = 'Caracteres inválidos para Oracle (permitido: A-Z, 0-9, _, @, ., $, #)';
          }
        } else {
          if (!/^[A-Za-z0-9_]+$/.test(name)) {
            item.status = 'ERROR';
            item.message = 'Caracteres inválidos (permitido: A-Z, 0-9, _)';
          }
        }

        // Duplicate within list
        if (item.status === 'VALID') {
          const upper = name.toUpperCase();
          if (seen.has(upper)) {
            item.status = 'DUPLICATE';
            item.message = `Duplicado (mesmo que linha ${seen.get(upper) + 1})`;
          } else {
            seen.set(upper, i);
          }
        }

        items.push(item);
      }

      // Check existing datasets in DB (batch)
      const validNames = items.filter(it => it.status === 'VALID').map(it => it.dataset_name);
      const existingSet = new Set();

      if (validNames.length > 0) {
        // Query in batches of 50
        for (let b = 0; b < validNames.length; b += 50) {
          const batch = validNames.slice(b, b + 50);
          const inClause = batch.map(n => sqlStringLiteral(n)).join(', ');
          const existing = await sqlQueryObjects(
            `SELECT dataset_name FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
              `WHERE dataset_name IN (${inClause})\n` +
              `AND project_id = ${sqlStringLiteral(projectId)}\n` +
              `AND area_id = ${sqlStringLiteral(areaId)}`
          );
          for (const row of existing) {
            existingSet.add(String(row.dataset_name || '').toUpperCase());
          }
        }
      }

      // Mark existing
      for (const item of items) {
        if (item.status === 'VALID' && existingSet.has(item.dataset_name.toUpperCase())) {
          item.status = 'EXISTS';
          item.message = 'Dataset já cadastrado neste Projeto/Área';
        }
      }

      const summary = {
        total: items.length,
        valid: items.filter(it => it.status === 'VALID').length,
        error: items.filter(it => it.status === 'ERROR').length,
        duplicate: items.filter(it => it.status === 'DUPLICATE').length,
        exists: items.filter(it => it.status === 'EXISTS').length,
      };

      return res.json({ ok: true, items, summary });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.post('/api/portal/datasets/bulk', async (req, res) => {
    const body = req.body || {};
    const projectId = String(body.project_id || '').trim();
    const areaId = String(body.area_id || '').trim();
    const sourceType = String(body.source_type || '').trim().toUpperCase();
    const connectionId = String(body.connection_id || '').trim();
    const rawNames = Array.isArray(body.dataset_names) ? body.dataset_names : [];
    const namingVersion = body.naming_version != null ? parseIntStrict(body.naming_version) : null;
    const createdBy = getRequestUser(req);

    if (!projectId || !areaId || !sourceType || !connectionId) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Campos obrigatórios ausentes.' });
    }
    if (rawNames.length === 0) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Nenhum dataset_name fornecido.' });
    }
    if (rawNames.length > 200) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Máximo de 200 datasets por vez.' });
    }

    try {
      // Load naming convention (versão específica ou ativa)
      const bulkNamingWhere = namingVersion != null
        ? `WHERE naming_version = ${namingVersion}`
        : `WHERE is_active = true`;
      const namingArr = await sqlQueryObjects(
        `SELECT naming_version, bronze_pattern, silver_pattern\n` +
          `FROM ${portalCfg.ctrlSchema}.naming_conventions\n` +
          `${bulkNamingWhere}\n` +
          `ORDER BY naming_version DESC\n` +
          `LIMIT 1`
      );
      if (!namingArr.length) return res.status(400).json({ ok: false, error: 'NO_NAMING', message: namingVersion != null ? `Naming convention v${namingVersion} não encontrada.` : 'Sem naming_conventions ativa.' });

      const naming = namingArr[0];
      const bronzePattern = String(naming.bronze_pattern || '').trim();
      const silverPattern = String(naming.silver_pattern || '').trim();

      // Load area_name to use for catalog naming (normalized)
      const areaArr = await sqlQueryObjects(
        `SELECT area_name FROM ${portalCfg.ctrlSchema}.areas\n` +
          `WHERE area_id = ${sqlStringLiteral(areaId)} LIMIT 1`
      );
      const areaName = areaArr.length ? String(areaArr[0].area_name || '').trim() : areaId;
      // Normalize area name: lowercase, replace spaces/special chars with underscore
      const normalizedAreaName = areaName
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '') // Remove accents
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, ''); // Trim underscores
      const areaForNaming = normalizedAreaName || areaId;

      // Parse, validate, dedupe
      const parsed = rawNames.map(n => String(n || '').trim()).filter(n => n.length > 0);
      const seen = new Set();
      const validNames = [];
      const results = [];

      for (const name of parsed) {
        const upper = name.toUpperCase();

        // Format check
        const validChars = sourceType === 'ORACLE'
          ? /^[A-Za-z0-9_@.$#]+$/.test(name)
          : /^[A-Za-z0-9_]+$/.test(name);

        if (!validChars) {
          results.push({ dataset_name: name, status: 'ERROR', message: 'Caracteres inválidos' });
          continue;
        }

        if (seen.has(upper)) {
          results.push({ dataset_name: name, status: 'DUPLICATE', message: 'Duplicado na lista' });
          continue;
        }
        seen.add(upper);
        validNames.push(name);
      }

      // Check existing in DB
      const existingSet = new Set();
      for (let b = 0; b < validNames.length; b += 50) {
        const batch = validNames.slice(b, b + 50);
        const inClause = batch.map(n => sqlStringLiteral(n)).join(', ');
        const existing = await sqlQueryObjects(
          `SELECT dataset_name FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
            `WHERE dataset_name IN (${inClause})\n` +
            `AND project_id = ${sqlStringLiteral(projectId)}\n` +
            `AND area_id = ${sqlStringLiteral(areaId)}`
        );
        for (const row of existing) {
          existingSet.add(String(row.dataset_name || '').toUpperCase());
        }
      }

      // Auto-create schemas if they don't exist (compute from first valid name)
      {
        const sampleName = (validNames[0] || '').split('@')[0].trim().replace(/\./g, '_');
        const sampleBronze = `${portalCfg.catalog}.${bronzePattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sampleName)}`;
        const sampleSilver = `${portalCfg.catalog}.${silverPattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sampleName)}`;
        const schemasToEnsure = new Set();
        for (const tbl of [sampleBronze, sampleSilver]) {
          const parts = tbl.split('.');
          if (parts.length === 3) schemasToEnsure.add(`${parts[0]}.${parts[1]}`);
        }
        for (const schema of schemasToEnsure) {
          try {
            await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
            console.log(`[BULK] Schema garantido: ${schema}`);
          } catch (e) {
            console.warn(`[BULK] Erro ao criar schema ${schema}: ${e.message}`);
          }
        }
      }

      // Create each valid dataset
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      let created = 0;
      let failed = 0;

      for (const name of validNames) {
        if (existingSet.has(name.toUpperCase())) {
          results.push({ dataset_name: name, status: 'EXISTS', message: 'Já cadastrado' });
          continue;
        }

        try {
          const datasetId = crypto.randomUUID();
          const sanitizedName = name.split('@')[0].trim().replace(/\./g, '_');
          const bronzeShort = bronzePattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sanitizedName);
          const silverShort = silverPattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sanitizedName);
          const bronzeTable = `${portalCfg.catalog}.${bronzeShort}`;
          const silverTable = `${portalCfg.catalog}.${silverShort}`;

          await db.query(
            `INSERT INTO ${portalCfg.ctrlSchema}.dataset_control\n` +
            `(dataset_id, project_id, area_id, dataset_name, source_type, connection_id, execution_state, bronze_table, silver_table, current_schema_ver, last_success_run_id, created_at, created_by)\n` +
            `VALUES (\n` +
            `  ${sqlStringLiteral(datasetId)}, ${sqlStringLiteral(projectId)}, ${sqlStringLiteral(areaId)},\n` +
            `  ${sqlStringLiteral(name)}, ${sqlStringLiteral(sourceType)}, ${sqlStringLiteral(connectionId)},\n` +
            `  'ACTIVE', ${sqlStringLiteral(bronzeTable)}, ${sqlStringLiteral(silverTable)},\n` +
            `  NULL, NULL, TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(createdBy)}\n` +
            `)`
          );

          await db.query(
            `INSERT INTO ${portalCfg.ctrlSchema}.dataset_state_changes\n` +
            `(change_id, dataset_id, old_state, new_state, reason, changed_at, changed_by)\n` +
            `VALUES (${sqlStringLiteral(crypto.randomUUID())}, ${sqlStringLiteral(datasetId)}, 'NONE', 'ACTIVE', 'CREATED_BY_PORTAL_BULK', TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(createdBy)})`
          );

          results.push({ dataset_name: name, status: 'CREATED', dataset_id: datasetId, bronze_table: bronzeTable, silver_table: silverTable });
          created++;
        } catch (err) {
          results.push({ dataset_name: name, status: 'ERROR', message: err.message });
          failed++;
        }
      }

      const summary = {
        total: parsed.length,
        created,
        failed,
        exists: results.filter(r => r.status === 'EXISTS').length,
        duplicate: results.filter(r => r.status === 'DUPLICATE').length,
        error: results.filter(r => r.status === 'ERROR').length,
      };

      console.log(`[BULK] Criação em massa: ${JSON.stringify(summary)}`);
      return res.status(201).json({ ok: true, results, summary });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== BATCH CREATE DATASETS (async with progress) =====

  app.post('/api/portal/datasets/batch-create', async (req, res) => {
    const body = req.body || {};
    const projectId = String(body.project_id || '').trim();
    const areaId = String(body.area_id || '').trim();
    const sourceType = String(body.source_type || '').trim().toUpperCase();
    const connectionId = String(body.connection_id || '').trim();
    const rawNames = Array.isArray(body.dataset_names) ? body.dataset_names : [];
    const namingVersion = body.naming_version != null ? parseIntStrict(body.naming_version) : null;
    const createdBy = getRequestUser(req);

    if (!projectId || !areaId || !sourceType || !connectionId) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Campos obrigatórios ausentes.' });
    }
    if (rawNames.length === 0) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Nenhum dataset_name fornecido.' });
    }
    if (rawNames.length > 200) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Máximo de 200 datasets por vez.' });
    }

    const batchId = crypto.randomUUID();
    const batchState = {
      batch_id: batchId,
      status: 'RUNNING',
      started_at_ms: Date.now(),
      total: rawNames.length,
      processed: 0,
      created: 0,
      failed: 0,
      exists: 0,
      duplicate: 0,
      error: 0,
      results: [],
    };
    batchStore.set(batchId, batchState);

    // Return immediately with batch_id
    res.status(202).json({ ok: true, batch_id: batchId });

    // Process in background (async, outside of request lifecycle)
    (async () => {
      try {
        // Load naming convention (versão específica ou ativa)
        const batchNamingWhere = namingVersion != null
          ? `WHERE naming_version = ${namingVersion}`
          : `WHERE is_active = true`;
        const namingArr = await sqlQueryObjects(
          `SELECT naming_version, bronze_pattern, silver_pattern\n` +
            `FROM ${portalCfg.ctrlSchema}.naming_conventions\n` +
            `${batchNamingWhere}\n` +
            `ORDER BY naming_version DESC\n` +
            `LIMIT 1`
        );
        if (!namingArr.length) {
          batchState.status = 'FAILED';
          batchState.error_message = namingVersion != null ? `Naming convention v${namingVersion} não encontrada.` : 'Sem naming_conventions ativa.';
          return;
        }

        const naming = namingArr[0];
        const bronzePattern = String(naming.bronze_pattern || '').trim();
        const silverPattern = String(naming.silver_pattern || '').trim();

        // Load area_name to use for catalog naming (normalized)
        const areaArr = await sqlQueryObjects(
          `SELECT area_name FROM ${portalCfg.ctrlSchema}.areas\n` +
            `WHERE area_id = ${sqlStringLiteral(areaId)} LIMIT 1`
        );
        const areaName = areaArr.length ? String(areaArr[0].area_name || '').trim() : areaId;
        // Normalize area name: lowercase, replace spaces/special chars with underscore
        const normalizedAreaName = areaName
          .toLowerCase()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '') // Remove accents
          .replace(/[^a-z0-9]+/g, '_')
          .replace(/^_+|_+$/g, ''); // Trim underscores
        const areaForNaming = normalizedAreaName || areaId;

        // Parse, validate, dedupe
        const parsed = rawNames.map(n => String(n || '').trim()).filter(n => n.length > 0);
        batchState.total = parsed.length;
        const seen = new Set();
        const validNames = [];

        for (const name of parsed) {
          const upper = name.toUpperCase();
          const validChars = sourceType === 'ORACLE'
            ? /^[A-Za-z0-9_@.$#]+$/.test(name)
            : /^[A-Za-z0-9_]+$/.test(name);

          if (!validChars) {
            batchState.results.push({ dataset_name: name, status: 'ERROR', message: 'Caracteres inválidos' });
            batchState.error++;
            batchState.processed++;
            continue;
          }
          if (seen.has(upper)) {
            batchState.results.push({ dataset_name: name, status: 'DUPLICATE', message: 'Duplicado na lista' });
            batchState.duplicate++;
            batchState.processed++;
            continue;
          }
          seen.add(upper);
          validNames.push(name);
        }

        // Check existing in DB
        const existingSet = new Set();
        for (let b = 0; b < validNames.length; b += 50) {
          const batch = validNames.slice(b, b + 50);
          const inClause = batch.map(n => sqlStringLiteral(n)).join(', ');
          const existing = await sqlQueryObjects(
            `SELECT dataset_name FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
              `WHERE dataset_name IN (${inClause})\n` +
              `AND project_id = ${sqlStringLiteral(projectId)}\n` +
              `AND area_id = ${sqlStringLiteral(areaId)}`
          );
          for (const row of existing) {
            existingSet.add(String(row.dataset_name || '').toUpperCase());
          }
        }

        // Auto-create schemas if they don't exist (compute from first valid name)
        if (validNames.length > 0) {
          const sampleName = validNames[0].split('@')[0].trim().replace(/\./g, '_');
          const sampleBronze = `${portalCfg.catalog}.${bronzePattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sampleName)}`;
          const sampleSilver = `${portalCfg.catalog}.${silverPattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sampleName)}`;
          const schemasToEnsure = new Set();
          for (const tbl of [sampleBronze, sampleSilver]) {
            const parts = tbl.split('.');
            if (parts.length === 3) schemasToEnsure.add(`${parts[0]}.${parts[1]}`);
          }
          for (const schema of schemasToEnsure) {
            try {
              await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
              console.log(`[BATCH] Schema garantido: ${schema}`);
            } catch (e) {
              console.warn(`[BATCH] Erro ao criar schema ${schema}: ${e.message}`);
            }
          }
        }

        // Create each valid dataset one-by-one, updating progress
        const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

        for (const name of validNames) {
          if (existingSet.has(name.toUpperCase())) {
            batchState.results.push({ dataset_name: name, status: 'EXISTS', message: 'Já cadastrado' });
            batchState.exists++;
            batchState.processed++;
            continue;
          }

          try {
            const datasetId = crypto.randomUUID();
            const sanitizedName = name.split('@')[0].trim().replace(/\./g, '_');
            const bronzeShort = bronzePattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sanitizedName);
            const silverShort = silverPattern.replaceAll('{area}', areaForNaming).replaceAll('{dataset}', sanitizedName);
            const bronzeTable = `${portalCfg.catalog}.${bronzeShort}`;
            const silverTable = `${portalCfg.catalog}.${silverShort}`;

            await db.query(
              `INSERT INTO ${portalCfg.ctrlSchema}.dataset_control\n` +
              `(dataset_id, project_id, area_id, dataset_name, source_type, connection_id, execution_state, bronze_table, silver_table, current_schema_ver, last_success_run_id, created_at, created_by)\n` +
              `VALUES (\n` +
              `  ${sqlStringLiteral(datasetId)}, ${sqlStringLiteral(projectId)}, ${sqlStringLiteral(areaId)},\n` +
              `  ${sqlStringLiteral(name)}, ${sqlStringLiteral(sourceType)}, ${sqlStringLiteral(connectionId)},\n` +
              `  'ACTIVE', ${sqlStringLiteral(bronzeTable)}, ${sqlStringLiteral(silverTable)},\n` +
              `  NULL, NULL, TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(createdBy)}\n` +
              `)`
            );

            await db.query(
              `INSERT INTO ${portalCfg.ctrlSchema}.dataset_state_changes\n` +
              `(change_id, dataset_id, old_state, new_state, reason, changed_at, changed_by)\n` +
              `VALUES (${sqlStringLiteral(crypto.randomUUID())}, ${sqlStringLiteral(datasetId)}, 'NONE', 'ACTIVE', 'CREATED_BY_PORTAL_BULK', TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(createdBy)})`
            );

            batchState.results.push({ dataset_name: name, status: 'CREATED', dataset_id: datasetId, bronze_table: bronzeTable, silver_table: silverTable });
            batchState.created++;
          } catch (err) {
            batchState.results.push({ dataset_name: name, status: 'ERROR', message: err.message });
            batchState.failed++;
          }
          batchState.processed++;
        }

        batchState.status = 'COMPLETED';
        batchState.finished_at_ms = Date.now();
        console.log(`[BATCH] Criação em massa concluída: batch=${batchId}, created=${batchState.created}, failed=${batchState.failed}, exists=${batchState.exists}`);
      } catch (e) {
        batchState.status = 'FAILED';
        batchState.error_message = e.message;
        batchState.finished_at_ms = Date.now();
        console.error(`[BATCH] Erro fatal no batch ${batchId}: ${e.message}`);
      }
    })();
  });

  app.get('/api/portal/datasets/batch-status/:batchId', (req, res) => {
    const batchId = String(req.params.batchId || '').trim();
    const batch = batchStore.get(batchId);
    if (!batch) {
      return res.status(404).json({ ok: false, error: 'NOT_FOUND', message: 'Batch não encontrado.' });
    }

    const elapsed_ms = (batch.finished_at_ms || Date.now()) - batch.started_at_ms;

    return res.json({
      ok: true,
      batch_id: batch.batch_id,
      status: batch.status,
      total: batch.total,
      processed: batch.processed,
      created: batch.created,
      failed: batch.failed,
      exists: batch.exists,
      duplicate: batch.duplicate,
      error: batch.error,
      elapsed_ms,
      error_message: batch.error_message || null,
      results: batch.results,
    });
  });

  // ===== BULK ENQUEUE DATASETS =====

  app.post('/api/portal/datasets/bulk-enqueue', async (req, res) => {
    const body = req.body || {};
    const datasetIds = Array.isArray(body.dataset_ids) ? body.dataset_ids : [];
    const strategy = String(body.strategy || 'parallel').trim().toLowerCase();
    const user = getRequestUser(req);

    if (!datasetIds.length) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'dataset_ids é obrigatório (array não vazio).' });
    }
    if (datasetIds.length > 200) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Máximo de 200 datasets por vez.' });
    }

    try {
      // Fetch all datasets in batches to validate eligibility
      const dsMap = new Map();
      for (let b = 0; b < datasetIds.length; b += 50) {
        const batch = datasetIds.slice(b, b + 50);
        const inClause = batch.map(id => sqlStringLiteral(String(id).trim())).join(', ');
        const rows = await sqlQueryObjects(
          `SELECT dataset_id, execution_state, dataset_name\n` +
            `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
            `WHERE dataset_id IN (${inClause})`
        );
        for (const row of rows) {
          dsMap.set(row.dataset_id, row);
        }
      }

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      const results = [];
      let enqueued = 0;
      let skipped = 0;
      const blockedStates = ['PAUSED', 'DEPRECATED', 'BLOCKED_SCHEMA_CHANGE'];

      // Determine priority: sequential uses descending priority so they run in order
      for (let i = 0; i < datasetIds.length; i++) {
        const dsId = String(datasetIds[i]).trim();
        const ds = dsMap.get(dsId);

        if (!ds) {
          results.push({ dataset_id: dsId, status: 'NOT_FOUND', message: 'Dataset não encontrado' });
          skipped++;
          continue;
        }

        const st = String(ds.execution_state || '').trim();
        if (blockedStates.includes(st)) {
          results.push({ dataset_id: dsId, dataset_name: ds.dataset_name, status: 'NOT_ELIGIBLE', message: `Estado ${st} não permite enqueue` });
          skipped++;
          continue;
        }

        try {
          const queueId = crypto.randomUUID();
          const priority = strategy === 'sequential' ? (200 - i) : 100;

          await db.query(
            `INSERT INTO ${portalCfg.opsSchema}.run_queue\n` +
              `(queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, attempt, max_retries)\n` +
              `VALUES (${sqlStringLiteral(queueId)}, ${sqlStringLiteral(dsId)}, 'MANUAL_BULK', ${sqlStringLiteral(user)}, TIMESTAMP ${sqlStringLiteral(now)}, ${priority}, 'PENDING', 0, 3)`
          );

          // Trigger orchestrator for each (in parallel mode they all fire; sequential relies on priority ordering)
          const triggerResult = await triggerOrchestratorJob(dsId);

          results.push({
            dataset_id: dsId,
            dataset_name: ds.dataset_name,
            status: 'ENQUEUED',
            queue_id: queueId,
            trigger: triggerResult && !triggerResult.error
              ? { triggered: true, databricks_run_id: triggerResult.run_id }
              : { triggered: false },
          });
          enqueued++;
        } catch (err) {
          results.push({ dataset_id: dsId, dataset_name: ds.dataset_name, status: 'ERROR', message: err.message });
          skipped++;
        }
      }

      console.log(`[BULK-ENQUEUE] ${enqueued} enqueued, ${skipped} skipped out of ${datasetIds.length}`);
      return res.json({
        ok: true,
        results,
        summary: { total: datasetIds.length, enqueued, skipped, error: results.filter(r => r.status === 'ERROR').length },
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.post('/api/portal/datasets/:datasetId/publish', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);

    try {
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, execution_state\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!dsArr.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      const oldState = String(dsArr[0].execution_state || '').trim().toUpperCase();

      // Guardrails: estados bloqueantes não podem enfileirar via publish
      if (['PAUSED', 'DEPRECATED', 'BLOCKED_SCHEMA_CHANGE'].includes(oldState)) {
        return res.status(409).json({ ok: false, error: 'NOT_ELIGIBLE', message: `Dataset não elegível para publish/enqueue (state=${oldState})` });
      }

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Se ainda está em DRAFT, promove para ACTIVE + audita
      if (oldState === 'DRAFT') {
        await db.query(
          `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
            `SET execution_state = 'ACTIVE', updated_at = TIMESTAMP ${sqlStringLiteral(now)}, updated_by = ${sqlStringLiteral(user)}\n` +
            `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
        );

        const changeId = crypto.randomUUID();
        await db.query(
          `INSERT INTO ${portalCfg.ctrlSchema}.dataset_state_changes\n` +
            `(change_id, dataset_id, old_state, new_state, reason, changed_at, changed_by)\n` +
            `VALUES (${sqlStringLiteral(changeId)}, ${sqlStringLiteral(datasetId)}, 'DRAFT', 'ACTIVE', 'PUBLISHED', TIMESTAMP ${sqlStringLiteral(
              now
            )}, ${sqlStringLiteral(user)})`
        );
      }

      // enqueue (inicial ou adicional)
      const queueId = crypto.randomUUID();
      await db.query(
        `INSERT INTO ${portalCfg.opsSchema}.run_queue\n` +
          `(queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, attempt, max_retries)\n` +
          `VALUES (${sqlStringLiteral(queueId)}, ${sqlStringLiteral(datasetId)}, 'MANUAL', ${sqlStringLiteral(user)}, TIMESTAMP ${sqlStringLiteral(
            now
          )}, 100, 'PENDING', 0, 3)`
      );

      const published = oldState === 'DRAFT';
      return res.json({ ok: true, dataset_id: datasetId, published, already_active: !published, queue_id: queueId });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.post('/api/portal/datasets/:datasetId/enqueue', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);

    console.log(`[ENQUEUE] Início da requisição - dataset_id=${datasetId}, user=${user}`);

    try {
      console.log(`[ENQUEUE] Consultando dataset_control para dataset_id=${datasetId}`);
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, execution_state, dataset_name, bronze_table, silver_table\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      
      if (!dsArr.length) {
        console.error(`[ENQUEUE] Dataset não encontrado - dataset_id=${datasetId}`);
        return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      }

      const ds = dsArr[0];
      const st = String(ds.execution_state || '').trim();
      console.log(`[ENQUEUE] Dataset encontrado - name=${ds.dataset_name}, state=${st}, bronze=${ds.bronze_table}, silver=${ds.silver_table}`);

      if (['PAUSED', 'DEPRECATED', 'BLOCKED_SCHEMA_CHANGE'].includes(st)) {
        console.warn(`[ENQUEUE] Dataset não elegível - dataset_id=${datasetId}, state=${st}`);
        return res.status(409).json({ ok: false, error: 'NOT_ELIGIBLE', message: `Dataset não elegível para enqueue (state=${st})` });
      }

      const queueId = crypto.randomUUID();
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      
      console.log(`[ENQUEUE] Inserindo na run_queue - queue_id=${queueId}, dataset_id=${datasetId}, status=PENDING`);

      const insertSql = 
        `INSERT INTO ${portalCfg.opsSchema}.run_queue\n` +
          `(queue_id, dataset_id, trigger_type, requested_by, requested_at, priority, status, attempt, max_retries)\n` +
          `VALUES (${sqlStringLiteral(queueId)}, ${sqlStringLiteral(datasetId)}, 'MANUAL', ${sqlStringLiteral(user)}, TIMESTAMP ${sqlStringLiteral(
            now
          )}, 100, 'PENDING', 0, 3)`;
      
      await db.query(insertSql);

      console.log(`[ENQUEUE] ✓ Registro inserido com sucesso na run_queue`);
      console.log(`[ENQUEUE] queue_id=${queueId}`);

      // Trigger on-demand execution via Databricks Jobs API
      const triggerResult = await triggerOrchestratorJob(datasetId);

      return res.json({ 
        ok: true, 
        queue_id: queueId,
        dataset_name: ds.dataset_name,
        trigger: triggerResult
          ? (triggerResult.error
              ? { triggered: false, error: triggerResult.error }
              : { triggered: true, databricks_run_id: triggerResult.run_id, number_in_job: triggerResult.number_in_job })
          : { triggered: false, reason: 'DATABRICKS_ORCHESTRATOR_JOB_ID não configurado' },
        message: triggerResult && !triggerResult.error
          ? `Job disparado imediatamente no Databricks (run_id=${triggerResult.run_id})`
          : 'Job enfileirado. Configure DATABRICKS_ORCHESTRATOR_JOB_ID para execução imediata.'
      });
    } catch (e) {
      console.error(`[ENQUEUE] Erro ao enfileirar - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== CONFIRM INCREMENTAL STRATEGY (after discovery) =====
  
  app.post('/api/portal/datasets/:datasetId/confirm-strategy', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);
    const { watermark_col, hash_exclude_cols, enable_reconciliation } = req.body;

    console.log(`[CONFIRM_STRATEGY] Início - dataset_id=${datasetId}, user=${user}`);

    try {
      // Fetch dataset with discovery status
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, discovery_status, discovery_suggestion, incremental_metadata\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!dsArr.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      const ds = dsArr[0];
      const discovery_status = String(ds.discovery_status || '').trim();
      const discovery_suggestion = String(ds.discovery_suggestion || '').trim();

      console.log(`[CONFIRM_STRATEGY] Dataset encontrado - discovery_status=${discovery_status}, suggestion=${discovery_suggestion}`);

      if (discovery_status !== 'PENDING_CONFIRMATION') {
        return res.status(409).json({
          ok: false,
          error: 'INVALID_STATUS',
          message: `Discovery status inválido: ${discovery_status}. Apenas PENDING_CONFIRMATION pode ser confirmado.`
        });
      }

      if (!discovery_suggestion) {
        return res.status(409).json({
          ok: false,
          error: 'NO_SUGGESTION',
          message: 'Nenhuma sugestão de estratégia disponível para confirmar.'
        });
      }

      // Parse existing metadata (may have pk, table_size_rows, etc)
      let metadata = {};
      try {
        if (ds.incremental_metadata) {
          metadata = JSON.parse(ds.incremental_metadata);
        }
      } catch (e) {
        console.error(`[CONFIRM_STRATEGY] Erro ao parsear incremental_metadata:`, e);
      }

      // User overrides (optional)
      if (watermark_col) metadata.watermark_col = watermark_col;
      if (Array.isArray(hash_exclude_cols)) metadata.hash_exclude_cols = hash_exclude_cols;

      const metadata_json = JSON.stringify(metadata);
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Activate incremental loading
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
          `SET \n` +
          `  enable_incremental = TRUE,\n` +
          `  incremental_strategy = ${sqlStringLiteral(discovery_suggestion)},\n` +
          `  incremental_metadata = ${sqlStringLiteral(metadata_json)},\n` +
          `  discovery_status = 'SUCCESS',\n` +
          `  strategy_locked = TRUE,\n` +
          `  enable_reconciliation = ${enable_reconciliation ? 'TRUE' : 'FALSE'},\n` +
          `  updated_at = TIMESTAMP ${sqlStringLiteral(now)},\n` +
          `  updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
      );

      console.log(`[CONFIRM_STRATEGY] ✓ Estratégia confirmada: ${discovery_suggestion}`);
      console.log(`[CONFIRM_STRATEGY] Metadata: ${metadata_json}`);

      return res.json({
        ok: true,
        confirmed: true,
        strategy: discovery_suggestion,
        metadata,
        enable_incremental: true
      });
    } catch (e) {
      console.error(`[CONFIRM_STRATEGY] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== REDISCOVER INCREMENTAL STRATEGY (force new discovery) =====
  
  app.post('/api/portal/datasets/:datasetId/rediscover', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);
    const force = req.query.force === 'true' || req.body.force === true;

    console.log(`[REDISCOVER] Início - dataset_id=${datasetId}, user=${user}, force=${force}`);

    try {
      // Fetch dataset with strategy_locked status
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, strategy_locked, incremental_strategy\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!dsArr.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      const ds = dsArr[0];
      const strategy_locked = Boolean(ds.strategy_locked);
      const current_strategy = String(ds.incremental_strategy || '').trim();

      console.log(`[REDISCOVER] Dataset encontrado - strategy_locked=${strategy_locked}, current_strategy=${current_strategy}`);

      if (strategy_locked && !force) {
        return res.status(409).json({
          ok: false,
          error: 'STRATEGY_LOCKED',
          message: `Estratégia bloqueada (${current_strategy}). Use force=true para forçar re-discovery ou desbloqueie antes.`
        });
      }

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Reset discovery status to PENDING and clear all incremental fields
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
          `SET \n` +
          `  discovery_status = 'PENDING',\n` +
          `  discovery_suggestion = NULL,\n` +
          `  enable_incremental = FALSE,\n` +
          `  incremental_strategy = NULL,\n` +
          `  incremental_metadata = NULL,\n` +
          `  strategy_locked = FALSE,\n` +
          `  bronze_mode = 'SNAPSHOT',\n` +
          `  updated_at = TIMESTAMP ${sqlStringLiteral(now)},\n` +
          `  updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
      );

      console.log(`[REDISCOVER] ✓ Discovery resetado para PENDING`);

      return res.json({
        ok: true,
        rediscovery_requested: true,
        discovery_status: 'PENDING',
        message: 'Discovery resetado. Execute o dataset novamente para forçar nova análise.'
      });
    } catch (e) {
      console.error(`[REDISCOVER] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  app.post('/api/portal/datasets/:datasetId/schema/:schemaVersion/approve', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const schemaVersion = parseIntStrict(req.params.schemaVersion);
    const user = getRequestUser(req);
    const comments = String(req.body?.comments || '').trim();

    if (schemaVersion == null) return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'schemaVersion inválido' });

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, execution_state\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!dsArr.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      const oldState = String(dsArr[0].execution_state || '').trim();

      const sv = await sqlQueryObjects(
        `SELECT status FROM ${portalCfg.ctrlSchema}.schema_versions\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND schema_version = ${schemaVersion}\n` +
          `LIMIT 1`
      );
      if (!sv.length) return res.status(404).json({ ok: false, error: 'SCHEMA_VERSION_NOT_FOUND' });
      if (String(sv[0].status || '').toUpperCase() !== 'PENDING') {
        return res.status(409).json({ ok: false, error: 'SCHEMA_VERSION_NOT_PENDING', message: 'Apenas schema_versions PENDING podem ser aprovadas.' });
      }

      const approvalId = crypto.randomUUID();
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.schema_approvals\n` +
          `(approval_id, dataset_id, schema_version, decision, decision_by, decision_at, comments)\n` +
          `VALUES (${sqlStringLiteral(approvalId)}, ${sqlStringLiteral(datasetId)}, ${schemaVersion}, 'APPROVED', ${sqlStringLiteral(
            user
          )}, TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(comments)})`
      );

      // apenas 1 ACTIVE
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.schema_versions\n` +
          `SET status = 'DEPRECATED'\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND status = 'ACTIVE' AND schema_version <> ${schemaVersion}`
      );

      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.schema_versions\n` +
          `SET status = 'ACTIVE'\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND schema_version = ${schemaVersion}`
      );

      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
          `SET current_schema_ver = ${schemaVersion}, execution_state = 'ACTIVE', updated_at = TIMESTAMP ${sqlStringLiteral(
            now
          )}, updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
      );

      const changeId = crypto.randomUUID();
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.dataset_state_changes\n` +
          `(change_id, dataset_id, old_state, new_state, reason, changed_at, changed_by)\n` +
          `VALUES (${sqlStringLiteral(changeId)}, ${sqlStringLiteral(datasetId)}, ${sqlStringLiteral(oldState)}, 'ACTIVE', 'SCHEMA_APPROVED', TIMESTAMP ${sqlStringLiteral(
            now
          )}, ${sqlStringLiteral(user)})`
      );

      return res.json({ ok: true, approved: true, schema_version: schemaVersion });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== DELETE DATASET =====

  app.delete('/api/portal/datasets/:datasetId', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const confirmName = String(req.body?.confirm_name || '').trim();
    const dropTables = Boolean(req.body?.drop_tables);
    const user = getRequestUser(req);

    console.log(`[DELETE] Início - dataset_id=${datasetId}, user=${user}, drop_tables=${dropTables}`);

    try {
      // Fetch dataset to validate existence and confirm_name
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, dataset_name, bronze_table, silver_table\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );
      if (!dsArr.length) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });

      const ds = dsArr[0];

      // Safety: user must type the exact dataset_name to confirm
      if (confirmName !== String(ds.dataset_name || '').trim()) {
        return res.status(400).json({
          ok: false,
          error: 'CONFIRMATION_MISMATCH',
          message: 'O nome do dataset não confere. A exclusão foi cancelada.',
          expected: ds.dataset_name,
        });
      }

      console.log(`[DELETE] ✓ Confirmação aceita para dataset: ${ds.dataset_name}`);

      // Reject if there are RUNNING/CLAIMED items in the queue
      const running = await sqlQueryObjects(
        `SELECT COUNT(*) AS cnt FROM ${portalCfg.opsSchema}.run_queue\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND status IN ('RUNNING', 'CLAIMED')`
      );
      if (Number(running[0]?.cnt || 0) > 0) {
        return res.status(409).json({
          ok: false,
          error: 'RUNNING_JOBS',
          message: 'Não é possível excluir: há jobs em execução para este dataset.',
        });
      }

      const deleted = {};

      // OPS tables
      const opsTables = [
        { table: 'batch_process_steps', col: 'dataset_id' },
        { table: 'batch_process_table_details', col: 'dataset_id' },
        { table: 'batch_process', col: 'dataset_id' },
        { table: 'run_queue', col: 'dataset_id' },
        { table: 'dataset_watermark', col: 'dataset_id' },
      ];

      for (const { table, col } of opsTables) {
        try {
          const r = await db.query(
            `DELETE FROM ${portalCfg.opsSchema}.${table} WHERE ${col} = ${sqlStringLiteral(datasetId)}`
          );
          deleted[table] = true;
          console.log(`[DELETE]   ✓ ${portalCfg.opsSchema}.${table}`);
        } catch (e) {
          console.warn(`[DELETE]   ⚠ ${portalCfg.opsSchema}.${table}: ${e.message}`);
          deleted[table] = false;
        }
      }

      // CTRL tables
      const ctrlTables = [
        { table: 'schema_approvals', col: 'dataset_id' },
        { table: 'schema_versions', col: 'dataset_id' },
        { table: 'dataset_state_changes', col: 'dataset_id' },
      ];

      for (const { table, col } of ctrlTables) {
        try {
          await db.query(
            `DELETE FROM ${portalCfg.ctrlSchema}.${table} WHERE ${col} = ${sqlStringLiteral(datasetId)}`
          );
          deleted[table] = true;
          console.log(`[DELETE]   ✓ ${portalCfg.ctrlSchema}.${table}`);
        } catch (e) {
          console.warn(`[DELETE]   ⚠ ${portalCfg.ctrlSchema}.${table}: ${e.message}`);
          deleted[table] = false;
        }
      }

      // Drop Delta tables if requested
      const droppedTables = [];
      if (dropTables) {
        for (const tbl of [ds.bronze_table, ds.silver_table]) {
          if (tbl && isSafeTableName(tbl)) {
            try {
              await db.query(`DROP TABLE IF EXISTS ${tbl}`);
              droppedTables.push(tbl);
              console.log(`[DELETE]   ✓ DROP TABLE ${tbl}`);
            } catch (e) {
              console.warn(`[DELETE]   ⚠ DROP TABLE ${tbl}: ${e.message}`);
            }
          }
        }
      }

      // Finally, delete the dataset_control record
      await db.query(
        `DELETE FROM ${portalCfg.ctrlSchema}.dataset_control WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
      );
      deleted['dataset_control'] = true;
      console.log(`[DELETE]   ✓ ${portalCfg.ctrlSchema}.dataset_control`);
      console.log(`[DELETE] ✓ Dataset ${ds.dataset_name} excluído com sucesso`);

      return res.json({
        ok: true,
        deleted_dataset: ds.dataset_name,
        deleted_tables: deleted,
        dropped_delta_tables: droppedTables,
      });
    } catch (e) {
      console.error(`[DELETE] Erro ao excluir - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== ADMIN: NAMING CONVENTIONS MANAGEMENT =====
  
  // List all naming conventions
  app.get('/api/portal/admin/naming-conventions', async (req, res) => {
    try {
      const rows = await sqlQueryObjects(
        `SELECT naming_version, bronze_pattern, silver_pattern, is_active, created_at, created_by, notes\n` +
          `FROM ${portalCfg.ctrlSchema}.naming_conventions\n` +
          `ORDER BY naming_version DESC`
      );
      return res.json({ ok: true, items: rows });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Create new naming convention
  app.post('/api/portal/admin/naming-conventions', async (req, res) => {
    const body = req.body || {};
    const bronzePattern = String(body.bronze_pattern || '').trim();
    const silverPattern = String(body.silver_pattern || '').trim();
    const notes = String(body.notes || '').trim();
    const user = getRequestUser(req);
    
    if (!bronzePattern || !silverPattern) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'bronze_pattern e silver_pattern são obrigatórios.' });
    }
    
    // Validar que padrões contém placeholders
    if (!bronzePattern.includes('{area}') || !bronzePattern.includes('{dataset}')) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'bronze_pattern deve conter {area} e {dataset}.' });
    }
    if (!silverPattern.includes('{area}') || !silverPattern.includes('{dataset}')) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'silver_pattern deve conter {area} e {dataset}.' });
    }
    
    try {
      // Get next version number
      const versionRows = await sqlQueryObjects(
        `SELECT COALESCE(MAX(naming_version), 0) + 1 as next_version FROM ${portalCfg.ctrlSchema}.naming_conventions`
      );
      const nextVersion = versionRows[0]?.next_version || 1;
      
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.naming_conventions\n` +
          `(naming_version, bronze_pattern, silver_pattern, is_active, created_at, created_by, notes)\n` +
          `VALUES (${nextVersion}, ${sqlStringLiteral(bronzePattern)}, ${sqlStringLiteral(silverPattern)}, false, TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(user)}, ${sqlStringLiteral(notes)})`
      );
      
      console.log(`[ADMIN] Nova naming convention criada: v${nextVersion} por ${user}`);
      
      return res.status(201).json({
        ok: true,
        naming_version: nextVersion,
        bronze_pattern: bronzePattern,
        silver_pattern: silverPattern,
        is_active: false,
      });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Activate naming convention (deactivates others)
  app.post('/api/portal/admin/naming-conventions/:version/activate', async (req, res) => {
    const version = parseIntStrict(req.params.version);
    const user = getRequestUser(req);
    
    if (version == null) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'versão inválida.' });
    }
    
    try {
      // Check if version exists
      const exists = await sqlQueryObjects(
        `SELECT naming_version FROM ${portalCfg.ctrlSchema}.naming_conventions WHERE naming_version = ${version}`
      );
      if (!exists.length) {
        return res.status(404).json({ ok: false, error: 'NOT_FOUND', message: `Naming convention v${version} não encontrada.` });
      }
      
      // Deactivate all
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.naming_conventions SET is_active = false`
      );
      
      // Activate the specified version
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.naming_conventions SET is_active = true WHERE naming_version = ${version}`
      );
      
      console.log(`[ADMIN] Naming convention v${version} ativada por ${user}`);
      
      return res.json({ ok: true, activated_version: version });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Update naming convention (only if not active)
  app.patch('/api/portal/admin/naming-conventions/:version', async (req, res) => {
    const version = parseIntStrict(req.params.version);
    const body = req.body || {};
    const user = getRequestUser(req);
    
    if (version == null) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'versão inválida.' });
    }
    
    try {
      // Check if exists and is not active
      const rows = await sqlQueryObjects(
        `SELECT is_active FROM ${portalCfg.ctrlSchema}.naming_conventions WHERE naming_version = ${version}`
      );
      if (!rows.length) {
        return res.status(404).json({ ok: false, error: 'NOT_FOUND', message: `Naming convention v${version} não encontrada.` });
      }
      if (rows[0].is_active) {
        return res.status(409).json({ ok: false, error: 'ACTIVE_CONVENTION', message: 'Não é possível editar convenção ativa. Desative primeiro.' });
      }
      
      const updates = [];
      if (body.bronze_pattern) {
        const bp = String(body.bronze_pattern).trim();
        if (!bp.includes('{area}') || !bp.includes('{dataset}')) {
          return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'bronze_pattern deve conter {area} e {dataset}.' });
        }
        updates.push(`bronze_pattern = ${sqlStringLiteral(bp)}`);
      }
      if (body.silver_pattern) {
        const sp = String(body.silver_pattern).trim();
        if (!sp.includes('{area}') || !sp.includes('{dataset}')) {
          return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'silver_pattern deve conter {area} e {dataset}.' });
        }
        updates.push(`silver_pattern = ${sqlStringLiteral(sp)}`);
      }
      if (body.notes !== undefined) {
        updates.push(`notes = ${sqlStringLiteral(String(body.notes).trim())}`);
      }
      
      if (updates.length === 0) {
        return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Nenhum campo para atualizar.' });
      }
      
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.naming_conventions SET ${updates.join(', ')} WHERE naming_version = ${version}`
      );
      
      console.log(`[ADMIN] Naming convention v${version} atualizada por ${user}`);
      
      return res.json({ ok: true, updated_version: version });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // Delete naming convention (only if not active)
  app.delete('/api/portal/admin/naming-conventions/:version', async (req, res) => {
    const version = parseIntStrict(req.params.version);
    const user = getRequestUser(req);
    
    if (version == null) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'versão inválida.' });
    }
    
    try {
      // Check if exists
      const exists = await sqlQueryObjects(
        `SELECT naming_version, is_active FROM ${portalCfg.ctrlSchema}.naming_conventions WHERE naming_version = ${version}`
      );
      
      if (!exists || exists.length === 0) {
        return res.status(404).json({ ok: false, error: 'NOT_FOUND', message: 'Convenção não encontrada.' });
      }
      
      // Cannot delete active convention
      if (exists[0].is_active) {
        return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'Não é possível excluir a convenção ativa. Ative outra convenção primeiro.' });
      }
      
      // Delete
      await db.query(
        `DELETE FROM ${portalCfg.ctrlSchema}.naming_conventions WHERE naming_version = ${version}`
      );
      
      console.log(`[ADMIN] Naming convention v${version} excluída por ${user}`);
      
      return res.json({ ok: true, message: 'Convenção excluída com sucesso.' });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // ===== BULK RENAME DATASETS =====
  
  app.post('/api/portal/datasets/bulk-rename', async (req, res) => {
    const body = req.body || {};
    const datasetIds = Array.isArray(body.dataset_ids) ? body.dataset_ids : [];
    const operation = String(body.operation || '').toUpperCase();
    const bronzeFrom = String(body.bronze_from || '').trim();
    const bronzeTo = String(body.bronze_to || '').trim();
    const silverFrom = String(body.silver_from || '').trim();
    const silverTo = String(body.silver_to || '').trim();
    const createSchemas = Boolean(body.create_schemas);
    const user = getRequestUser(req);
    
    if (datasetIds.length === 0) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'dataset_ids é obrigatório.' });
    }
    if (!['REPLACE_SCHEMA_PREFIX', 'REPLACE_CATALOG', 'REPLACE_FULL'].includes(operation)) {
      return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'operation inválida.' });
    }
    
    console.log(`[BULK_RENAME] Iniciando renomeação: ${datasetIds.length} datasets, operation=${operation}, user=${user}`);
    
    try {
      const results = [];
      const schemasToCreate = new Set();
      
      // Fetch all datasets
      const inClause = datasetIds.map(id => sqlStringLiteral(id)).join(', ');
      const datasets = await sqlQueryObjects(
        `SELECT dataset_id, dataset_name, bronze_table, silver_table FROM ${portalCfg.ctrlSchema}.dataset_control WHERE dataset_id IN (${inClause})`
      );
      
      if (datasets.length === 0) {
        return res.status(404).json({ ok: false, error: 'NOT_FOUND', message: 'Nenhum dataset encontrado.' });
      }
      
      // Process each dataset
      for (const ds of datasets) {
        const oldBronze = ds.bronze_table;
        const oldSilver = ds.silver_table;
        
        let newBronze = oldBronze;
        let newSilver = oldSilver;
        
        // Apply transformation based on operation
        if (operation === 'REPLACE_SCHEMA_PREFIX') {
          // catalog.schema_prefix.table → catalog.new_prefix.table
          if (bronzeFrom && bronzeTo && oldBronze) {
            const parts = oldBronze.split('.');
            if (parts.length === 3) {
              parts[1] = parts[1].replace(bronzeFrom, bronzeTo);
              newBronze = parts.join('.');
              const newSchema = parts[0] + '.' + parts[1];
              schemasToCreate.add(newSchema);
            }
          }
          if (silverFrom && silverTo && oldSilver) {
            const parts = oldSilver.split('.');
            if (parts.length === 3) {
              parts[1] = parts[1].replace(silverFrom, silverTo);
              newSilver = parts.join('.');
              const newSchema = parts[0] + '.' + parts[1];
              schemasToCreate.add(newSchema);
            }
          }
        } else if (operation === 'REPLACE_CATALOG') {
          // catalog.schema.table → new_catalog.schema.table
          if (bronzeFrom && bronzeTo && oldBronze) {
            const parts = oldBronze.split('.');
            if (parts.length === 3 && parts[0] === bronzeFrom) {
              parts[0] = bronzeTo;
              newBronze = parts.join('.');
              const newSchema = parts[0] + '.' + parts[1];
              schemasToCreate.add(newSchema);
            }
          }
          if (silverFrom && silverTo && oldSilver) {
            const parts = oldSilver.split('.');
            if (parts.length === 3 && parts[0] === silverFrom) {
              parts[0] = silverTo;
              newSilver = parts.join('.');
              const newSchema = parts[0] + '.' + parts[1];
              schemasToCreate.add(newSchema);
            }
          }
        } else if (operation === 'REPLACE_FULL') {
          // Substituíção completa
          if (bronzeTo) {
            newBronze = bronzeTo;
            const parts = bronzeTo.split('.');
            if (parts.length === 3) {
              const newSchema = parts[0] + '.' + parts[1];
              schemasToCreate.add(newSchema);
            }
          }
          if (silverTo) {
            newSilver = silverTo;
            const parts = silverTo.split('.');
            if (parts.length === 3) {
              const newSchema = parts[0] + '.' + parts[1];
              schemasToCreate.add(newSchema);
            }
          }
        }
        
        // Validar novos nomes
        if (!isSafeTableName(newBronze) || !isSafeTableName(newSilver)) {
          results.push({
            dataset_id: ds.dataset_id,
            dataset_name: ds.dataset_name,
            status: 'ERROR',
            message: 'Nome de tabela inválido após transformação',
          });
          continue;
        }
        
        // Verificar conflitos (se novo nome já existe)
        const conflict = await sqlQueryObjects(
          `SELECT dataset_id FROM ${portalCfg.ctrlSchema}.dataset_control ` +
            `WHERE (bronze_table = ${sqlStringLiteral(newBronze)} OR silver_table = ${sqlStringLiteral(newSilver)}) ` +
            `AND dataset_id != ${sqlStringLiteral(ds.dataset_id)} LIMIT 1`
        );
        if (conflict.length > 0) {
          results.push({
            dataset_id: ds.dataset_id,
            dataset_name: ds.dataset_name,
            status: 'CONFLICT',
            message: 'Novo nome já existe em outro dataset',
          });
          continue;
        }
        
        results.push({
          dataset_id: ds.dataset_id,
          dataset_name: ds.dataset_name,
          status: 'PREVIEW',
          old_bronze: oldBronze,
          new_bronze: newBronze,
          old_silver: oldSilver,
          new_silver: newSilver,
        });
      }
      
      // Se usuário pediu para criar schemas, criar agora
      const createdSchemas = [];
      if (createSchemas && schemasToCreate.size > 0) {
        for (const schema of schemasToCreate) {
          try {
            await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
            createdSchemas.push(schema);
            console.log(`[BULK_RENAME] Schema criado: ${schema}`);
          } catch (e) {
            console.warn(`[BULK_RENAME] Erro ao criar schema ${schema}: ${e.message}`);
          }
        }
      }
      
      // Se apenas preview (não tem confirmação), retornar
      if (!body.confirm) {
        return res.json({
          ok: true,
          preview: true,
          results,
          schemas_to_create: Array.from(schemasToCreate),
          created_schemas: createdSchemas,
        });
      }
      
      // Aplicar mudanças
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      let updated = 0;
      
      for (const result of results) {
        if (result.status !== 'PREVIEW') continue;
        
        try {
          // Update dataset_control
          await db.query(
            `UPDATE ${portalCfg.ctrlSchema}.dataset_control ` +
              `SET bronze_table = ${sqlStringLiteral(result.new_bronze)}, ` +
              `    silver_table = ${sqlStringLiteral(result.new_silver)}, ` +
              `    updated_at = TIMESTAMP ${sqlStringLiteral(now)}, ` +
              `    updated_by = ${sqlStringLiteral(user)} ` +
              `WHERE dataset_id = ${sqlStringLiteral(result.dataset_id)}`
          );
          
          // Log audit trail
          try {
            const auditId = `audit_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            const metadata = JSON.stringify({
              operation,
              bronze_from: bronzeFrom,
              bronze_to: bronzeTo,
              silver_from: silverFrom,
              silver_to: silverTo,
              create_schemas: createSchemas,
            });
            
            await db.query(
              `INSERT INTO ${portalCfg.ctrlSchema}.naming_audit_log ` +
                `(audit_id, dataset_id, operation_type, old_bronze_table, new_bronze_table, ` +
                `old_silver_table, new_silver_table, performed_by, performed_at, change_reason, metadata) ` +
                `VALUES (${sqlStringLiteral(auditId)}, ${sqlStringLiteral(result.dataset_id)}, ` +
                `${sqlStringLiteral('BULK_RENAME')}, ${sqlStringLiteral(result.old_bronze)}, ` +
                `${sqlStringLiteral(result.new_bronze)}, ${sqlStringLiteral(result.old_silver)}, ` +
                `${sqlStringLiteral(result.new_silver)}, ${sqlStringLiteral(user)}, ` +
                `TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral('Bulk rename operation')}, ` +
                `${sqlStringLiteral(metadata)})`
            );
          } catch (auditErr) {
            // Log error mas não falhar a operação
            console.warn(`[BULK_RENAME] Erro ao registrar audit log para ${result.dataset_id}: ${auditErr.message}`);
          }
          
          result.status = 'RENAMED';
          updated++;
        } catch (e) {
          result.status = 'ERROR';
          result.message = e.message;
        }
      }
      
      console.log(`[BULK_RENAME] Concluído: ${updated} datasets renomeados`);
      
      return res.json({
        ok: true,
        renamed: updated,
        results,
        created_schemas: createdSchemas,
      });
    } catch (e) {
      console.error(`[BULK_RENAME] Erro:`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });
  
  // ===== DATABRICKS JOBS API ENDPOINTS =====

  // List Databricks jobs
  app.get('/api/portal/databricks/jobs', async (req, res) => {
    if (!dbRest) return res.status(503).json({ ok: false, error: 'DATABRICKS_REST_NOT_CONFIGURED' });
    try {
      const name = req.query.name || '';
      const limit = Math.min(Number(req.query.limit) || 25, 100);
      const data = await dbRest.listJobs(limit, 0, name || undefined);
      return res.json({
        ok: true,
        jobs: (data.jobs || []).map(j => ({
          job_id: j.job_id,
          name: j.settings?.name,
          created_time: j.created_time,
          creator_user_name: j.creator_user_name,
          schedule: j.settings?.schedule || null,
          tags: j.settings?.tags || {},
        })),
        has_more: data.has_more || false,
        orchestrator_job_id: ORCHESTRATOR_JOB_ID,
      });
    } catch (e) {
      return res.status(502).json({ ok: false, error: 'DATABRICKS_API_ERROR', message: e.message });
    }
  });

  // Get job details
  app.get('/api/portal/databricks/jobs/:jobId', async (req, res) => {
    if (!dbRest) return res.status(503).json({ ok: false, error: 'DATABRICKS_REST_NOT_CONFIGURED' });
    try {
      const data = await dbRest.getJob(req.params.jobId);
      return res.json({ ok: true, job: data });
    } catch (e) {
      return res.status(502).json({ ok: false, error: 'DATABRICKS_API_ERROR', message: e.message });
    }
  });

  // List recent runs for a job
  app.get('/api/portal/databricks/jobs/:jobId/runs', async (req, res) => {
    if (!dbRest) return res.status(503).json({ ok: false, error: 'DATABRICKS_REST_NOT_CONFIGURED' });
    try {
      const limit = Math.min(Number(req.query.limit) || 10, 50);
      const data = await dbRest.listRuns(req.params.jobId, limit);
      return res.json({
        ok: true,
        runs: (data.runs || []).map(r => ({
          run_id: r.run_id,
          run_name: r.run_name,
          state: r.state,
          start_time: r.start_time,
          end_time: r.end_time,
          setup_duration: r.setup_duration,
          execution_duration: r.execution_duration,
          cleanup_duration: r.cleanup_duration,
          trigger: r.trigger,
          run_page_url: r.run_page_url,
          overriding_parameters: r.overriding_parameters,
        })),
      });
    } catch (e) {
      return res.status(502).json({ ok: false, error: 'DATABRICKS_API_ERROR', message: e.message });
    }
  });

  // Trigger a job run (manual trigger from UI)
  app.post('/api/portal/databricks/jobs/:jobId/run-now', async (req, res) => {
    if (!dbRest) return res.status(503).json({ ok: false, error: 'DATABRICKS_REST_NOT_CONFIGURED' });
    try {
      const params = req.body?.job_parameters || req.body?.notebook_params || {};
      const data = await dbRest.runNow(req.params.jobId, params);
      return res.json({ ok: true, run_id: data.run_id, number_in_job: data.number_in_job });
    } catch (e) {
      return res.status(502).json({ ok: false, error: 'DATABRICKS_API_ERROR', message: e.message });
    }
  });

  // Get run status
  app.get('/api/portal/databricks/runs/:runId', async (req, res) => {
    if (!dbRest) return res.status(503).json({ ok: false, error: 'DATABRICKS_REST_NOT_CONFIGURED' });
    try {
      const data = await dbRest.getRun(req.params.runId);
      return res.json({
        ok: true,
        run: {
          run_id: data.run_id,
          run_name: data.run_name,
          state: data.state,
          start_time: data.start_time,
          end_time: data.end_time,
          execution_duration: data.execution_duration,
          run_page_url: data.run_page_url,
          overriding_parameters: data.overriding_parameters,
          tasks: (data.tasks || []).map(t => ({
            task_key: t.task_key,
            state: t.state,
            start_time: t.start_time,
            end_time: t.end_time,
            execution_duration: t.execution_duration,
          })),
        },
      });
    } catch (e) {
      return res.status(502).json({ ok: false, error: 'DATABRICKS_API_ERROR', message: e.message });
    }
  });

  // Cancel a run
  app.post('/api/portal/databricks/runs/:runId/cancel', async (req, res) => {
    if (!dbRest) return res.status(503).json({ ok: false, error: 'DATABRICKS_REST_NOT_CONFIGURED' });
    try {
      await dbRest.cancelRun(req.params.runId);
      return res.json({ ok: true, cancelled: true });
    } catch (e) {
      return res.status(502).json({ ok: false, error: 'DATABRICKS_API_ERROR', message: e.message });
    }
  });

  // Orchestrator config status
  app.get('/api/portal/databricks/config', (req, res) => {
    return res.json({
      ok: true,
      rest_api_configured: Boolean(dbRest),
      orchestrator_job_id: ORCHESTRATOR_JOB_ID,
      host: dbHost ? `${dbHost.substring(0, 30)}...` : null,
    });
  });

  app.post('/api/portal/datasets/:datasetId/schema/:schemaVersion/reject', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const schemaVersion = parseIntStrict(req.params.schemaVersion);
    const user = getRequestUser(req);
    const comments = String(req.body?.comments || '').trim();

    if (schemaVersion == null) return res.status(400).json({ ok: false, error: 'VALIDATION_ERROR', message: 'schemaVersion inválido' });

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      const sv = await sqlQueryObjects(
        `SELECT status FROM ${portalCfg.ctrlSchema}.schema_versions\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND schema_version = ${schemaVersion}\n` +
          `LIMIT 1`
      );
      if (!sv.length) return res.status(404).json({ ok: false, error: 'SCHEMA_VERSION_NOT_FOUND' });
      if (String(sv[0].status || '').toUpperCase() !== 'PENDING') {
        return res.status(409).json({ ok: false, error: 'SCHEMA_VERSION_NOT_PENDING', message: 'Apenas schema_versions PENDING podem ser rejeitadas.' });
      }

      const approvalId = crypto.randomUUID();
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.schema_approvals\n` +
          `(approval_id, dataset_id, schema_version, decision, decision_by, decision_at, comments)\n` +
          `VALUES (${sqlStringLiteral(approvalId)}, ${sqlStringLiteral(datasetId)}, ${schemaVersion}, 'REJECTED', ${sqlStringLiteral(
            user
          )}, TIMESTAMP ${sqlStringLiteral(now)}, ${sqlStringLiteral(comments)})`
      );

      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.schema_versions\n` +
          `SET status = 'REJECTED'\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} AND schema_version = ${schemaVersion}`
      );

      // Mantém dataset bloqueado (ou estado atual)
      return res.json({ ok: true, rejected: true, schema_version: schemaVersion });
    } catch (e) {
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===========================
  // INCREMENTAL LOADING ENDPOINTS
  // ===========================

  // Confirm incremental strategy (after discovery)
  app.post('/api/portal/datasets/:datasetId/confirm-strategy', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);

    console.log(`[CONFIRM_STRATEGY] Início - dataset_id=${datasetId}, user=${user}`);

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Load dataset config
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, discovery_status, discovery_suggestion, incremental_metadata, strategy_locked\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );

      if (!dsArr.length) {
        console.error(`[CONFIRM_STRATEGY] Dataset não encontrado - dataset_id=${datasetId}`);
        return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      }

      const ds = dsArr[0];
      const discoveryStatus = String(ds.discovery_status || '').trim().toUpperCase();
      const suggestion = String(ds.discovery_suggestion || '').trim();
      const locked = Boolean(ds.strategy_locked);

      console.log(`[CONFIRM_STRATEGY] Status atual - discovery_status=${discoveryStatus}, suggestion=${suggestion}, locked=${locked}`);

      // Validation: must have PENDING_CONFIRMATION
      if (discoveryStatus !== 'PENDING_CONFIRMATION') {
        console.warn(`[CONFIRM_STRATEGY] Discovery não está pendente - status=${discoveryStatus}`);
        return res.status(409).json({
          ok: false,
          error: 'INVALID_STATE',
          message: `Discovery deve estar em PENDING_CONFIRMATION (atual: ${discoveryStatus})`,
        });
      }

      if (!suggestion) {
        console.error(`[CONFIRM_STRATEGY] Sem sugestão de estratégia`);
        return res.status(400).json({ ok: false, error: 'NO_SUGGESTION', message: 'Nenhuma estratégia sugerida pelo discovery' });
      }

      // Allow user overrides from request body
      const strategy = String(req.body?.strategy || suggestion).trim();
      const bronzeMode = String(req.body?.bronze_mode || 'CURRENT').trim();
      const metadata = req.body?.metadata || ds.incremental_metadata;

      // Validate strategy
      const validStrategies = ['WATERMARK', 'HASH_MERGE', 'SNAPSHOT', 'APPEND_LOG', 'REQUIRES_CDC'];
      if (!validStrategies.includes(strategy)) {
        return res.status(400).json({
          ok: false,
          error: 'INVALID_STRATEGY',
          message: `Estratégia inválida. Deve ser uma de: ${validStrategies.join(', ')}`,
        });
      }

      // GUARDRAIL: CURRENT mode requires PK
      if (bronzeMode === 'CURRENT') {
        let metadataParsed = {};
        try {
          metadataParsed = typeof metadata === 'string' ? JSON.parse(metadata) : metadata || {};
        } catch {}
        const pkCols = metadataParsed.pk || [];
        const pkConfidence = metadataParsed.pk_confidence || 0;
        const pkSource = metadataParsed.pk_source || '';

        if (pkCols.length === 0) {
          console.warn(`[CONFIRM_STRATEGY] BLOCKED: CURRENT mode without PK`);
          return res.status(400).json({
            ok: false,
            error: 'CURRENT_REQUIRES_PK',
            message: 'Modo CURRENT requer PK definida. Configure PK manualmente ou use SNAPSHOT.',
          });
        }
        if (pkSource === 'CANDIDATE_DISCOVERY' && pkConfidence < 0.90) {
          console.warn(`[CONFIRM_STRATEGY] BLOCKED: PK candidate confidence too low (${pkConfidence})`);
          return res.status(400).json({
            ok: false,
            error: 'PK_LOW_CONFIDENCE',
            message: `PK candidata com confian\u00e7a baixa (${(pkConfidence * 100).toFixed(0)}%). Valide a unicidade antes de confirmar CURRENT.`,
          });
        }
      }

      console.log(`[CONFIRM_STRATEGY] Confirmando estrat\u00e9gia - strategy=${strategy}, bronze_mode=${bronzeMode}`);

      // Activate incremental loading
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
          `SET enable_incremental = TRUE,\n` +
          `    incremental_strategy = ${sqlStringLiteral(strategy)},\n` +
          `    bronze_mode = ${sqlStringLiteral(bronzeMode)},\n` +
          `    strategy_locked = TRUE,\n` +
          `    discovery_status = 'SUCCESS',\n` +
          `    incremental_metadata = ${metadata ? sqlStringLiteral(JSON.stringify(metadata)) : 'incremental_metadata'},\n` +
          `    updated_at = TIMESTAMP ${sqlStringLiteral(now)},\n` +
          `    updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
      );

      console.log(`[CONFIRM_STRATEGY] ✓ Estratégia confirmada e ativada`);

      return res.json({
        ok: true,
        confirmed: true,
        strategy,
        bronze_mode: bronzeMode,
        enable_incremental: true,
        message: `Carga incremental ativada com estratégia ${strategy}`,
      });
    } catch (e) {
      console.error(`[CONFIRM_STRATEGY] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Update incremental load parameters
  app.patch('/api/portal/datasets/:datasetId/incremental-config', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);

    console.log(`[UPDATE_INCREMENTAL_CONFIG] Início - dataset_id=${datasetId}, user=${user}`);

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Load dataset config
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, enable_incremental, incremental_strategy, bronze_mode, incremental_metadata, override_watermark_value\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );

      if (!dsArr.length) {
        console.error(`[UPDATE_INCREMENTAL_CONFIG] Dataset não encontrado - dataset_id=${datasetId}`);
        return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      }

      const ds = dsArr[0];
      const updates = [];

      // Allow updating load type (full vs incremental)
      if (req.body.enable_incremental !== undefined) {
        const enableIncremental = Boolean(req.body.enable_incremental);
        updates.push(`enable_incremental = ${enableIncremental}`);
        console.log(`[UPDATE_INCREMENTAL_CONFIG] enable_incremental = ${enableIncremental}`);
      }

      // Allow updating bronze mode (SNAPSHOT, CURRENT, APPEND_LOG)
      if (req.body.bronze_mode) {
        const bronzeMode = String(req.body.bronze_mode).trim().toUpperCase();
        const validModes = ['SNAPSHOT', 'CURRENT', 'APPEND_LOG'];
        if (!validModes.includes(bronzeMode)) {
          return res.status(400).json({
            ok: false,
            error: 'INVALID_BRONZE_MODE',
            message: `bronze_mode inválido. Deve ser: ${validModes.join(', ')}`,
          });
        }
        updates.push(`bronze_mode = ${sqlStringLiteral(bronzeMode)}`);
        console.log(`[UPDATE_INCREMENTAL_CONFIG] bronze_mode = ${bronzeMode}`);
      }

      // Allow updating incremental strategy
      if (req.body.incremental_strategy) {
        const strategy = String(req.body.incremental_strategy).trim().toUpperCase();
        const validStrategies = ['WATERMARK', 'HASH_MERGE', 'SNAPSHOT', 'APPEND_LOG', 'REQUIRES_CDC'];
        if (!validStrategies.includes(strategy)) {
          return res.status(400).json({
            ok: false,
            error: 'INVALID_STRATEGY',
            message: `incremental_strategy inválido. Deve ser: ${validStrategies.join(', ')}`,
          });
        }
        updates.push(`incremental_strategy = ${sqlStringLiteral(strategy)}`);
        console.log(`[UPDATE_INCREMENTAL_CONFIG] incremental_strategy = ${strategy}`);
      }

      // Allow watermark override for historical backfill (e.g., "2024-01-01 00:00:00")
      if (req.body.override_watermark_value !== undefined) {
        const watermarkValue = req.body.override_watermark_value
          ? sqlStringLiteral(String(req.body.override_watermark_value).trim())
          : 'NULL';
        updates.push(`override_watermark_value = ${watermarkValue}`);
        console.log(`[UPDATE_INCREMENTAL_CONFIG] override_watermark_value = ${watermarkValue}`);
      }

      // Allow updating incremental metadata (lookback days, etc.)
      if (req.body.incremental_metadata) {
        let metadata = req.body.incremental_metadata;
        if (typeof metadata !== 'string') {
          metadata = JSON.stringify(metadata);
        }
        updates.push(`incremental_metadata = ${sqlStringLiteral(metadata)}`);
        console.log(`[UPDATE_INCREMENTAL_CONFIG] incremental_metadata updated`);
      }

      if (updates.length === 0) {
        return res.status(400).json({
          ok: false,
          error: 'NO_UPDATES',
          message: 'Nenhuma atualiza\u00e7\u00e3o fornecida',
        });
      }

      // GUARDRAIL: CURRENT mode requires PK
      const newBronzeMode = req.body.bronze_mode
        ? String(req.body.bronze_mode).trim().toUpperCase()
        : String(ds.bronze_mode || '').trim().toUpperCase();
      const newEnableIncremental = req.body.enable_incremental !== undefined
        ? Boolean(req.body.enable_incremental)
        : Boolean(ds.enable_incremental);

      if (newEnableIncremental && newBronzeMode === 'CURRENT') {
        let meta = {};
        try {
          if (req.body.incremental_metadata) {
            meta = typeof req.body.incremental_metadata === 'string'
              ? JSON.parse(req.body.incremental_metadata)
              : req.body.incremental_metadata;
          } else if (ds.incremental_metadata) {
            meta = JSON.parse(ds.incremental_metadata);
          }
        } catch {}
        const pkCols = meta.pk || [];
        if (pkCols.length === 0) {
          console.warn(`[UPDATE_INCREMENTAL_CONFIG] BLOCKED: CURRENT mode without PK`);
          return res.status(400).json({
            ok: false,
            error: 'CURRENT_REQUIRES_PK',
            message: 'Modo CURRENT requer PK definida. Configure PK ou use SNAPSHOT.',
          });
        }
      }

      // Sync discovery_status when enabling incremental with a valid config
      // Prevents inconsistency where strategy_locked=TRUE but discovery_status=PENDING_CONFIRMATION
      if (newEnableIncremental) {
        updates.push(`discovery_status = 'SUCCESS'`);
        console.log(`[UPDATE_INCREMENTAL_CONFIG] discovery_status synced to SUCCESS`);
      }

      // Execute UPDATE
      updates.push(`updated_at = TIMESTAMP ${sqlStringLiteral(now)}`);
      updates.push(`updated_by = ${sqlStringLiteral(user)}`);

      const updateSql =
        `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
        `SET ${updates.join(',\n    ')}\n` +
        `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`;

      console.log(`[UPDATE_INCREMENTAL_CONFIG] Executando SQL:\n${updateSql}`);
      await db.query(updateSql);

      console.log(`[UPDATE_INCREMENTAL_CONFIG] ✓ Configurações atualizadas`);

      // Return updated config
      const updatedDs = await sqlQueryObjects(
        `SELECT dataset_id, enable_incremental, incremental_strategy, bronze_mode, incremental_metadata, override_watermark_value\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );

      return res.json({
        ok: true,
        updated: true,
        config: updatedDs[0],
      });
    } catch (e) {
      console.error(`[UPDATE_INCREMENTAL_CONFIG] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Get columns with preview and smart watermark column suggestions
  app.get('/api/portal/datasets/:datasetId/columns-preview', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const limit = clampLimit(req.query.limit, 5);

    console.log(`[COLUMNS_PREVIEW] Início - dataset_id=${datasetId}`);

    try {
      // Load dataset config
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, bronze_table, incremental_metadata, source_type, dataset_name\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );

      if (!dsArr.length) {
        return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      }

      const ds = dsArr[0];
      let bronzeTable = String(ds.bronze_table || '').trim();
      
      if (!bronzeTable) {
        return res.status(400).json({ 
          ok: false, 
          error: 'MISSING_BRONZE_TABLE',
          message: 'Tabela Bronze não configurada para este dataset'
        });
      }

      // Support schema.table format (prefix with catalog)
      const parts = bronzeTable.split('.');
      if (parts.length === 2) {
        bronzeTable = `${portalCfg.catalog}.${bronzeTable}`;
      }

      if (!isSafeTableName(bronzeTable)) {
        return res.status(400).json({ 
          ok: false, 
          error: 'INVALID_TABLE_NAME',
          message: 'Nome da tabela Bronze inválido' 
        });
      }

      console.log(`[COLUMNS_PREVIEW] Buscando schema da tabela: ${bronzeTable}`);

      // Get table schema using DESCRIBE
      const schemaRows = await db.query(`DESCRIBE TABLE ${bronzeTable}`);
      const columns = schemaRows.rows
        .filter(r => r[0] && r[0] !== '' && !r[0].startsWith('#')) // Filter empty rows and comments
        .map(r => ({
          name: r[0],
          type: r[1],
          comment: r[2] || null
        }));

      console.log(`[COLUMNS_PREVIEW] ${columns.length} colunas encontradas`);

      // Identify date/timestamp columns and suggest watermark column
      const dateColumns = columns.filter(c => {
        const type = String(c.type || '').toLowerCase();
        return type.includes('timestamp') || type.includes('date');
      });

      // Smart watermark column suggestion based on column names and types
      const watermarkKeywords = [
        'updated_at', 'update_at', 'dt_update', 'dt_updated',
        'modified_at', 'modify_at', 'dt_modified', 'dt_modify',
        'changed_at', 'change_at', 'dt_changed', 'dt_change',
        'created_at', 'create_at', 'dt_created', 'dt_create',
        'inserted_at', 'insert_at', 'dt_inserted', 'dt_insert',
        'data_atualizacao', 'dt_atualizacao', 'data_alteracao',
        'data_modificacao', 'data_criacao', 'data_inclusao',
        // Oracle-common patterns
        'datalt', 'dt_ultima_alteracao', 'data_ultima_alteracao', 'dt_ult_alteracao',
        'data_ult_atualizacao', 'dt_ultima_atualizacao', 'dt_ult_atualizacao',
        'data_ultima_modificacao', 'last_update_date', 'last_modified_date', 'last_updated_date',
        'update_date', 'modify_date', 'dt_cadastro', 'data_cadastro',
        'dt_inclusao', 'dt_registro', 'data_registro',
        'dt_processamento', 'data_processamento',
        'timestamp', 'dt_timestamp', 'last_modified', 'last_updated'
      ];

      let suggestedColumn = null;
      let suggestionReason = null;

      // Priority 1: Check if incremental_metadata already has watermark_column
      try {
        const metadata = ds.incremental_metadata ? JSON.parse(ds.incremental_metadata) : {};
        if (metadata.watermark_column && columns.find(c => c.name === metadata.watermark_column)) {
          suggestedColumn = metadata.watermark_column;
          suggestionReason = 'already_configured';
          console.log(`[COLUMNS_PREVIEW] Coluna já configurada: ${suggestedColumn}`);
        }
      } catch {}

      // Priority 2: Look for common watermark column names
      if (!suggestedColumn && dateColumns.length > 0) {
        for (const keyword of watermarkKeywords) {
          const match = dateColumns.find(c => 
            c.name.toLowerCase() === keyword.toLowerCase()
          );
          if (match) {
            suggestedColumn = match.name;
            suggestionReason = 'name_pattern';
            console.log(`[COLUMNS_PREVIEW] Coluna sugerida por nome: ${suggestedColumn}`);
            break;
          }
        }
      }

      // Priority 3: If no exact match, use first date column with "update" or "modified" in name
      if (!suggestedColumn && dateColumns.length > 0) {
        const partialMatch = dateColumns.find(c => {
          const name = c.name.toLowerCase();
          return name.includes('update') || name.includes('modif') || name.includes('alter') 
            || name.includes('ultima') || name.includes('ult_') || name.includes('atualizacao');
        });
        if (partialMatch) {
          suggestedColumn = partialMatch.name;
          suggestionReason = 'partial_match';
          console.log(`[COLUMNS_PREVIEW] Coluna sugerida por correspondência parcial: ${suggestedColumn}`);
        }
      }

      // Priority 4: Default to first date/timestamp column
      if (!suggestedColumn && dateColumns.length > 0) {
        suggestedColumn = dateColumns[0].name;
        suggestionReason = 'first_date_column';
        console.log(`[COLUMNS_PREVIEW] Usando primeira coluna de data: ${suggestedColumn}`);
      }

      // Get sample data using explicit column names (resilient to schema mismatches)
      console.log(`[COLUMNS_PREVIEW] Buscando ${limit} linhas de preview`);

      let sampleRows = [];
      let previewError = null;
      let previewColumns = [];

      if (columns.length > 0) {
        try {
          // Filter out technical columns that may not exist (_watermark_value only exists after incremental loads)
          previewColumns = columns.filter(c => c.name !== '_watermark_value');
          
          const colNames = previewColumns
            .map(c => `\`${String(c.name).replace(/`/g, '``')}\``)
            .join(', ');

          const previewQuery = `SELECT ${colNames} FROM ${bronzeTable} LIMIT ${limit}`;
          const previewResult = await db.query(previewQuery);
          sampleRows = previewResult.rows;
        } catch (previewErr) {
          // Example: [INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND] Could not find column ...
          console.warn(`[COLUMNS_PREVIEW] ⚠️ Falha ao buscar sample data: ${previewErr.message}`);
          previewError = previewErr.message;
        }
      }

      // Build preview data for each column
      const columnsWithPreview = columns.map(col => {
        // Find the column index in previewColumns (may not match if column was filtered)
        const previewIdx = previewColumns.findIndex(pc => pc.name === col.name);
        const sampleValues = previewIdx >= 0 
          ? sampleRows.map(row => row[previewIdx]).filter(v => v !== null && v !== undefined)
          : [];
        
        return {
          ...col,
          sample_values: sampleValues.slice(0, 3), // Show max 3 samples
          is_date: dateColumns.some(dc => dc.name === col.name),
          is_suggested: col.name === suggestedColumn
        };
      });

      // ===== PK DETECTION =====
      let suggestedPkColumns = [];
      let pkSuggestionReason = null;

      // Priority 1: Check if incremental_metadata already has PK configured
      try {
        const metadata = ds.incremental_metadata ? JSON.parse(ds.incremental_metadata) : {};
        const existingPk = metadata.pk || [];
        if (existingPk.length > 0 && existingPk.every(pk => columns.find(c => c.name === pk))) {
          suggestedPkColumns = existingPk;
          pkSuggestionReason = 'already_configured';
          console.log(`[COLUMNS_PREVIEW] PK já configurada: ${suggestedPkColumns.join(', ')}`);
        }
      } catch {}

      // Priority 2: Detect from source PostgreSQL (Supabase only)
      const sourceType = String(ds.source_type || '').trim().toUpperCase();
      if (suggestedPkColumns.length === 0 && sourceType === 'SUPABASE' && supabaseClient) {
        try {
          const dsName = String(ds.dataset_name || '').trim();
          let sSchema = 'public';
          let sTable = dsName;
          if (dsName.includes('_')) {
            const firstPart = dsName.split('_', 1)[0];
            if (['public', 'auth', 'storage', 'extensions'].includes(firstPart.toLowerCase())) {
              sSchema = firstPart;
              sTable = dsName.split('_').slice(1).join('_');
            }
          }
          console.log(`[COLUMNS_PREVIEW] Detectando PK da origem: ${sSchema}.${sTable}`);
          const detectedPk = await supabaseClient.getPkColumns(sTable, sSchema);
          if (detectedPk.length > 0) {
            suggestedPkColumns = detectedPk.filter(pk => columns.find(c => c.name === pk));
            if (suggestedPkColumns.length > 0) {
              pkSuggestionReason = 'source_detected';
              console.log(`[COLUMNS_PREVIEW] PK detectada da origem: ${suggestedPkColumns.join(', ')}`);
            }
          }
        } catch (pkErr) {
          console.warn(`[COLUMNS_PREVIEW] ⚠️ Erro ao detectar PK da origem: ${pkErr.message}`);
        }
      }

      // Priority 3: Fallback — look for column named 'id'
      if (suggestedPkColumns.length === 0) {
        const idCol = columns.find(c => c.name.toLowerCase() === 'id');
        if (idCol) {
          suggestedPkColumns = [idCol.name];
          pkSuggestionReason = 'id_column_fallback';
          console.log(`[COLUMNS_PREVIEW] PK fallback: coluna 'id' encontrada`);
        }
      }

      // Mark columns with is_pk flag
      const columnsWithPk = columnsWithPreview.map(col => ({
        ...col,
        is_pk: suggestedPkColumns.includes(col.name),
      }));

      console.log(`[COLUMNS_PREVIEW] ✓ Preview gerado com sucesso`);

      return res.json({
        ok: true,
        dataset_id: datasetId,
        bronze_table: bronzeTable,
        columns: columnsWithPk,
        date_columns: dateColumns.map(c => c.name),
        suggested_watermark_column: suggestedColumn,
        suggestion_reason: suggestionReason,
        suggested_pk_columns: suggestedPkColumns,
        pk_suggestion_reason: pkSuggestionReason,
        sample_row_count: sampleRows.length,
        ...(previewError ? { preview_warning: 'Não foi possível carregar sample data. As colunas foram obtidas via DESCRIBE.' } : {}),
      });
    } catch (e) {
      console.error(`[COLUMNS_PREVIEW] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Force re-discovery (admin action)
  app.post('/api/portal/datasets/:datasetId/rediscover', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const user = getRequestUser(req);

    console.log(`[REDISCOVER] Início - dataset_id=${datasetId}, user=${user}`);

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Load dataset config
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, strategy_locked, incremental_strategy\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );

      if (!dsArr.length) {
        console.error(`[REDISCOVER] Dataset não encontrado - dataset_id=${datasetId}`);
        return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      }

      const ds = dsArr[0];
      const locked = Boolean(ds.strategy_locked);
      const currentStrategy = String(ds.incremental_strategy || '').trim();

      console.log(`[REDISCOVER] Status atual - locked=${locked}, strategy=${currentStrategy}`);

      // Validation: strategy must not be locked (safety check)
      if (locked && !req.body?.force) {
        console.warn(`[REDISCOVER] Estratégia está locked - requer force=true`);
        return res.status(409).json({
          ok: false,
          error: 'STRATEGY_LOCKED',
          message: 'Estratégia está confirmada e locked. Use force=true para forçar re-discovery.',
        });
      }

      console.log(`[REDISCOVER] Resetando discovery status...`);

      // Reset discovery to PENDING (will trigger automatic discovery on next run)
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.dataset_control\n` +
          `SET discovery_status = 'PENDING',\n` +
          `    discovery_suggestion = NULL,\n` +
          `    last_discovery_at = NULL,\n` +
          `    strategy_locked = FALSE,\n` +
          `    enable_incremental = FALSE,\n` +
          `    updated_at = TIMESTAMP ${sqlStringLiteral(now)},\n` +
          `    updated_by = ${sqlStringLiteral(user)}\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)}`
      );

      console.log(`[REDISCOVER] ✓ Discovery resetado para PENDING`);

      return res.json({
        ok: true,
        rediscovered: true,
        discovery_status: 'PENDING',
        message: 'Discovery foi resetado. Execute o dataset novamente para rodar discovery automático.',
      });
    } catch (e) {
      console.error(`[REDISCOVER] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // Validate PK uniqueness on Bronze table
  app.post('/api/portal/datasets/:datasetId/validate-pk', async (req, res) => {
    const datasetId = String(req.params.datasetId || '').trim();
    const pkColumnsRaw = req.body?.pk_columns;
    const scope = String(req.body?.scope || 'bronze').trim().toLowerCase();

    console.log(`[VALIDATE_PK] Início - dataset_id=${datasetId}, scope=${scope}`);

    if (!Array.isArray(pkColumnsRaw) || pkColumnsRaw.length === 0) {
      return res.status(400).json({
        ok: false,
        error: 'MISSING_PK_COLUMNS',
        message: 'Informe pk_columns (array de nomes de colunas)',
      });
    }

    const pkColumns = pkColumnsRaw.map(c => String(c).trim()).filter(Boolean);
    if (pkColumns.length === 0 || pkColumns.length > 10) {
      return res.status(400).json({
        ok: false,
        error: 'INVALID_PK_COLUMNS',
        message: 'pk_columns deve ter entre 1 e 10 colunas',
      });
    }

    // Validate column names are safe identifiers
    if (!pkColumns.every(c => isSafeIdentifier(c))) {
      return res.status(400).json({
        ok: false,
        error: 'INVALID_COLUMN_NAME',
        message: 'Nomes de coluna contêm caracteres inválidos',
      });
    }

    if (scope === 'source') {
      return res.status(501).json({
        ok: false,
        error: 'SOURCE_VALIDATION_NOT_AVAILABLE',
        message: 'Validação direta na origem Oracle não está disponível via Portal. Use scope=bronze.',
      });
    }

    try {
      // Load dataset to get bronze_table
      const dsArr = await sqlQueryObjects(
        `SELECT dataset_id, bronze_table\n` +
          `FROM ${portalCfg.ctrlSchema}.dataset_control\n` +
          `WHERE dataset_id = ${sqlStringLiteral(datasetId)} LIMIT 1`
      );

      if (!dsArr.length) {
        return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
      }

      let bronzeTable = String(dsArr[0].bronze_table || '').trim();
      if (!bronzeTable) {
        return res.status(400).json({
          ok: false,
          error: 'MISSING_BRONZE_TABLE',
          message: 'Tabela Bronze não configurada',
        });
      }

      const parts = bronzeTable.split('.');
      if (parts.length === 2) {
        bronzeTable = `${portalCfg.catalog}.${bronzeTable}`;
      }
      if (!isSafeTableName(bronzeTable)) {
        return res.status(400).json({
          ok: false,
          error: 'INVALID_TABLE_NAME',
          message: 'Nome da tabela Bronze inválido',
        });
      }

      // Build safe column references
      const colRefs = pkColumns.map(c => `\`${c.replace(/`/g, '``')}\``).join(', ');
      const concatExpr = pkColumns.length === 1
        ? `CAST(${colRefs} AS STRING)`
        : `CONCAT_WS('\u241F', ${pkColumns.map(c => `COALESCE(CAST(\`${c.replace(/`/g, '``')}\` AS STRING), '\u2205')`).join(', ')})`;

      console.log(`[VALIDATE_PK] Validando unicidade em ${bronzeTable}: [${pkColumns.join(', ')}]`);

      // Count total rows and distinct PK combinations
      const countQuery = `SELECT COUNT(*) AS total_rows, COUNT(DISTINCT ${concatExpr}) AS distinct_rows FROM ${bronzeTable}`;
      const countResult = await db.query(countQuery);
      const totalRows = Number(countResult.rows[0]?.[0] || 0);
      const distinctRows = Number(countResult.rows[0]?.[1] || 0);
      const duplicateCount = totalRows - distinctRows;
      const isUnique = duplicateCount === 0;

      console.log(`[VALIDATE_PK] total=${totalRows}, distinct=${distinctRows}, duplicates=${duplicateCount}, unique=${isUnique}`);

      // If not unique, get sample duplicates (top 5)
      let sampleDuplicates = [];
      if (!isUnique) {
        try {
          const dupQuery =
            `SELECT ${colRefs}, COUNT(*) AS dup_count ` +
            `FROM ${bronzeTable} ` +
            `GROUP BY ${colRefs} ` +
            `HAVING COUNT(*) > 1 ` +
            `ORDER BY COUNT(*) DESC ` +
            `LIMIT 5`;
          const dupResult = await db.query(dupQuery);
          sampleDuplicates = dupResult.rows.map(row => {
            const obj = {};
            pkColumns.forEach((col, idx) => { obj[col] = row[idx]; });
            obj._count = Number(row[pkColumns.length]);
            return obj;
          });
        } catch (dupErr) {
          console.warn(`[VALIDATE_PK] Erro ao buscar sample duplicates: ${dupErr.message}`);
        }
      }

      return res.json({
        ok: true,
        unique: isUnique,
        pk_columns: pkColumns,
        total_rows: totalRows,
        distinct_rows: distinctRows,
        duplicate_count: duplicateCount,
        sample_duplicates: sampleDuplicates,
        scope: 'bronze',
        table: bronzeTable,
      });
    } catch (e) {
      console.error(`[VALIDATE_PK] Erro - dataset_id=${datasetId}`, e);
      if (e.code === 'DATABRICKS_NOT_CONFIGURED') return res.status(503).json(notConfiguredResponse(e));
      return res.status(502).json({ ok: false, error: 'DATABRICKS_ERROR', message: e.message });
    }
  });

  // ===== SUPABASE ROUTES =====
  console.log('[PORTAL] Configurando rotas Supabase...');
  
  const { getSupabaseConfigFromEnv, createSupabaseClient } = require('./supabaseClient');
  const supabaseConfig = getSupabaseConfigFromEnv(process.env);
  
  let supabaseClient = null;
  try {
    supabaseClient = createSupabaseClient(supabaseConfig);
    console.log('[PORTAL] Supabase client: ✓ configurado');
  } catch (e) {
    console.log('[PORTAL] Supabase client: ✗ não configurado -', e.message);
  }

  function notConfiguredSupabaseResponse(err) {
    return {
      ok: false,
      error: err.code || 'SUPABASE_NOT_CONFIGURED',
      message: err.message,
      hint: 'Configure VITE_SUPABASE_URL e VITE_SUPABASE_PUBLISHABLE_KEY no arquivo .env',
    };
  }

  // Test Supabase connection
  app.get('/api/portal/supabase/test-connection', async (req, res) => {
    console.log('[SUPABASE] Test connection');
    try {
      if (!supabaseClient) {
        const err = new Error('Supabase não configurado');
        err.code = 'SUPABASE_NOT_CONFIGURED';
        return res.status(503).json(notConfiguredSupabaseResponse(err));
      }

      const result = await supabaseClient.testConnection();
      return res.json({ ok: true, ...result });
    } catch (e) {
      console.error('[SUPABASE] Error testing connection:', e);
      return res.status(502).json({ ok: false, error: 'SUPABASE_ERROR', message: e.message });
    }
  });

  // List Supabase schemas
  app.get('/api/portal/supabase/schemas', async (req, res) => {
    console.log('[SUPABASE] List schemas');
    try {
      if (!supabaseClient) {
        const err = new Error('Supabase não configurado');
        err.code = 'SUPABASE_NOT_CONFIGURED';
        return res.status(503).json(notConfiguredSupabaseResponse(err));
      }

      const schemas = await supabaseClient.listSchemas();
      return res.json({ ok: true, schemas });
    } catch (e) {
      console.error('[SUPABASE] Error listing schemas:', e);
      return res.status(502).json({ ok: false, error: 'SUPABASE_ERROR', message: e.message });
    }
  });

  // List Supabase tables in a schema
  app.get('/api/portal/supabase/tables', async (req, res) => {
    const schema = String(req.query.schema || 'public').trim();
    console.log(`[SUPABASE] 🔍 List tables - schema=${schema}`);
    console.log(`[SUPABASE] 📝 Request query:`, req.query);
    
    try {
      if (!supabaseClient) {
        const err = new Error('Supabase não configurado');
        err.code = 'SUPABASE_NOT_CONFIGURED';
        console.error('[SUPABASE] ❌ Client não configurado');
        return res.status(503).json(notConfiguredSupabaseResponse(err));
      }

      console.log('[SUPABASE] 📡 Chamando listTables...');
      const tables = await supabaseClient.listTables(schema);
      console.log(`[SUPABASE] ✅ listTables retornou ${tables.length} tabelas:`, tables);
      
      if (tables.length === 0) {
        console.warn('[SUPABASE] ⚠️ Nenhuma tabela encontrada! Verifique se a função RPC existe e se há tabelas no schema.');
      }
      
      // Get row counts for tables (in parallel, with timeout)
      console.log('[SUPABASE] 📊 Buscando row counts...');
      const tablesWithInfo = await Promise.all(
        tables.map(async (table) => {
          try {
            const { count } = await supabaseClient.client
              .from(table.table_name)
              .select('*', { count: 'exact', head: true });
            
            return {
              ...table,
              row_count: count || 0,
            };
          } catch (e) {
            console.warn(`[SUPABASE] ⚠️ Could not get count for ${table.table_name}:`, e.message);
            return table;
          }
        })
      );

      console.log(`[SUPABASE] 🎉 Retornando ${tablesWithInfo.length} tabelas com info`);
      return res.json({ ok: true, tables: tablesWithInfo, schema });
    } catch (e) {
      console.error('[SUPABASE] ❌ Error listing tables:', e);
      return res.status(502).json({ ok: false, error: 'SUPABASE_ERROR', message: e.message });
    }
  });

  // Get table info
  app.get('/api/portal/supabase/tables/:tableName/info', async (req, res) => {
    const tableName = String(req.params.tableName || '').trim();
    const schema = String(req.query.schema || 'public').trim();
    console.log(`[SUPABASE] Get table info - table=${schema}.${tableName}`);
    
    try {
      if (!supabaseClient) {
        const err = new Error('Supabase não configurado');
        err.code = 'SUPABASE_NOT_CONFIGURED';
        return res.status(503).json(notConfiguredSupabaseResponse(err));
      }

      const info = await supabaseClient.getTableInfo(tableName, schema);
      return res.json({ ok: true, table: info });
    } catch (e) {
      console.error('[SUPABASE] Error getting table info:', e);
      return res.status(502).json({ ok: false, error: 'SUPABASE_ERROR', message: e.message });
    }
  });

  console.log('[PORTAL] Supabase routes configured');
  
  // ==================== JOBS ROUTES ====================
  // Import and initialize scheduled jobs routes
  const jobsRoutes = require('./jobsRoutes');
  jobsRoutes(app, db, portalCfg, sqlStringLiteral, sqlQueryObjects, getRequestUser);
}

module.exports = {
  registerPortalRoutes,
};
