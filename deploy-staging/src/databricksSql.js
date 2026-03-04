function _sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeHost(host) {
  let h = String(host || '').trim();
  if (!h) return '';
  if (h.endsWith('/')) h = h.slice(0, -1);
  if (!/^https?:\/\//i.test(h)) h = `https://${h}`;
  return h;
}

function warehouseIdFromHttpPath(httpPath) {
  const p = String(httpPath || '').trim();
  if (!p) return '';
  const m = p.match(/\/warehouses\/([0-9a-f-]+)/i);
  return m ? m[1] : '';
}

function getDatabricksSqlConfigFromEnv(env) {
  const host = normalizeHost(env.DATABRICKS_HOST || env.DATABRICKS_WORKSPACE_HOST);

  // Local dev: PAT
  const token = String(env.DATABRICKS_TOKEN || '').trim();

  // Databricks Apps: service principal credentials
  const clientId = String(env.DATABRICKS_CLIENT_ID || '').trim();
  const clientSecret = String(env.DATABRICKS_CLIENT_SECRET || '').trim();

  // allow WAREHOUSE_ID (common in examples) as alias
  const warehouseId = String(
    env.DATABRICKS_SQL_WAREHOUSE_ID ||
      env.DATABRICKS_WAREHOUSE_ID ||
      env.WAREHOUSE_ID ||
      warehouseIdFromHttpPath(env.DATABRICKS_HTTP_PATH)
  ).trim();

  // LOG: Diagnostics
  console.log('[DATABRICKS CONFIG]', {
    host: host ? `${host.substring(0, 30)}...` : '(empty)',
    hasToken: Boolean(token),
    tokenLength: token.length,
    hasClientId: Boolean(clientId),
    warehouseId: warehouseId || '(empty)',
    envVars: {
      DATABRICKS_SQL_WAREHOUSE_ID: env.DATABRICKS_SQL_WAREHOUSE_ID ? '✓' : '✗',
      DATABRICKS_WAREHOUSE_ID: env.DATABRICKS_WAREHOUSE_ID ? '✓' : '✗',
      WAREHOUSE_ID: env.WAREHOUSE_ID ? '✓' : '✗',
      DATABRICKS_HTTP_PATH: env.DATABRICKS_HTTP_PATH ? '✓' : '✗',
    }
  });

  return {
    host,
    token,
    clientId,
    clientSecret,
    warehouseId,
  };
}

function createDatabricksSqlClient({ host, token, clientId, clientSecret, warehouseId, userAgent } = {}) {
  const cfg = {
    host: normalizeHost(host),
    token: String(token || '').trim(),
    clientId: String(clientId || '').trim(),
    clientSecret: String(clientSecret || '').trim(),
    warehouseId: String(warehouseId || '').trim(),
    userAgent: String(userAgent || 'data-load-tools/portal').trim(),
  };

  let oauthCache = null; // { access_token, expires_at_ms }

  async function getAccessToken() {
    if (cfg.token) return cfg.token;

    if (!cfg.clientId || !cfg.clientSecret) {
      const err = new Error('Sem credenciais Databricks: informe DATABRICKS_TOKEN ou DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET.');
      err.code = 'DATABRICKS_NOT_CONFIGURED';
      throw err;
    }

    const now = Date.now();
    if (oauthCache && oauthCache.expires_at_ms && oauthCache.expires_at_ms - now > 60_000) {
      return oauthCache.access_token;
    }

    // Databricks workspace OAuth (OIDC) token endpoint
    const url = `${cfg.host}/oidc/v1/token`;

    const body = new URLSearchParams();
    body.set('grant_type', 'client_credentials');
    body.set('scope', 'all-apis');

    const basic = Buffer.from(`${cfg.clientId}:${cfg.clientSecret}`, 'utf8').toString('base64');

    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${basic}`,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'User-Agent': cfg.userAgent,
      },
      body,
    });

    const text = await res.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { _raw: text };
    }

    if (!res.ok) {
      const err = new Error(data?.error_description || data?.error || `OAuth token HTTP ${res.status}`);
      err.code = 'DATABRICKS_OAUTH_ERROR';
      err.httpStatus = res.status;
      err.details = data;
      throw err;
    }

    const expiresInSec = Number(data.expires_in || 0);
    oauthCache = {
      access_token: data.access_token,
      expires_at_ms: expiresInSec ? now + expiresInSec * 1000 : now + 10 * 60_000,
    };

    return oauthCache.access_token;
  }

  function assertConfigured() {
    const missing = [];
    if (!cfg.host) missing.push('DATABRICKS_HOST');
    if (!cfg.warehouseId) missing.push('DATABRICKS_SQL_WAREHOUSE_ID (ou DATABRICKS_HTTP_PATH ou WAREHOUSE_ID)');

    if (!cfg.token && !(cfg.clientId && cfg.clientSecret)) {
      missing.push('DATABRICKS_TOKEN (ou DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET)');
    }

    if (missing.length) {
      console.error('[DATABRICKS ERROR] Configuração incompleta:', {
        missing,
        current: {
          host: cfg.host ? '✓' : '✗',
          token: cfg.token ? '✓ (length: ' + cfg.token.length + ')' : '✗',
          warehouseId: cfg.warehouseId ? '✓' : '✗',
        }
      });
      const err = new Error(`Databricks SQL client não configurado. Faltando: ${missing.join(', ')}`);
      err.code = 'DATABRICKS_NOT_CONFIGURED';
      throw err;
    }
    console.log('[DATABRICKS] ✓ Configuração OK');
  }

  async function _fetchJson(url, options) {
    const res = await fetch(url, options);
    const text = await res.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { _raw: text };
    }

    if (!res.ok) {
      const msg = data?.message || data?.error || `HTTP ${res.status}`;
      const err = new Error(msg);
      err.httpStatus = res.status;
      err.details = data;
      throw err;
    }

    return data;
  }

  async function submitStatement(statement, { onBehalfOf, catalog, schema } = {}) {
    assertConfigured();

    const url = `${cfg.host}/api/2.0/sql/statements`;

    const body = {
      statement: String(statement || ''),
      warehouse_id: cfg.warehouseId,
      disposition: 'INLINE',
      format: 'JSON_ARRAY',
    };

    if (catalog) body.catalog = String(catalog);
    if (schema) body.schema = String(schema);

    const accessToken = await getAccessToken();

    const headers = {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': cfg.userAgent,
    };

    if (onBehalfOf) headers['X-Databricks-User-Agent'] = String(onBehalfOf);

    return _fetchJson(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });
  }

  async function getStatement(statementId) {
    assertConfigured();
    const url = `${cfg.host}/api/2.0/sql/statements/${encodeURIComponent(statementId)}`;
    const accessToken = await getAccessToken();
    return _fetchJson(url, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Accept': 'application/json',
        'User-Agent': cfg.userAgent,
      },
    });
  }

  function _extractResult(resp) {
    const cols = resp?.manifest?.schema?.columns || [];
    const rows = resp?.result?.data_array || [];

    const columns = cols.map((c) => ({
      name: c.name,
      type: c.type_text || c.type_name || c.type || undefined,
    }));

    return {
      columns,
      rows,
      rowCount: resp?.result?.row_count,
      totalRowCount: resp?.manifest?.total_row_count,
      truncated: Boolean(resp?.manifest?.truncated),
    };
  }

  async function query(statement, { timeoutMs = 60_000, pollMs = 750 } = {}) {
    const submitted = await submitStatement(statement);
    const statementId = submitted.statement_id;

    let last = submitted;
    const started = Date.now();

    while (true) {
      const state = last?.status?.state;
      if (state === 'SUCCEEDED') {
        return {
          statementId,
          state,
          ..._extractResult(last),
          raw: last,
        };
      }

      if (state === 'FAILED' || state === 'CANCELED') {
        const errInfo = last?.status?.error || {};
        const err = new Error(errInfo.message || `Statement ${state}`);
        err.code = errInfo.error_code || state;
        err.statementId = statementId;
        err.details = last;
        throw err;
      }

      if (Date.now() - started > timeoutMs) {
        const err = new Error(`Timeout aguardando execução do statement (${timeoutMs}ms)`);
        err.code = 'DATABRICKS_STATEMENT_TIMEOUT';
        err.statementId = statementId;
        err.details = last;
        throw err;
      }

      await _sleep(pollMs);
      last = await getStatement(statementId);
    }
  }

  function rowsAsObjects(result) {
    const cols = result?.columns || [];
    const rows = result?.rows || [];
    return rows.map((r) => {
      const obj = {};
      for (let i = 0; i < cols.length; i++) {
        obj[cols[i].name] = r[i];
      }
      return obj;
    });
  }

  return {
    cfg,
    query,
    rowsAsObjects,
    // exposed for troubleshooting
    _getAccessToken: getAccessToken,
  };
}

module.exports = {
  normalizeHost,
  warehouseIdFromHttpPath,
  getDatabricksSqlConfigFromEnv,
  createDatabricksSqlClient,
};
