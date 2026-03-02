const express = require('express');
const path = require('path');

const { registerPortalRoutes } = require('./src/portalRoutes');

console.log('='.repeat(60));
console.log('[SERVER] Inicializando servidor...');
console.log('[SERVER] CWD:', process.cwd());
console.log('[SERVER] NODE_ENV:', process.env.NODE_ENV || 'development');
console.log('='.repeat(60));

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '1mb' }));

// Static frontend
app.use(express.static(path.join(__dirname, 'public')));

// Mocked in-memory data
let nextId = 3;
let configs = [
  {
    id: 1,
    name: 'clientes_diario',
    type: 'INCREMENTAL',
    sourceKind: 'Tabela (Unity Catalog)',
    sourceValue: 'main.crm.clientes',
    target: 'bronze.crm.clientes',
    writeMode: 'append',
    incrementalColumn: 'updated_at',
    batchSize: 1000,
    dryRun: true,
    createdAt: new Date().toISOString()
  },
  {
    id: 2,
    name: 'produtos_full',
    type: 'FULL',
    sourceKind: 'Arquivo/Path',
    sourceValue: '/Volumes/landing/produtos/',
    target: 'bronze.erp.produtos',
    writeMode: 'overwrite',
    incrementalColumn: null,
    batchSize: 5000,
    dryRun: true,
    createdAt: new Date().toISOString()
  }
];

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    app: process.env.APP_TITLE || 'Data Load Tools',
    env: process.env.APP_ENV || 'dev',
    now: new Date().toISOString()
  });
});

app.get('/api/configs', (req, res) => {
  res.json({ items: configs });
});

// Governed Ingestion Portal (Fase 5)
registerPortalRoutes(app);

app.post('/api/configs', (req, res) => {
  const body = req.body || {};

  const name = String(body.name || '').trim();
  const type = String(body.type || '').trim();
  const sourceKind = String(body.sourceKind || '').trim();
  const sourceValue = String(body.sourceValue || '').trim();
  const target = String(body.target || '').trim();
  const writeMode = String(body.writeMode || '').trim();
  const incrementalColumn = body.incrementalColumn == null ? null : String(body.incrementalColumn).trim();
  const batchSize = Number.isFinite(Number(body.batchSize)) ? Number(body.batchSize) : 1000;
  const dryRun = Boolean(body.dryRun);

  const errors = [];
  if (!name) errors.push('Informe o nome da carga.');
  if (!['FULL', 'INCREMENTAL'].includes(type)) errors.push('Tipo inválido.');
  if (!sourceKind) errors.push('Informe o tipo da fonte.');
  if (!sourceValue) errors.push('Informe o valor da fonte.');
  if (!target) errors.push('Informe o destino.');
  if (!['append', 'overwrite'].includes(writeMode)) errors.push('Write mode inválido.');
  if (type === 'INCREMENTAL' && !incrementalColumn) errors.push('Para INCREMENTAL, informe a coluna incremental.');

  if (errors.length) {
    return res.status(400).json({ error: 'validation_error', errors });
  }

  const item = {
    id: nextId++,
    name,
    type,
    sourceKind,
    sourceValue,
    target,
    writeMode,
    incrementalColumn: type === 'INCREMENTAL' ? incrementalColumn : null,
    batchSize,
    dryRun,
    createdAt: new Date().toISOString()
  };

  configs = [item, ...configs];
  return res.status(201).json(item);
});

// SPA fallback (so / always loads the UI)
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const port = Number(process.env.DATABRICKS_APP_PORT || process.env.PORT) || 3000;
app.listen(port, () => {
  console.log('='.repeat(60));
  console.log(`[SERVER] ✅ Servidor rodando em http://localhost:${port}`);
  console.log(`[SERVER] ✅ Interface V2: http://localhost:${port}/v2.html`);
  console.log(`[SERVER] ✅ Interface V1: http://localhost:${port}/index.html`);
  console.log('='.repeat(60));
});
