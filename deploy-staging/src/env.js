const fs = require('fs');
const path = require('path');

function loadEnvFile(filePath) {
  try {
    if (!filePath) return { loaded: false };
    if (!fs.existsSync(filePath)) return { loaded: false };

    const raw = fs.readFileSync(filePath, 'utf8');
    const lines = raw.split(/\r?\n/);

    for (const line of lines) {
      const trimmed = String(line || '').trim();
      if (!trimmed || trimmed.startsWith('#')) continue;

      const idx = trimmed.indexOf('=');
      if (idx <= 0) continue;

      const key = trimmed.slice(0, idx).trim();
      let value = trimmed.slice(idx + 1).trim();

      // strip optional quotes
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }

      if (!Object.prototype.hasOwnProperty.call(process.env, key)) {
        process.env[key] = value;
      }
    }

    return { loaded: true, filePath };
  } catch {
    return { loaded: false };
  }
}

function tryLoadDefaultEnv() {
  // Prefer root .env. For local dev convenience, optionally fall back to mcp-databricks-server/.env
  const rootEnv = path.join(process.cwd(), '.env');
  const mcpEnv = path.join(process.cwd(), 'mcp-databricks-server', '.env');

  const r1 = loadEnvFile(rootEnv);
  if (r1.loaded) return r1;
  const r2 = loadEnvFile(mcpEnv);
  return r2;
}

module.exports = {
  loadEnvFile,
  tryLoadDefaultEnv,
};
