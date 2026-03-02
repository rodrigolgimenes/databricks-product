// Test Databricks Connection
const { tryLoadDefaultEnv } = require('./src/env');
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('./src/databricksSql');

console.log('='.repeat(60));
console.log('[TEST] Teste de Conexão com Databricks');
console.log('='.repeat(60));

// Load .env
console.log('\n[1] Carregando arquivo .env...');
const envResult = tryLoadDefaultEnv();
console.log('   Result:', envResult);

// Show environment variables
console.log('\n[2] Variáveis de Ambiente:');
console.log('   DATABRICKS_HOST:', process.env.DATABRICKS_HOST || '(não definida)');
console.log('   DATABRICKS_TOKEN:', process.env.DATABRICKS_TOKEN ? `✓ (${process.env.DATABRICKS_TOKEN.length} chars)` : '✗ (não definida)');
console.log('   DATABRICKS_SQL_WAREHOUSE_ID:', process.env.DATABRICKS_SQL_WAREHOUSE_ID || '(não definida)');
console.log('   UC_CATALOG:', process.env.UC_CATALOG || '(não definida)');

// Get config
console.log('\n[3] Configuração do Databricks:');
const dbCfg = getDatabricksSqlConfigFromEnv(process.env);

// Create client
console.log('\n[4] Criando cliente SQL...');
const db = createDatabricksSqlClient(dbCfg);

// Test query
console.log('\n[5] Executando query de teste...');
console.log('   Query: SELECT 1 AS test');

(async () => {
  try {
    const result = await db.query('SELECT 1 AS test');
    console.log('\n✅ SUCESSO! Conexão funcionando!');
    console.log('   Result:', result);
    
    // Test catalog query
    console.log('\n[6] Testando acesso ao catálogo...');
    const catalogResult = await db.query('SHOW CATALOGS');
    const catalogs = db.rowsAsObjects(catalogResult);
    console.log('   Catalogs encontrados:', catalogs.length);
    catalogs.forEach(c => console.log('     -', c.catalog));
    
    // Test schemas
    console.log('\n[7] Testando schemas do catálogo cm_dbx_dev...');
    const schemasResult = await db.query('SHOW SCHEMAS IN cm_dbx_dev');
    const schemas = db.rowsAsObjects(schemasResult);
    console.log('   Schemas encontrados:', schemas.length);
    schemas.forEach(s => console.log('     -', s.databaseName));
    
    console.log('\n' + '='.repeat(60));
    console.log('✅ TODOS OS TESTES PASSARAM!');
    console.log('='.repeat(60));
    process.exit(0);
    
  } catch (error) {
    console.error('\n❌ ERRO na conexão:');
    console.error('   Code:', error.code);
    console.error('   Message:', error.message);
    if (error.details) {
      console.error('   Details:', JSON.stringify(error.details, null, 2));
    }
    console.log('\n' + '='.repeat(60));
    console.log('❌ TESTE FALHOU');
    console.log('='.repeat(60));
    process.exit(1);
  }
})();
