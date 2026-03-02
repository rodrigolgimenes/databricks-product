// Script to add Sistema Orçamentos project and area to Databricks
const { getDatabricksSqlConfigFromEnv, createDatabricksSqlClient } = require('./src/databricksSql');
const { tryLoadDefaultEnv } = require('./src/env');

async function main() {
  console.log('🔄 Carregando variáveis de ambiente...');
  tryLoadDefaultEnv();
  
  console.log('🔄 Conectando ao Databricks...');
  const config = getDatabricksSqlConfigFromEnv(process.env);
  const db = createDatabricksSqlClient(config);
  
  const catalog = String(process.env.UC_CATALOG || 'cm_dbx_dev').trim();
  const ctrlSchema = `${catalog}.ingestion_sys_ctrl`;
  
  try {
    console.log('\n✨ Adicionando projeto SISTEMA ORÇAMENTOS...');
    
    // Insert project
    await db.query(`
      INSERT INTO ${ctrlSchema}.projects
        (project_id, project_name, description, is_active, created_at, created_by)
      VALUES
        ('sistema_orcamentos', 'SISTEMA ORÇAMENTOS', 'Sistema de gerenciamento de orçamentos', 'true', current_timestamp(), 'admin')
    `);
    console.log('✅ Projeto criado: sistema_orcamentos');
    
    // Insert area
    await db.query(`
      INSERT INTO ${ctrlSchema}.areas
        (area_id, project_id, area_name, description, is_active, created_at, created_by)
      VALUES
        ('orcamentos_220', 'sistema_orcamentos', 'Orçamentos (220)', 'Área de orçamentos com 220 tabelas', 'true', current_timestamp(), 'admin')
    `);
    console.log('✅ Área criada: orcamentos_220 - Orçamentos (220)');
    
    // Verify
    console.log('\n📊 Verificando dados criados...');
    const projectCheck = await db.query(`SELECT * FROM ${ctrlSchema}.projects WHERE project_id = 'sistema_orcamentos'`);
    const areaCheck = await db.query(`SELECT * FROM ${ctrlSchema}.areas WHERE project_id = 'sistema_orcamentos'`);
    
    console.log('\n✅ Projeto:', db.rowsAsObjects(projectCheck)[0]);
    console.log('✅ Área:', db.rowsAsObjects(areaCheck)[0]);
    
    console.log('\n🎉 Dados adicionados com sucesso!');
  } catch (error) {
    if (error.message && error.message.includes('ALREADY_EXISTS')) {
      console.log('\n⚠️  Projeto/Área já existe. Ignorando...');
    } else {
      console.error('\n❌ Erro:', error.message);
      process.exit(1);
    }
  }
}

main().then(() => {
  console.log('\n✅ Script concluído!');
  process.exit(0);
}).catch((err) => {
  console.error('\n❌ Erro fatal:', err);
  process.exit(1);
});
