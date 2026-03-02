-- Add Sistema Orçamentos Project and Area
-- Execute this in Databricks SQL

-- Insert new project
INSERT INTO cm_dbx_dev.ingestion_sys_ctrl.projects
  (project_id, project_name, description, is_active, created_at, created_by)
VALUES
  ('sistema_orcamentos', 'SISTEMA ORÇAMENTOS', 'Sistema de gerenciamento de orçamentos', 'true', current_timestamp(), 'admin');

-- Insert new area
INSERT INTO cm_dbx_dev.ingestion_sys_ctrl.areas
  (area_id, project_id, area_name, description, is_active, created_at, created_by)
VALUES
  ('orcamentos_220', 'sistema_orcamentos', 'Orçamentos (220)', 'Área de orçamentos com 220 tabelas', 'true', current_timestamp(), 'admin');

-- Verify
SELECT * FROM cm_dbx_dev.ingestion_sys_ctrl.projects WHERE project_id = 'sistema_orcamentos';
SELECT * FROM cm_dbx_dev.ingestion_sys_ctrl.areas WHERE project_id = 'sistema_orcamentos';
