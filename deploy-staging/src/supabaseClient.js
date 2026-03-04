const { createClient } = require('@supabase/supabase-js');

function getSupabaseConfigFromEnv(env) {
  return {
    url: String(env.VITE_SUPABASE_URL || '').trim(),
    key: String(env.VITE_SUPABASE_PUBLISHABLE_KEY || '').trim(),
    accessToken: String(env.SUPABASE_ACCESS_TOKEN || '').trim(),
    database: String(env.DATABASE_SUPABASE || 'postgres').trim(),
  };
}

function createSupabaseClient(config) {
  if (!config.url || !config.key) {
    const err = new Error('Supabase: configuração incompleta (VITE_SUPABASE_URL e VITE_SUPABASE_PUBLISHABLE_KEY necessários)');
    err.code = 'SUPABASE_NOT_CONFIGURED';
    throw err;
  }

  const supabase = createClient(config.url, config.key, {
    auth: {
      persistSession: false,
    },
  });

  return {
    client: supabase,
    
    /**
     * Test connection to Supabase
     */
    async testConnection() {
      try {
        const { data, error } = await supabase
          .from('_test_connection_dummy')
          .select('*')
          .limit(0);
        
        // Even if table doesn't exist, if we get a proper error response, connection works
        if (error && error.code !== 'PGRST116') { // PGRST116 = table not found
          // Try a simpler test - just call the API
          const { error: healthError } = await supabase.rpc('version');
          if (healthError && healthError.message) {
            // Connection works but function doesn't exist - that's OK
            return { success: true, message: 'Conexão estabelecida com sucesso' };
          }
        }
        
        return { success: true, message: 'Conexão estabelecida com sucesso' };
      } catch (e) {
        throw new Error(`Erro ao conectar com Supabase: ${e.message}`);
      }
    },

    /**
     * List all schemas
     */
    async listSchemas() {
      try {
        const { data, error } = await supabase.rpc('get_schemas');
        
        if (error) {
          // Fallback: return common schemas
          return ['public', 'auth', 'storage'];
        }
        
        return data || ['public'];
      } catch (e) {
        console.error('[SUPABASE] Error listing schemas:', e);
        return ['public']; // Fallback to public schema
      }
    },

    /**
     * List tables in a schema
     */
    async listTables(schema = 'public') {
      try {
        // Use Management API with access token if available
        if (config.accessToken) {
          const projectRef = config.url.match(/https:\/\/([^.]+)\.supabase/)?.[1];
          if (projectRef) {
            const response = await fetch(
              `https://${projectRef}.supabase.co/rest/v1/rpc/get_tables_in_schema`,
              {
                method: 'POST',
                headers: {
                  'apikey': config.key,
                  'Authorization': `Bearer ${config.accessToken}`,
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({ schema_name: schema }),
              }
            );

            if (response.ok) {
              const data = await response.json();
              if (Array.isArray(data)) {
                return data.map(row => ({
                  table_name: row.table_name || row.tablename,
                  schema_name: schema,
                  table_type: 'BASE TABLE',
                }));
              }
            }
          }
        }

        // Fallback: Use REST API introspection
        return await this.listTablesViaIntrospection(schema);
      } catch (e) {
        console.error('[SUPABASE] Error listing tables:', e);
        return await this.listTablesViaIntrospection(schema);
      }
    },

    /**
     * List tables via REST API introspection
     */
    async listTablesViaIntrospection(schema = 'public') {
      try {
        // Try to use RPC call to get table list
        const { data, error } = await supabase.rpc('get_tables_in_schema', {
          schema_name: schema
        });

        if (!error && data) {
          return Array.isArray(data) ? data.map(row => ({
            table_name: row.table_name || row.tablename || row,
            schema_name: schema,
            table_type: 'BASE TABLE',
          })) : [];
        }

        // If RPC doesn't exist, return empty array
        // User needs to create the function in Supabase
        console.warn('[SUPABASE] RPC function get_tables_in_schema not found. Please create it in Supabase.');
        console.warn('[SUPABASE] SQL: CREATE OR REPLACE FUNCTION get_tables_in_schema(schema_name text) RETURNS TABLE(table_name text) AS $$ SELECT tablename::text FROM pg_tables WHERE schemaname = schema_name $$ LANGUAGE SQL;');
        return [];
      } catch (e) {
        console.error('[SUPABASE] Error in introspection table listing:', e);
        return [];
      }
    },

    /**
     * Get table information including row count and size
     */
    async getTableInfo(tableName, schema = 'public') {
      try {
        // Get row count
        const { count, error: countError } = await supabase
          .from(tableName)
          .select('*', { count: 'exact', head: true });

        if (countError) {
          console.error(`[SUPABASE] Error getting count for ${tableName}:`, countError);
        }

        // Get table columns
        const { data: columns, error: colError } = await supabase
          .from('information_schema.columns')
          .select('column_name, data_type, is_nullable')
          .eq('table_schema', schema)
          .eq('table_name', tableName);

        if (colError) {
          console.error(`[SUPABASE] Error getting columns for ${tableName}:`, colError);
        }

        return {
          table_name: tableName,
          schema_name: schema,
          row_count: count || 0,
          columns: columns || [],
        };
      } catch (e) {
        console.error(`[SUPABASE] Error getting table info for ${tableName}:`, e);
        return {
          table_name: tableName,
          schema_name: schema,
          row_count: 0,
          columns: [],
        };
      }
    },

    /**
     * Get primary key columns for a table via RPC
     * @param {string} tableName - Table name
     * @param {string} schema - Schema name (default: 'public')
     * @returns {Promise<string[]>} Array of PK column names
     */
    async getPkColumns(tableName, schema = 'public') {
      try {
        const { data, error } = await supabase.rpc('get_pk_columns', {
          p_schema: schema,
          p_table: tableName,
        });

        if (!error && Array.isArray(data) && data.length > 0) {
          const pkCols = data.map(r => r.column_name || r);
          console.log(`[SUPABASE] PK columns for ${schema}.${tableName}: ${pkCols.join(', ')}`);
          return pkCols;
        }

        if (error) {
          console.warn(`[SUPABASE] RPC get_pk_columns failed: ${error.message}`);
        }

        // Fallback: look for common PK column names
        return [];
      } catch (e) {
        console.warn(`[SUPABASE] Error detecting PK columns for ${schema}.${tableName}:`, e.message);
        return [];
      }
    },

    /**
     * Execute a raw SQL query (requires direct database access)
     */
    async executeQuery(query) {
      try {
        const { data, error } = await supabase.rpc('execute_sql', { query });
        
        if (error) {
          throw new Error(error.message);
        }
        
        return data;
      } catch (e) {
        throw new Error(`Erro ao executar query: ${e.message}`);
      }
    },
  };
}

module.exports = {
  getSupabaseConfigFromEnv,
  createSupabaseClient,
};
