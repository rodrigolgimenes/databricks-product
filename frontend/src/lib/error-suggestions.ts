/**
 * Maps error patterns to actionable suggestions in Portuguese.
 * Pure function — no React dependencies.
 */
export function getErrorSuggestion(errorMessage?: string, errorClass?: string): string | null {
  if (!errorMessage && !errorClass) return null;
  const msg = (errorMessage || '').toLowerCase();
  const cls = (errorClass || '').toLowerCase();

  if (msg.includes('timeout') || cls.includes('timeout'))
    return 'Considerar aumentar o timeout do job ou otimizar a query da fonte.';
  if (msg.includes('connection') || msg.includes('connect') || cls.includes('connection'))
    return 'Verificar conectividade com a fonte de dados (Oracle/banco de origem).';
  if (msg.includes('schema') || msg.includes('column') || cls.includes('schema'))
    return 'Schema da fonte pode ter mudado. Verificar e re-publicar o dataset.';
  if (msg.includes('permission') || msg.includes('access') || cls.includes('auth'))
    return 'Verificar permissões de acesso no Databricks e na fonte de dados.';
  if (msg.includes('memory') || msg.includes('oom') || cls.includes('resource'))
    return 'Recurso insuficiente. Considerar aumentar o cluster ou particionar os dados.';
  if (msg.includes('duplicate') || msg.includes('unique') || msg.includes('constraint'))
    return 'Conflito de dados duplicados. Verificar a chave primária do dataset.';
  return 'Verificar logs detalhados no Databricks para diagnóstico completo.';
}

/**
 * Classifies failure speed to give context on the type of error.
 */
export function classifyFailureSpeed(durationMs: number | undefined): string | null {
  if (!durationMs) return null;
  const seconds = durationMs / 1000;
  if (seconds < 10) return 'Falha imediata — provável erro de configuração ou conexão';
  if (seconds < 60) return 'Falha rápida — possível erro de schema ou permissão';
  if (seconds > 600) return 'Falha tardia — possível gargalo de performance ou volume';
  return null;
}
