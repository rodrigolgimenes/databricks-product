/**
 * Dicionário centralizado de explicações "For Dummies".
 * Cada chave mapeia para um texto simples que explica o campo
 * para alguém sem conhecimento de engenharia de dados.
 */
export const FIELD_EXPLANATIONS: Record<string, string> = {
  // ── Job config ──────────────────────────────────────
  job_id:
    "Identificador único do job no portal. Usado internamente para rastrear configurações e execuções.",
  databricks_job_id:
    "Identificador do job no Databricks — a plataforma que realmente executa o processamento de dados. Se vazio, o job ainda não foi registrado lá.",
  schedule_type:
    "Define a frequência do job. DAILY = roda todo dia, WEEKLY = toda semana, CRON = usa expressão personalizada para horários específicos.",
  cron_expression:
    "Define exatamente quando o job executa automaticamente. É uma notação técnica de agendamento. Exemplo: '00 07 * * *' significa 'todo dia às 7h da manhã'.",
  timezone:
    "Fuso horário usado para o agendamento. Os horários do cron são interpretados neste fuso.",
  next_run_at:
    "Próxima data/hora em que o job será executado automaticamente pelo agendador.",
  last_run_at:
    "Data/hora da última vez que este job foi executado, junto com o resultado (sucesso ou falha).",
  last_run_duration:
    "Quanto tempo a última execução levou do início ao fim. Útil para comparar com a média e detectar anomalias.",
  max_concurrent_runs:
    "Quantas execuções deste job podem rodar ao mesmo tempo. Se 1, uma nova execução só inicia quando a anterior terminar. Se 2+, podem rodar em paralelo.",
  timeout_seconds:
    "Tempo máximo (em segundos) que o job pode rodar. Se ultrapassar, será cancelado automaticamente para evitar que fique preso consumindo recursos.",
  retry_on_timeout:
    "Se ativado, quando o job estourar o tempo limite, ele tentará executar novamente automaticamente. Útil para falhas temporárias, mas pode mascarar problemas crônicos.",
  job_status:
    "Indica se o job está ativo (será executado nos horários agendados) ou inativo (pausado, não executa automaticamente).",
  created_at:
    "Quando este job foi criado no portal e por quem.",
  updated_at:
    "Última vez que a configuração do job foi modificada.",

  // ── KPI Metrics ─────────────────────────────────────
  success_rate:
    "Percentual de execuções que completaram sem erros. Abaixo de 80% é sinal de atenção; abaixo de 50% é crítico.",
  avg_duration:
    "Tempo médio de execução baseado no histórico. Se a última execução demorou muito mais que a média, pode indicar problema de performance ou volume de dados maior que o normal.",
  total_runs:
    "Quantidade total de vezes que este job foi executado (agendado ou manualmente).",
  dataset_count:
    "Quantos conjuntos de dados (datasets) este job processa a cada execução. Cada dataset representa uma tabela sendo copiada da origem para o lakehouse.",

  // ── Dataset fields ──────────────────────────────────
  bronze_table:
    "Nome da tabela na camada Bronze do lakehouse. Bronze é a cópia crua dos dados da origem, sem transformações.",
  silver_table:
    "Nome da tabela na camada Silver. Silver contém dados limpos e padronizados, prontos para análise.",
  source_type:
    "Tipo da fonte de dados de onde os dados são extraídos (ex: ORACLE, SHAREPOINT).",
  dataset_status:
    "Estado atual do dataset no pipeline: PUBLISHED = pronto para executar, DRAFT = em configuração.",
  load_strategy:
    "FULL = copia todos os dados toda vez (mais seguro, mais lento). INCREMENTAL = copia só o que mudou desde a última execução (mais rápido, precisa de watermark). SNAPSHOT = cópia completa com controle de versão.",
  watermark:
    "Marca d'água que indica até onde os dados foram sincronizados. Na próxima execução incremental, só serão buscados registros alterados após este ponto.",

  // ── Run / Execution fields ──────────────────────────
  triggered_by:
    "Como a execução foi iniciada: SCHEDULE = pelo agendamento automático, MANUAL = alguém clicou em 'Executar Agora'.",
  run_duration:
    "Tempo total da execução do início ao fim.",
  datasets_processed:
    "Quantos datasets foram processados com sucesso nesta execução.",
  datasets_failed:
    "Quantos datasets falharam durante esta execução. Cada falha deve ser investigada individualmente.",
  datasets_total:
    "Total de datasets que o job tentou processar nesta execução.",
  error_message:
    "Mensagem de erro retornada pelo Databricks quando algo falhou. Contém detalhes técnicos úteis para diagnóstico.",
  error_class:
    "Categoria do erro (ex: ConnectionError, TimeoutError, SchemaError). Ajuda a identificar padrões de falha recorrentes.",
  run_page_url:
    "Link direto para os logs completos desta execução no Databricks. Útil para investigação detalhada por engenheiros.",

  // ── Actions ─────────────────────────────────────────
  action_run_now:
    "Executa o job imediatamente, fora do agendamento normal. Os mesmos datasets serão processados como em uma execução agendada.",
  action_toggle:
    "Ativa ou desativa o agendamento automático do job. Quando inativo, o job não executa nos horários programados, mas ainda pode ser executado manualmente.",
  action_sync:
    "Sincroniza a configuração do job entre o portal e o Databricks. Use quando o job foi criado mas não aparece no Databricks.",
  action_edit:
    "Abre o editor para modificar configurações como agendamento, datasets associados e parâmetros de execução.",
  action_delete:
    "Remove permanentemente o job do portal E do Databricks. Esta ação é irreversível — o job e todo seu histórico de configuração serão apagados.",

  // ── Risk indicator ──────────────────────────────
  risk_stable:
    "O job está funcionando normalmente. Execuções recentes foram bem-sucedidas e a duração está dentro do esperado.",
  risk_unstable:
    "O job apresenta sinais de instabilidade. Pode haver falhas recorrentes ou variação anormal na duração. Requer atenção.",
  risk_critical:
    "O job está em estado crítico. A última execução falhou, a taxa de sucesso está baixa ou há falhas recorrentes. Ação imediata recomendada.",

  // ── Pipeline diagram ────────────────────────────
  pipeline_diagram:
    "Visualização do fluxo de dados deste job. Cada dataset passa por 3 etapas: Leitura da fonte → Escrita na camada Bronze (dados crus) → Escrita na camada Silver (dados tratados). As cores indicam o status da última execução.",
  step_read_source:
    "Etapa de extração: conecta na fonte de dados (Oracle, SharePoint, etc.) e lê os registros. Se falhar aqui, geralmente é problema de conexão ou permissão.",
  step_write_bronze:
    "Etapa Bronze: grava os dados brutos no lakehouse exatamente como vieram da fonte. Operação OVERWRITE substitui tudo; MERGE atualiza apenas o que mudou.",
  step_write_silver:
    "Etapa Silver: aplica limpeza e padronização nos dados Bronze, gerando uma versão pronta para análise. Sempre usa formato Delta.",
};
