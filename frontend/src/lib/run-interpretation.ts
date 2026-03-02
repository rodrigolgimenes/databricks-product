/**
 * Motor de interpretação automática de execuções.
 *
 * Funções puras que transformam metadados crus do batch_process,
 * table_details e dataset_control em textos interpretados para
 * as 3 camadas da UI enterprise (Resumo, Operacional, Diagnóstico).
 *
 * Regra central: silverOperation é a fonte de verdade sobre o que
 * realmente aconteceu — loadType é fallback.
 */

/* ── Tipos ──────────────────────────────────────────── */

export interface RunInterpretationInput {
  // Da execução (batch_process + table_details)
  loadType: string;                    // SNAPSHOT | INCREMENTAL | FULL
  status: string;                      // SUCCEEDED | FAILED | RUNNING
  bronzeOperation?: string;            // OVERWRITE | MERGE | APPEND
  silverOperation?: string;            // OVERWRITE | MERGE | APPEND | REPLACE_WHERE
  bronzeRowCount: number;
  silverRowCount: number;
  silverInsertedCount?: number | null;
  silverUpdatedCount?: number | null;
  durationSeconds?: number | null;
  triggerType?: string;                // MANUAL | SCHEDULE | CRON

  // Do dataset_control (contexto de governança)
  incrementalStrategy?: string;        // SNAPSHOT | CURRENT | APPEND_LOG
  incrementalMetadata?: string | Record<string, any> | null;
  discoveryStatus?: string;            // PENDING_CONFIRMATION | CONFIRMED | etc.
  discoverySuggestion?: string;        // APPEND_LOG | CURRENT | etc.
  enableIncremental?: boolean;
  strategyDecisionLog?: string | any[] | null;

  // Comparativo (execução anterior bem-sucedida)
  previousSilverRowCount?: number | null;
  previousDurationSeconds?: number | null;
}

export type ProcessingIcon = "refresh" | "delta" | "append";
export type VariationSeverity = "normal" | "warning" | "critical";
export type RiskLevel = "none" | "low" | "medium" | "high";

export interface ProcessingType {
  label: string;
  reason: string;
  icon: ProcessingIcon;
}

export interface ResultInterpretation {
  summary: string;
  detail: string;
  countNote?: string;
}

export interface ImpactInterpretation {
  previousCount: number | null;
  currentCount: number;
  variationPercent: number | null;
  variationLabel: string;
  variationSeverity: VariationSeverity;
}

export interface RiskInterpretation {
  level: RiskLevel;
  label: string;
  details: string[];
}

export interface DiagnosticInterpretation {
  reasons: string[];
  suggestion?: string;
  suggestionStrategy?: string;
  status: string;
  decisionLog?: DiagnosticDecisionEntry[];
}

export interface DiagnosticDecisionEntry {
  timestamp: string;
  event: string;
  strategy: string;
  inputs: Record<string, any>;
  reason: string;
}

export interface RunInterpretation {
  processingType: ProcessingType;
  result: ResultInterpretation;
  impact: ImpactInterpretation;
  risk: RiskInterpretation;
  diagnostic?: DiagnosticInterpretation;
}

/* ── Helpers internos ───────────────────────────────── */

const fmtNum = (n: number) => n.toLocaleString("pt-BR");

const safePct = (prev: number | null | undefined, curr: number): number | null => {
  if (prev == null || prev === 0) return null;
  return Math.round(((curr - prev) / prev) * 100);
};

const pctLabel = (pct: number | null): string => {
  if (pct == null) return "Primeira execução";
  if (pct === 0) return "0%";
  return pct > 0 ? `+${pct}%` : `${pct}%`;
};

const pctSeverity = (pct: number | null): VariationSeverity => {
  if (pct == null) return "normal";
  const abs = Math.abs(pct);
  if (abs <= 5) return "normal";
  if (abs <= 20) return "warning";
  return "critical";
};

/** Parse incremental_metadata de forma defensiva */
function parseMetadata(raw: string | Record<string, any> | null | undefined): Record<string, any> | null {
  if (!raw) return null;
  if (typeof raw === "object") return raw as Record<string, any>;
  try {
    const parsed = JSON.parse(raw);
    return typeof parsed === "object" && parsed !== null ? parsed : null;
  } catch {
    return null;
  }
}

/** Parse strategy_decision_log de forma defensiva */
function parseDecisionLog(raw: string | any[] | null | undefined): DiagnosticDecisionEntry[] {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw as DiagnosticDecisionEntry[];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/** Mapeia reason técnico do backend para lista de razões legíveis em PT-BR */
function mapReasonToHuman(reason: string | undefined | null, meta: Record<string, any> | null): string[] {
  if (!reason) return [];
  const lower = reason.toLowerCase();
  const reasons: string[] = [];

  if (lower.includes("no pk")) {
    reasons.push("Não possui Primary Key");
  }
  if (lower.includes("no reliable timestamp") || lower.includes("no watermark")) {
    reasons.push("Não possui coluna de atualização confiável");
  }
  if (lower.includes("small table") || lower.includes("table_size")) {
    const size = meta?.table_size_rows;
    reasons.push(size ? `Tabela pequena (${fmtNum(size)} linhas estimadas)` : "Tabela pequena");
  }
  if (lower.includes("pk_confidence") && meta?.pk_confidence != null && meta.pk_confidence < 0.5) {
    reasons.push(`Confiança na PK muito baixa (${Math.round(meta.pk_confidence * 100)}%)`);
  }

  // Fallback: se nenhuma regra bateu, retorna a reason original
  if (reasons.length === 0) {
    reasons.push(reason);
  }
  return reasons;
}

/** Label legível para discovery_status */
function discoveryStatusLabel(status: string | undefined | null): string {
  if (!status) return "—";
  switch (status.toUpperCase()) {
    case "PENDING_CONFIRMATION": return "Aguardando confirmação";
    case "CONFIRMED": return "Confirmado";
    case "PENDING": return "Pendente";
    case "REJECTED": return "Rejeitado";
    default: return status;
  }
}

/** Label legível para estratégia sugerida */
function strategySuggestionLabel(strategy: string | undefined | null): string | undefined {
  if (!strategy) return undefined;
  switch (strategy.toUpperCase()) {
    case "APPEND_LOG":
      return "Estratégia APPEND_LOG detectada como alternativa. Avaliar inclusão de PK ou coluna de modificação para reduzir custo de processamento.";
    case "CURRENT":
      return "Estratégia CURRENT (MERGE por PK) detectada como alternativa. Confirmação recomendada para habilitar carga incremental.";
    default:
      return `Estratégia ${strategy} detectada como alternativa possível.`;
  }
}

/* ── Motor principal ────────────────────────────────── */

export function interpretRunData(input: RunInterpretationInput): RunInterpretation {
  const processingType = interpretProcessingType(input);
  const result = interpretResult(input);
  const impact = interpretImpact(input);
  const risk = interpretRisk(input, impact);
  const diagnostic = interpretDiagnostic(input);

  return { processingType, result, impact, risk, diagnostic };
}

/* ── ProcessingType ─────────────────────────────────── */

function interpretProcessingType(input: RunInterpretationInput): ProcessingType {
  const silverOp = (input.silverOperation || "").toUpperCase();
  const loadType = (input.loadType || "").toUpperCase();
  const meta = parseMetadata(input.incrementalMetadata);
  const reasonRaw = meta?.reason || meta?._last_decision_log?.reason;

  // Fonte de verdade: silverOperation (o que realmente aconteceu)
  let label: string;
  let icon: ProcessingIcon;

  if (silverOp === "MERGE") {
    label = "INCREMENTAL (Apenas Alterações)";
    icon = "delta";
  } else if (silverOp === "REPLACE_WHERE") {
    label = "INCREMENTAL (Janela Parcial)";
    icon = "delta";
  } else if (silverOp === "APPEND") {
    label = "APPEND (Acúmulo Histórico)";
    icon = "append";
  } else if (silverOp === "OVERWRITE") {
    label = "FULL REFRESH (Substituição Completa)";
    icon = "refresh";
  } else {
    // Fallback por loadType
    if (loadType === "INCREMENTAL") {
      label = "INCREMENTAL (Apenas Alterações)";
      icon = "delta";
    } else if (loadType === "SNAPSHOT" || loadType === "FULL") {
      label = "FULL REFRESH (Substituição Completa)";
      icon = "refresh";
    } else {
      label = loadType || "DESCONHECIDO";
      icon = "refresh";
    }
  }

  // Reason legível
  let reason = "—";
  if (reasonRaw) {
    const humanReasons = mapReasonToHuman(reasonRaw, meta);
    reason = `Origem ${humanReasons.map((r) => r.toLowerCase()).join(" e ")}.`;
  } else if (icon === "refresh") {
    reason = input.enableIncremental === false
      ? "Carga incremental desabilitada para este dataset."
      : "Estratégia configurada como substituição completa.";
  }

  return { label, reason, icon };
}

/* ── Result ─────────────────────────────────────────── */

function interpretResult(input: RunInterpretationInput): ResultInterpretation {
  const silverOp = (input.silverOperation || "").toUpperCase();
  const count = input.silverRowCount ?? input.bronzeRowCount ?? 0;
  const summary = `${fmtNum(count)} registros processados`;

  const inserted = input.silverInsertedCount;
  const updated = input.silverUpdatedCount;
  const hasInsertedOrUpdated = (inserted != null && inserted > 0) || (updated != null && updated > 0);

  if (silverOp === "OVERWRITE") {
    return {
      summary,
      detail: "Tabela Silver substituída integralmente",
      countNote: "Não há contagem individual de inserções/atualizações neste modo.",
    };
  }

  if (silverOp === "MERGE") {
    if (!hasInsertedOrUpdated) {
      return {
        summary,
        detail: "Nenhuma alteração detectada nesta execução",
        countNote: "Dados da origem idênticos à Silver. Nenhuma linha inserida ou atualizada.",
      };
    }
    const parts: string[] = [];
    if (inserted != null && inserted > 0) parts.push(`${fmtNum(inserted)} inseridas`);
    if (updated != null && updated > 0) parts.push(`${fmtNum(updated)} atualizadas`);
    return {
      summary,
      detail: `${parts.join(", ")} via MERGE`,
    };
  }

  if (silverOp === "APPEND") {
    return {
      summary,
      detail: `${fmtNum(count)} registros adicionados ao histórico`,
    };
  }

  if (silverOp === "REPLACE_WHERE") {
    return {
      summary,
      detail: "Janela de dados substituída na Silver",
    };
  }

  // Fallback
  return { summary, detail: `${fmtNum(count)} registros na Silver` };
}

/* ── Impact ─────────────────────────────────────────── */

function interpretImpact(input: RunInterpretationInput): ImpactInterpretation {
  const current = input.silverRowCount ?? input.bronzeRowCount ?? 0;
  const prev = input.previousSilverRowCount;
  const pct = safePct(prev, current);

  return {
    previousCount: prev ?? null,
    currentCount: current,
    variationPercent: pct,
    variationLabel: pctLabel(pct),
    variationSeverity: pctSeverity(pct),
  };
}

/* ── Risk ───────────────────────────────────────────── */

function interpretRisk(input: RunInterpretationInput, impact: ImpactInterpretation): RiskInterpretation {
  const details: string[] = [];
  let level: RiskLevel = "none";

  const promote = (newLevel: RiskLevel) => {
    const order: RiskLevel[] = ["none", "low", "medium", "high"];
    if (order.indexOf(newLevel) > order.indexOf(level)) level = newLevel;
  };

  // 1. Execução falhou
  if ((input.status || "").toUpperCase() === "FAILED") {
    promote("high");
    details.push("Execução falhou");
  }

  // 2. Silent failure (sucesso sem dados)
  const count = input.silverRowCount ?? input.bronzeRowCount ?? 0;
  if ((input.status || "").toUpperCase() === "SUCCEEDED" && count === 0) {
    promote("high");
    details.push("Sucesso sem nenhum registro processado (possível falha silenciosa)");
  }

  // 3. Queda abrupta de volume
  const pct = impact.variationPercent;
  if (pct != null && pct < -50) {
    promote("high");
    details.push(`Queda abrupta de volume (${pct}%) — possível perda de dados na origem`);
  } else if (pct != null && pct < -20) {
    promote("medium");
    details.push(`Queda significativa de volume (${pct}%)`);
  }

  // 4. Aumento inesperado
  if (pct != null && pct > 50) {
    promote("medium");
    details.push(`Aumento inesperado de volume (+${pct}%)`);
  }

  // 5. Performance degradada
  if (
    input.durationSeconds != null &&
    input.previousDurationSeconds != null &&
    input.previousDurationSeconds > 0 &&
    input.durationSeconds > input.previousDurationSeconds * 2
  ) {
    promote("low");
    details.push("Duração mais que o dobro da execução anterior");
  }

  // Label consolidado
  let label: string;
  if (level === "none") label = "Nenhum risco identificado";
  else if (level === "low") label = "Atenção baixa";
  else if (level === "medium") label = "Atenção recomendada";
  else label = "Risco identificado";

  return { level, label, details };
}

/* ── Diagnostic ─────────────────────────────────────── */

export function interpretDiagnostic(input: RunInterpretationInput): DiagnosticInterpretation | undefined {
  const meta = parseMetadata(input.incrementalMetadata);
  const strategy = (input.incrementalStrategy || "").toUpperCase();
  const discoveryStatus = (input.discoveryStatus || "").toUpperCase();
  const suggestion = input.discoverySuggestion;

  // Se não há metadata, nem sugestão pendente, sem diagnóstico para exibir
  const reasonRaw = meta?.reason || meta?._last_decision_log?.reason;
  const hasPendingSuggestion = suggestion && discoveryStatus === "PENDING_CONFIRMATION";

  if (!reasonRaw && !hasPendingSuggestion && strategy !== "SNAPSHOT") {
    return undefined;
  }

  const reasons = mapReasonToHuman(reasonRaw, meta);
  const suggestionText = strategySuggestionLabel(suggestion);
  const decisionLog = parseDecisionLog(input.strategyDecisionLog);

  return {
    reasons,
    suggestion: suggestionText,
    suggestionStrategy: suggestion || undefined,
    status: discoveryStatusLabel(input.discoveryStatus),
    decisionLog: decisionLog.length > 0 ? decisionLog : undefined,
  };
}
