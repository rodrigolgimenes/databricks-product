export type RunStatus =
  | "QUEUED"
  | "STARTING"
  | "RUNNING"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED"
  | "TIMED_OUT"
  | "SUCCEEDED_WITH_ISSUES"
  | "ORPHANED"
  | "INCONSISTENT";

export type DbxExecutionState =
  | "PENDING"
  | "QUEUED"
  | "RUNNING"
  | "TERMINATING"
  | "TERMINATED"
  | "SKIPPED"
  | "INTERNAL_ERROR"
  | "UNKNOWN";

export interface RunTelemetry {
  runId: string;
  status: RunStatus;
  startTime?: string;
  endTime?: string;
  durationMs?: number;
  recordsWritten?: number;
  dqFailCount?: number;
  expectedRecordsMin?: number;
  expectedRecordsMax?: number;
  watermarkBefore?: string;
  watermarkAfter?: string;
}

export interface BaselineMetrics {
  p50DurationMs: number;
  p95DurationMs: number;
  p99DurationMs: number;
  sampleSize: number;
}

export interface GuardrailConfig {
  portalJobId: string;
  expectedFrequency: "hourly" | "daily" | "weekly";
  volumeMin?: number;
  volumeMax?: number;
  watermarkRequired: boolean;
  dqRequired: boolean;
  silentFailureCheck: boolean;
}

export interface GuardrailResultItem {
  check: "watermark" | "volume" | "dq" | "silent_failure";
  status: "PASS" | "WARN" | "FAIL";
  message: string;
}

export interface GuardrailResult {
  finalStatus: RunStatus;
  checks: GuardrailResultItem[];
  needsAttention: boolean;
}

export interface ReconcileResult {
  executionSource: "DATABRICKS";
  runStatus: RunStatus;
  inconsistent: boolean;
  reason?: string;
}

const normalizeErrorText = (input: string) =>
  input
    .toLowerCase()
    .replace(/\d{4}-\d{2}-\d{2}t\d{2}:\d{2}:\d{2}(?:\.\d+)?z?/g, "<ts>")
    .replace(/[a-f0-9]{8,}/g, "<hex>")
    .replace(/\d+/g, "<num>")
    .replace(/\s+/g, " ")
    .trim();

const hashString = (text: string) => {
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = (hash << 5) - hash + text.charCodeAt(i);
    hash |= 0;
  }
  return `sig_${Math.abs(hash)}`;
};

export const buildErrorSignature = (params: {
  errorCode?: string;
  errorMessage?: string;
  stackTopFrames?: string[];
}) => {
  const code = normalizeErrorText(params.errorCode || "unknown_code");
  const message = normalizeErrorText(params.errorMessage || "unknown_message");
  const frames = normalizeErrorText((params.stackTopFrames || []).slice(0, 3).join("|"));
  return hashString(`${code}|${message}|${frames}`);
};

const mapDbxStateToPortal = (dbxState: DbxExecutionState): RunStatus => {
  switch (dbxState) {
    case "PENDING":
    case "QUEUED":
      return "QUEUED";
    case "RUNNING":
      return "RUNNING";
    case "TERMINATING":
      return "RUNNING";
    case "TERMINATED":
      return "SUCCEEDED";
    case "SKIPPED":
      return "CANCELLED";
    case "INTERNAL_ERROR":
      return "FAILED";
    default:
      return "INCONSISTENT";
  }
};

export const reconcileRunState = (
  dbxState: DbxExecutionState,
  portalStatus: RunStatus
): ReconcileResult => {
  const mapped = mapDbxStateToPortal(dbxState);
  if (mapped === "INCONSISTENT") {
    return {
      executionSource: "DATABRICKS",
      runStatus: "INCONSISTENT",
      inconsistent: true,
      reason: "Estado Databricks desconhecido",
    };
  }
  if (portalStatus === "ORPHANED" && (mapped === "RUNNING" || mapped === "QUEUED")) {
    return {
      executionSource: "DATABRICKS",
      runStatus: mapped,
      inconsistent: false,
      reason: "Reconciliado com estado ativo do Databricks",
    };
  }
  if (portalStatus === "RUNNING" && mapped === "SUCCEEDED") {
    return {
      executionSource: "DATABRICKS",
      runStatus: "SUCCEEDED",
      inconsistent: true,
      reason: "Portal atrasado: Databricks já terminou",
    };
  }
  return {
    executionSource: "DATABRICKS",
    runStatus: mapped,
    inconsistent: mapped !== portalStatus && portalStatus !== "SUCCEEDED_WITH_ISSUES",
  };
};

const percentile = (sorted: number[], p: number) => {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)];
};

export const calculateBaselineMetrics = (durationsMs: number[]): BaselineMetrics => {
  const valid = durationsMs.filter((x) => Number.isFinite(x) && x > 0).sort((a, b) => a - b);
  return {
    p50DurationMs: percentile(valid, 50),
    p95DurationMs: percentile(valid, 95),
    p99DurationMs: percentile(valid, 99),
    sampleSize: valid.length,
  };
};

export const evaluateGuardrails = (
  telemetry: RunTelemetry,
  guardrail: GuardrailConfig
): GuardrailResult => {
  const checks: GuardrailResultItem[] = [];
  const hasWatermarkProgress =
    !guardrail.watermarkRequired || (telemetry.watermarkBefore && telemetry.watermarkAfter && telemetry.watermarkBefore !== telemetry.watermarkAfter);
  checks.push({
    check: "watermark",
    status: hasWatermarkProgress ? "PASS" : "FAIL",
    message: hasWatermarkProgress ? "Watermark avançou" : "Watermark não avançou",
  });

  const volume = telemetry.recordsWritten ?? 0;
  const min = guardrail.volumeMin ?? 0;
  const max = guardrail.volumeMax ?? Number.MAX_SAFE_INTEGER;
  const volumeOk = volume >= min && volume <= max;
  checks.push({
    check: "volume",
    status: volumeOk ? "PASS" : "FAIL",
    message: volumeOk ? "Volume dentro do esperado" : `Volume fora da faixa (${min}-${max})`,
  });

  const dqFails = telemetry.dqFailCount ?? 0;
  const dqOk = !guardrail.dqRequired || dqFails === 0;
  checks.push({
    check: "dq",
    status: dqOk ? "PASS" : "FAIL",
    message: dqOk ? "Sem falhas críticas de DQ" : `${dqFails} falha(s) crítica(s) de DQ`,
  });

  const silentFailure = guardrail.silentFailureCheck && telemetry.status === "SUCCEEDED" && volume === 0;
  checks.push({
    check: "silent_failure",
    status: silentFailure ? "FAIL" : "PASS",
    message: silentFailure ? "Sucesso sem efeito detectado" : "Sem falha silenciosa detectada",
  });

  const hasFail = checks.some((c) => c.status === "FAIL");
  return {
    finalStatus: hasFail && telemetry.status === "SUCCEEDED" ? "SUCCEEDED_WITH_ISSUES" : telemetry.status,
    checks,
    needsAttention: hasFail,
  };
};

export const computeNearMissFlag = (durationMs: number, baseline: BaselineMetrics, bufferPercent = 10) => {
  const threshold = baseline.p95DurationMs * (1 + bufferPercent / 100);
  return baseline.sampleSize >= 10 && durationMs > threshold;
};

// ── Feature Flags ──────────────────────────────────────────

export interface OpsFeatureFlags {
  enableGuardrails: boolean;
  enableReplayPolicy: boolean;
  enableNearMiss: boolean;
  enableIncidentAutoCreate: boolean;
  guardrailMode: "monitor" | "enforce";
}

export const DEFAULT_OPS_FLAGS: OpsFeatureFlags = {
  enableGuardrails: true,
  enableReplayPolicy: true,
  enableNearMiss: true,
  enableIncidentAutoCreate: false,
  guardrailMode: "monitor",
};

/**
 * Evaluate guardrails respecting feature flags.
 * In "monitor" mode, checks run but finalStatus is never promoted to SUCCEEDED_WITH_ISSUES.
 */
export const evaluateGuardrailsWithFlags = (
  telemetry: RunTelemetry,
  guardrail: GuardrailConfig,
  flags: OpsFeatureFlags = DEFAULT_OPS_FLAGS
): GuardrailResult => {
  if (!flags.enableGuardrails) {
    return { finalStatus: telemetry.status, checks: [], needsAttention: false };
  }
  const result = evaluateGuardrails(telemetry, guardrail);
  if (flags.guardrailMode === "monitor") {
    return { ...result, finalStatus: telemetry.status };
  }
  return result;
};

// ── Ingestion idempotency key ──────────────────────────────

export const buildIdempotencyKey = (params: {
  workspaceId: string;
  jobId: string;
  runId: string;
  attempt: number;
}) => `${params.workspaceId}|${params.jobId}|${params.runId}|${params.attempt}`;

// ── Shared Ops types ───────────────────────────────────────

export interface OpsRunRealtimeItem {
  runId: string;
  portalJobId: string;
  jobName: string;
  domainId: string;
  criticalityTier: "T0" | "T1" | "T2" | "T3";
  status: RunStatus;
  durationMs: number;
  p95DurationMs: number;
  recordsWritten: number;
  watermarkBefore: string;
  watermarkAfter: string;
  nearMissRiskPercent: number;
  needsAttention: boolean;
  sparkUiUrl?: string;
}

export interface OpsIncidentItem {
  incidentId: string;
  title: string;
  severity: "S0" | "S1" | "S2" | "S3" | "S4";
  status: "OPEN" | "ACK" | "INVESTIGATING" | "RESOLVED";
  rootCategory: string;
  firstSeenAt: string;
  lastSeenAt: string;
  occurrenceCount: number;
  owner?: string;
  runbookUrl?: string;
  primaryRunId?: string;
}

export interface ReplayAssessment {
  mode: "RETRY_RUN" | "REPLAY_SAFE" | "REPLAY_SANDBOX" | "BACKFILL_RANGE";
  policyDecision: "ALLOWED" | "NEEDS_APPROVAL" | "BLOCKED";
  riskScore: number;
  estimatedCostUsd: number;
  estimatedRows: number;
  impactJobs: number;
  reason: string;
}

// ── Ingestion overlap window ───────────────────────────────

export const computeCollectionWindow = (params: {
  lastCollectedAt: Date;
  overlapMinutes?: number;
}) => {
  const overlap = params.overlapMinutes ?? 10;
  const start = new Date(params.lastCollectedAt.getTime() - overlap * 60_000);
  return { start, end: new Date() };
};

