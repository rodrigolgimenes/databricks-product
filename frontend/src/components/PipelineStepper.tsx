import { useState, useEffect, useRef } from "react";
import { Badge } from "@/components/ui/badge";
import {
  CheckCircle, AlertCircle, Loader2, Circle,
  Play, Download, HardDrive, Layers, Flag,
  ArrowRight, Timer, Database, RefreshCw,
} from "lucide-react";
import * as api from "@/lib/api";

/* ── types ──────────────────────────────────────────── */

interface PipelinePhase {
  key: string;
  label: string;
  icon: typeof Download;
  stepKeys: string[];   // which step_keys map to this phase
  phaseKeys: string[];  // which phase values map here
}

type PhaseStatus = "succeeded" | "running" | "failed" | "pending";

interface PhaseData {
  phase: PipelinePhase;
  status: PhaseStatus;
  durationSec: number | null;
  kpis: { label: string; value: string; highlight?: boolean }[];
  message?: string;
}

interface PipelineStepperProps {
  runId: string;
  /** Current batch_process status (RUNNING, SUCCEEDED, FAILED, etc.) */
  runStatus: string;
  /** Pre-loaded steps if already available */
  initialSteps?: any[];
  /** batch_process data for extra context */
  batchProcess?: any;
  /** Compact mode — hides KPI details */
  compact?: boolean;
}

/* ── constants ──────────────────────────────────────── */

const PHASES: PipelinePhase[] = [
  {
    key: "START",
    label: "Início",
    icon: Play,
    stepKeys: ["RUN_STARTED"],
    phaseKeys: ["ORCHESTRATOR"],
  },
  {
    key: "READ_SOURCE",
    label: "Leitura Fonte",
    icon: Download,
    stepKeys: ["BRONZE_LOAD"],
    phaseKeys: ["BRONZE"],
  },
  {
    key: "BRONZE",
    label: "Bronze",
    icon: HardDrive,
    stepKeys: ["BRONZE_LOAD"],
    phaseKeys: ["BRONZE"],
  },
  {
    key: "SILVER",
    label: "Silver",
    icon: Layers,
    stepKeys: ["SILVER_PROMOTE"],
    phaseKeys: ["SILVER"],
  },
  {
    key: "DONE",
    label: "Finalizado",
    icon: Flag,
    stepKeys: ["WATERMARK"],
    phaseKeys: ["ORCHESTRATOR"],
  },
];

/* ── helpers ─────────────────────────────────────────── */

const fmtNum = (n: any) => {
  const v = Number(n);
  return isNaN(v) ? "—" : v.toLocaleString("pt-BR");
};

const fmtDuration = (s: number | null) => {
  if (s == null || isNaN(s)) return "—";
  if (s < 60) return `${Math.round(s)}s`;
  const m = Math.floor(s / 60);
  const sec = Math.round(s % 60);
  if (m < 60) return `${m}m ${sec}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
};

function parseDetails(step: any): Record<string, any> | null {
  if (step.details && typeof step.details === "object") return step.details;
  if (step.details_json) {
    try { return JSON.parse(step.details_json); } catch { /* ignore */ }
  }
  return null;
}

/** Map raw steps into pipeline PhaseData[] */
function buildPhaseData(
  steps: any[],
  runStatus: string,
  bp?: any,
): PhaseData[] {
  const upper = (s: string) => String(s || "").toUpperCase();
  const isRunning = ["RUNNING", "CLAIMED"].includes(upper(runStatus));
  const isFailed = upper(runStatus) === "FAILED";

  // Find steps by step_key
  const runStarted = steps.find((s) => upper(s.step_key) === "RUN_STARTED");
  const bronzeLoad = steps.find((s) => upper(s.step_key) === "BRONZE_LOAD");
  const silverPromote = steps.find((s) => upper(s.step_key) === "SILVER_PROMOTE");
  const watermark = steps.find((s) => upper(s.step_key) === "WATERMARK");

  const stepDuration = (step: any) => {
    if (!step?.started_at) return null;
    const end = step.finished_at ? new Date(step.finished_at) : new Date();
    return Math.round((end.getTime() - new Date(step.started_at).getTime()) / 1000);
  };

  const stepStatus = (step: any, fallback: PhaseStatus = "pending"): PhaseStatus => {
    if (!step) return fallback;
    const st = upper(step.status);
    if (st === "SUCCEEDED") return "succeeded";
    if (st === "FAILED") return "failed";
    if (st === "RUNNING" || st === "CLAIMED") return "running";
    return "pending";
  };

  // ── Início ──
  const startStatus = runStarted
    ? (upper(runStarted.status) === "RUNNING" && !bronzeLoad ? "running" : "succeeded")
    : (isRunning ? "running" : "pending");
  const startDetails = parseDetails(runStarted || {});
  const startKpis: { label: string; value: string; highlight?: boolean }[] = [];
  if (runStarted?.started_at) {
    startKpis.push({
      label: "Início",
      value: new Date(runStarted.started_at).toLocaleString("pt-BR", {
        hour: "2-digit", minute: "2-digit", second: "2-digit",
      }),
    });
  }
  if (startDetails?.sre_metrics) {
    const sre = startDetails.sre_metrics;
    if (sre.claim_attempt_count) {
      startKpis.push({ label: "Claims", value: String(sre.claim_attempt_count) });
    }
  }

  // ── Leitura Fonte (derived from BRONZE_LOAD step) ──
  const bronzeDetails = parseDetails(bronzeLoad || {});
  let readStatus: PhaseStatus = "pending";
  if (bronzeLoad) {
    const bs = upper(bronzeLoad.status);
    if (bs === "RUNNING") readStatus = "running";
    else if (bs === "SUCCEEDED") readStatus = "succeeded";
    else if (bs === "FAILED") readStatus = "failed";
  } else if (startStatus === "succeeded" && isRunning) {
    readStatus = "running";
  }

  const readKpis: { label: string; value: string; highlight?: boolean }[] = [];
  if (bronzeDetails?.oracle_table) {
    readKpis.push({ label: "Tabela Oracle", value: bronzeDetails.oracle_table });
  }
  if (bronzeDetails?.incremental != null) {
    readKpis.push({
      label: "Modo",
      value: bronzeDetails.incremental ? "INCREMENTAL" : "FULL",
      highlight: true,
    });
  } else if (bp?.load_type) {
    readKpis.push({ label: "Modo", value: bp.load_type, highlight: true });
  }
  if (bp?.incremental_rows_read != null && Number(bp.incremental_rows_read) > 0) {
    readKpis.push({
      label: "Registros lidos",
      value: fmtNum(bp.incremental_rows_read),
      highlight: true,
    });
  }
  if (bronzeDetails?.watermark_end) {
    readKpis.push({ label: "Watermark", value: String(bronzeDetails.watermark_end) });
  }

  // ── Bronze ──
  const bronzeStatus = bronzeLoad
    ? stepStatus(bronzeLoad)
    : (readStatus === "succeeded" ? (isRunning ? "running" : "pending") : "pending");
  const bronzeKpis: { label: string; value: string; highlight?: boolean }[] = [];
  if (bronzeDetails?.bronze_row_count != null) {
    bronzeKpis.push({
      label: "Registros Bronze",
      value: fmtNum(bronzeDetails.bronze_row_count),
      highlight: true,
    });
  } else if (bp?.bronze_row_count != null) {
    bronzeKpis.push({
      label: "Registros Bronze",
      value: fmtNum(bp.bronze_row_count),
      highlight: true,
    });
  }
  if (bronzeDetails?.strategy) {
    bronzeKpis.push({ label: "Strategy", value: String(bronzeDetails.strategy) });
  }
  if (bronzeDetails?.optimize_executed) {
    bronzeKpis.push({ label: "OPTIMIZE", value: "✓" });
  }

  // ── Silver ──
  const silverDetails = parseDetails(silverPromote || {});
  let silverStatus: PhaseStatus = "pending";
  if (silverPromote) {
    silverStatus = stepStatus(silverPromote);
  } else if (bronzeStatus === "succeeded" && isRunning) {
    silverStatus = "running";
  }
  const silverKpis: { label: string; value: string; highlight?: boolean }[] = [];
  if (silverDetails?.silver_table) {
    silverKpis.push({ label: "Tabela Silver", value: silverDetails.silver_table });
  }
  if (bp?.silver_row_count != null) {
    silverKpis.push({
      label: "Registros Silver",
      value: fmtNum(bp.silver_row_count),
      highlight: true,
    });
  }

  // ── Finalizado ──
  let doneStatus: PhaseStatus = "pending";
  if (watermark) {
    doneStatus = stepStatus(watermark);
  } else if (!isRunning && !isFailed && silverStatus === "succeeded") {
    doneStatus = "succeeded";
  } else if (isFailed) {
    doneStatus = "failed";
  }
  const doneKpis: { label: string; value: string; highlight?: boolean }[] = [];
  if (runStarted?.finished_at) {
    const totalSec = stepDuration(runStarted);
    if (totalSec != null) {
      doneKpis.push({ label: "Duração Total", value: fmtDuration(totalSec), highlight: true });
    }
  }

  return [
    { phase: PHASES[0], status: startStatus, durationSec: stepDuration(runStarted), kpis: startKpis, message: runStarted?.message },
    { phase: PHASES[1], status: readStatus, durationSec: bronzeLoad ? stepDuration(bronzeLoad) : null, kpis: readKpis, message: bronzeLoad?.message },
    { phase: PHASES[2], status: bronzeStatus, durationSec: bronzeLoad ? stepDuration(bronzeLoad) : null, kpis: bronzeKpis },
    { phase: PHASES[3], status: silverStatus, durationSec: stepDuration(silverPromote), kpis: silverKpis, message: silverPromote?.message },
    { phase: PHASES[4], status: doneStatus, durationSec: null, kpis: doneKpis },
  ];
}

/* ── status visual mapping ───────────────────────────── */

const STATUS_STYLES: Record<PhaseStatus, {
  ring: string; bg: string; text: string; line: string; iconEl: JSX.Element;
}> = {
  succeeded: {
    ring: "ring-green-400 bg-green-100",
    bg: "bg-green-50 border-green-200",
    text: "text-green-700",
    line: "bg-green-400",
    iconEl: <CheckCircle className="h-4 w-4 text-green-600" />,
  },
  running: {
    ring: "ring-blue-400 bg-blue-100",
    bg: "bg-blue-50 border-blue-200",
    text: "text-blue-700",
    line: "bg-blue-400",
    iconEl: <Loader2 className="h-4 w-4 text-blue-600 animate-spin" />,
  },
  failed: {
    ring: "ring-red-400 bg-red-100",
    bg: "bg-red-50 border-red-200",
    text: "text-red-700",
    line: "bg-red-400",
    iconEl: <AlertCircle className="h-4 w-4 text-red-600" />,
  },
  pending: {
    ring: "ring-gray-300 bg-gray-100",
    bg: "bg-gray-50 border-gray-200",
    text: "text-gray-500",
    line: "bg-gray-300",
    iconEl: <Circle className="h-4 w-4 text-gray-400" />,
  },
};

/* ── component ───────────────────────────────────────── */

export function PipelineStepper({
  runId,
  runStatus,
  initialSteps,
  batchProcess,
  compact = false,
}: PipelineStepperProps) {
  const [steps, setSteps] = useState<any[]>(initialSteps || []);
  const [loading, setLoading] = useState(!initialSteps);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const isActive = ["RUNNING", "CLAIMED"].includes(String(runStatus).toUpperCase());

  // Fetch steps on mount
  useEffect(() => {
    if (initialSteps && initialSteps.length > 0) {
      setSteps(initialSteps);
      setLoading(false);
      return;
    }
    fetchSteps();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  // Poll every 5s for active executions
  useEffect(() => {
    if (isActive) {
      intervalRef.current = setInterval(fetchSteps, 5000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, isActive]);

  const fetchSteps = async () => {
    try {
      const data = await api.getRunSteps(runId);
      setSteps(data.items || []);
    } catch {
      /* silently ignore polling errors */
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-3 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" /> Carregando pipeline...
      </div>
    );
  }

  if (steps.length === 0 && !isActive) return null;

  const phases = buildPhaseData(steps, runStatus, batchProcess);

  return (
    <div className="rounded-lg border bg-card overflow-hidden">
      {/* ── Header ── */}
      <div className="flex items-center justify-between px-4 py-2.5 bg-muted/30 border-b">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Database className="h-4 w-4 text-purple-500" />
          Pipeline de Execução
          {isActive && (
            <Badge variant="secondary" className="text-[10px] animate-pulse">
              LIVE
            </Badge>
          )}
        </div>
        {isActive && (
          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
            <RefreshCw className="h-3 w-3 animate-spin" /> Atualiza a cada 5s
          </span>
        )}
      </div>

      {/* ── Stepper Row ── */}
      <div className="px-4 py-4">
        <div className="flex items-start gap-0">
          {phases.map((pd, idx) => {
            const style = STATUS_STYLES[pd.status];
            const PhaseIcon = pd.phase.icon;
            const isLast = idx === phases.length - 1;

            return (
              <div key={pd.phase.key} className="flex items-start flex-1 min-w-0">
                {/* Phase Node */}
                <div className="flex flex-col items-center min-w-[80px]">
                  {/* Circle icon */}
                  <div className={`flex items-center justify-center w-9 h-9 rounded-full ring-2 ${style.ring}`}>
                    {pd.status === "running" ? (
                      <Loader2 className={`h-4.5 w-4.5 animate-spin ${style.text}`} />
                    ) : pd.status === "succeeded" ? (
                      <CheckCircle className={`h-4.5 w-4.5 ${style.text}`} />
                    ) : pd.status === "failed" ? (
                      <AlertCircle className={`h-4.5 w-4.5 ${style.text}`} />
                    ) : (
                      <PhaseIcon className="h-4 w-4 text-gray-400" />
                    )}
                  </div>

                  {/* Label + duration */}
                  <span className={`mt-1.5 text-xs font-medium ${style.text}`}>
                    {pd.phase.label}
                  </span>
                  {pd.durationSec != null && pd.durationSec > 0 && (
                    <span className="text-[10px] text-muted-foreground flex items-center gap-0.5">
                      <Timer className="h-2.5 w-2.5" /> {fmtDuration(pd.durationSec)}
                    </span>
                  )}

                  {/* KPIs below the node (not in compact mode) */}
                  {!compact && pd.kpis.length > 0 && (
                    <div className={`mt-2 w-full max-w-[140px] rounded-md border p-2 space-y-1 text-[10px] ${style.bg}`}>
                      {pd.kpis.map((kpi, ki) => (
                        <div key={ki} className="flex flex-col">
                          <span className="text-muted-foreground leading-tight">{kpi.label}</span>
                          <span
                            className={`font-mono leading-tight truncate ${
                              kpi.highlight ? "font-semibold " + style.text : ""
                            }`}
                            title={kpi.value}
                          >
                            {kpi.value}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Connector line */}
                {!isLast && (
                  <div className="flex-1 flex items-center pt-4 px-1">
                    <div className={`h-0.5 w-full rounded ${
                      pd.status === "succeeded" ? style.line : "bg-gray-200"
                    }`} />
                    <ArrowRight className={`h-3 w-3 flex-shrink-0 -ml-0.5 ${
                      pd.status === "succeeded" ? style.text : "text-gray-300"
                    }`} />
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

/* ── Helper to determine current phase label for badges ── */

export function getCurrentPhaseLabel(steps: any[], runStatus: string): string | null {
  const upper = (s: string) => String(s || "").toUpperCase();
  const isRunning = ["RUNNING", "CLAIMED"].includes(upper(runStatus));
  if (!isRunning) return null;

  // Find the latest RUNNING step
  const runningStep = steps.find((s) => upper(s.status) === "RUNNING");
  if (runningStep) {
    const key = upper(runningStep.step_key);
    if (key === "RUN_STARTED") return "Iniciando";
    if (key === "BRONZE_LOAD") return "Lendo Fonte / Bronze";
    if (key === "SILVER_PROMOTE") return "Promovendo Silver";
    if (key === "WATERMARK") return "Finalizando";
    return runningStep.step_key;
  }

  // No RUNNING step found — infer from completed steps
  const succeededKeys = steps
    .filter((s) => upper(s.status) === "SUCCEEDED")
    .map((s) => upper(s.step_key));

  if (succeededKeys.includes("SILVER_PROMOTE")) return "Finalizando";
  if (succeededKeys.includes("BRONZE_LOAD")) return "Promovendo Silver";
  if (succeededKeys.includes("RUN_STARTED") && !succeededKeys.includes("BRONZE_LOAD")) return "Lendo Fonte / Bronze";
  return "Iniciando";
}

export default PipelineStepper;
