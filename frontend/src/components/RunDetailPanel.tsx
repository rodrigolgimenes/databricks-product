import { useState, useEffect, useRef } from "react";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  CheckCircle, AlertCircle, Clock, Loader2, Database,
  ChevronDown, ChevronRight, FileText, Layers, Timer,
  ArrowRight, Server, Hash, BarChart3, AlertTriangle,
  Plus, RefreshCw, Wrench,
} from "lucide-react";
import * as api from "@/lib/api";
import { useViewMode } from "@/contexts/ViewModeContext";
import { interpretRunData, type RunInterpretationInput } from "@/lib/run-interpretation";
import { RunExecutiveSummary } from "@/components/RunExecutiveSummary";
import { RunDiagnosticCard } from "@/components/RunDiagnosticCard";
import { PipelineStepper } from "@/components/PipelineStepper";

/* ── helpers ────────────────────────────────────────── */

const statusIcon = (status: string, size = "h-4 w-4") => {
  const s = String(status).toUpperCase();
  if (s === "SUCCEEDED") return <CheckCircle className={`${size} text-green-500`} />;
  if (s === "FAILED") return <AlertCircle className={`${size} text-red-500`} />;
  if (s === "RUNNING" || s === "CLAIMED") return <Loader2 className={`${size} text-blue-500 animate-spin`} />;
  if (s === "SKIPPED") return <ArrowRight className={`${size} text-gray-400`} />;
  return <Clock className={`${size} text-yellow-500`} />;
};

const statusBadgeVariant = (s: string) => {
  const u = String(s).toUpperCase();
  if (u === "SUCCEEDED") return "default" as const;
  if (u === "FAILED") return "destructive" as const;
  return "secondary" as const;
};

const formatDuration = (seconds: any) => {
  const s = Number(seconds);
  if (!s || isNaN(s)) return "—";
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const sec = Math.round(s % 60);
  if (m < 60) return `${m}m ${sec}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
};

const formatTs = (ts: any) =>
  ts ? new Date(ts).toLocaleString("pt-BR") : "—";

const fmtNum = (n: any) => {
  const v = Number(n);
  if (isNaN(v)) return "—";
  return v.toLocaleString("pt-BR");
};

/* ── types ──────────────────────────────────────────── */

interface RunDetailPanelProps {
  runId: string;
  /** If true, the panel auto-fetches data when mounted */
  autoLoad?: boolean;
  /** If already available, pass the batch_process object to avoid extra fetch */
  batchProcess?: any;
  /** Compact mode for inline use */
  compact?: boolean;
}

/* ── component ──────────────────────────────────────── */

const RunDetailPanel = ({ runId, autoLoad = true, batchProcess: bpProp, compact = false }: RunDetailPanelProps) => {
  const { isEngineering } = useViewMode();
  const [runData, setRunData] = useState<any>(null);
  const [steps, setSteps] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [stepsLoading, setStepsLoading] = useState(false);
  const [error, setError] = useState("");
  const [showTechnical, setShowTechnical] = useState(false);
  const [showSteps, setShowSteps] = useState(false);
  const [showTableDetails, setShowTableDetails] = useState(false);
  const [showError, setShowError] = useState(false);
  const [showStacktrace, setShowStacktrace] = useState(false);
  const [expandedSteps, setExpandedSteps] = useState<Set<number>>(new Set());
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Engineering mode: expand technical details by default
  useEffect(() => {
    setShowTechnical(isEngineering);
  }, [isEngineering]);

  useEffect(() => {
    if (!autoLoad || !runId) return;
    fetchRunData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, autoLoad]);

  // Auto-poll run data every 5s when execution is RUNNING
  const currentStatus = String(runData?.batch_process?.status || bpProp?.status || "").toUpperCase();
  const isRunningExecution = ["RUNNING", "CLAIMED"].includes(currentStatus);

  useEffect(() => {
    if (isRunningExecution && runId) {
      pollRef.current = setInterval(async () => {
        try {
          const data = await api.getRunDetails(runId);
          setRunData(data);
        } catch { /* ignore polling errors */ }
      }, 5000);
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [runId, isRunningExecution]);

  const fetchRunData = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await api.getRunDetails(runId);
      setRunData(data);
    } catch (e: any) {
      setError(e.message || "Erro ao carregar detalhes da execução");
    } finally {
      setLoading(false);
    }
  };

  const fetchSteps = async () => {
    if (steps.length > 0) { setShowSteps(!showSteps); return; }
    setStepsLoading(true);
    try {
      const data = await api.getRunSteps(runId);
      setSteps(data.items || []);
      setShowSteps(true);
    } catch {
      setSteps([]);
      setShowSteps(true);
    } finally {
      setStepsLoading(false);
    }
  };

  const toggleStepExpand = (idx: number) => {
    setExpandedSteps((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx); else next.add(idx);
      return next;
    });
  };

  const bp = runData?.batch_process || bpProp;
  const rq = runData?.run_queue;
  const tableDetails: any[] = runData?.table_details || [];
  const dsCtx = runData?.dataset_context;
  const prevRun = runData?.previous_run;

  // Derive Bronze merge metrics from table_details
  const bronzeTd = tableDetails.find((td: any) => String(td.layer).toUpperCase() === 'BRONZE');
  const silverTd = tableDetails.find((td: any) => String(td.layer).toUpperCase() === 'SILVER');
  const isIncremental = String(bp?.load_type || '').toUpperCase() === 'INCREMENTAL';

  // Build interpretation input
  const durationSec = bp?.started_at && bp?.finished_at
    ? Math.round((new Date(bp.finished_at).getTime() - new Date(bp.started_at).getTime()) / 1000)
    : null;

  const interpretationInput: RunInterpretationInput | null = bp ? {
    loadType: bp.load_type || bp.execution_mode || "",
    status: bp.status || "",
    bronzeOperation: bronzeTd?.operation,
    silverOperation: silverTd?.operation,
    bronzeRowCount: Number(bp.bronze_row_count) || 0,
    silverRowCount: Number(bp.silver_row_count) || 0,
    silverInsertedCount: silverTd?.inserted_count != null ? Number(silverTd.inserted_count) : null,
    silverUpdatedCount: silverTd?.updated_count != null ? Number(silverTd.updated_count) : null,
    durationSeconds: durationSec,
    triggerType: rq?.trigger_type,
    incrementalStrategy: dsCtx?.incremental_strategy,
    incrementalMetadata: dsCtx?.incremental_metadata,
    discoveryStatus: dsCtx?.discovery_status,
    discoverySuggestion: dsCtx?.discovery_suggestion,
    enableIncremental: dsCtx?.enable_incremental,
    strategyDecisionLog: dsCtx?.strategy_decision_log,
    previousSilverRowCount: prevRun?.silver_row_count != null ? Number(prevRun.silver_row_count) : null,
    previousDurationSeconds: prevRun?.duration_seconds != null ? Number(prevRun.duration_seconds) : null,
  } : null;

  const interpretation = interpretationInput ? interpretRunData(interpretationInput) : null;

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Carregando detalhes...
      </div>
    );
  }

  if (error) {
    return (
      <div className="py-3 px-4 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
        {error}
      </div>
    );
  }

  if (!bp && !runData) return null;

  const isFailed = String(bp?.status).toUpperCase() === "FAILED";

  return (
    <div className={`space-y-3 ${compact ? "" : "mt-2"}`}>

      {/* ══════════════════════════════════════════════ */}
      {/* PIPELINE STEPPER — visual pipeline phases     */}
      {/* ══════════════════════════════════════════════ */}
      {bp && runId && (
        <PipelineStepper
          runId={runId}
          runStatus={bp.status || ""}
          batchProcess={bp}
        />
      )}

      {/* ══════════════════════════════════════════════ */}
      {/* CAMADA 1 — Resumo Executivo (sempre visível)  */}
      {/* ══════════════════════════════════════════════ */}
      {interpretation && (
        <RunExecutiveSummary
          interpretation={interpretation}
          durationSeconds={durationSec}
          startedAt={bp?.started_at}
          finishedAt={bp?.finished_at}
          triggerType={rq?.trigger_type}
          requestedBy={rq?.requested_by}
        />
      )}

      {/* ══════════════════════════════════════════════ */}
      {/* CAMADA 3 — Diagnóstico Inteligente            */}
      {/* ══════════════════════════════════════════════ */}
      {interpretation?.diagnostic && (
        <RunDiagnosticCard
          diagnostic={interpretation.diagnostic}
          showDecisionLog={isEngineering}
        />
      )}

      {/* ══════════════════════════════════════════════ */}
      {/* Error Details (always visible when failed)    */}
      {/* ══════════════════════════════════════════════ */}
      {isFailed && (bp?.error_class || bp?.error_message) && (
        <div className="border border-red-200 rounded-lg overflow-hidden">
          <button
            className="w-full flex items-center justify-between p-3 bg-red-50 hover:bg-red-100 transition-colors text-left"
            onClick={() => setShowError(!showError)}
          >
            <span className="flex items-center gap-2 text-sm font-medium text-red-800">
              <AlertCircle className="h-4 w-4" /> Erro da Execução
              {bp.error_class && (
                <Badge variant="destructive" className="text-xs">{bp.error_class}</Badge>
              )}
            </span>
            {showError ? <ChevronDown className="h-4 w-4 text-red-600" /> : <ChevronRight className="h-4 w-4 text-red-600" />}
          </button>
          {showError && (
            <div className="p-4 space-y-3 bg-red-50/50">
              {bp.error_message && (
                <div>
                  <p className="text-xs font-medium text-red-700 mb-1">Mensagem de Erro:</p>
                  <pre className="text-xs text-red-800 bg-red-100 p-3 rounded overflow-auto max-h-48 whitespace-pre-wrap font-mono">
                    {bp.error_message}
                  </pre>
                </div>
              )}
              {bp.error_stacktrace && (
                <div>
                  <button
                    className="text-xs font-medium text-red-700 mb-1 flex items-center gap-1 hover:underline"
                    onClick={() => setShowStacktrace(!showStacktrace)}
                  >
                    {showStacktrace ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                    Stacktrace
                  </button>
                  {showStacktrace && (
                    <pre className="text-xs text-red-800 bg-red-100 p-3 rounded overflow-auto max-h-64 whitespace-pre-wrap font-mono">
                      {bp.error_stacktrace}
                    </pre>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════ */}
      {/* CAMADA 2 — Detalhes Técnicos (colapsável)     */}
      {/* ══════════════════════════════════════════════ */}
      <div className="border rounded-lg overflow-hidden">
        <button
          className="w-full flex items-center justify-between p-3 bg-muted/30 hover:bg-muted/50 transition-colors text-left"
          onClick={() => setShowTechnical(!showTechnical)}
        >
          <span className="flex items-center gap-2 text-sm font-medium">
            <Wrench className="h-4 w-4 text-slate-500" /> Detalhes Técnicos
          </span>
          {showTechnical ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </button>
        {showTechnical && (
          <div className="p-3 space-y-3">
            {/* ── Execution KPIs ── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <InfoBlock icon={<Timer className="h-4 w-4 text-blue-500" />} label="Duração" value={formatDuration(durationSec)} />
              <InfoBlock
                icon={<Database className="h-4 w-4 text-amber-500" />}
                label="Tipo de Carga"
                value={bp?.load_type || bp?.execution_mode || "—"}
              />
              {isIncremental ? (
                <InfoBlock icon={<ArrowRight className="h-4 w-4 text-green-500" />} label="Linhas Lidas (Fonte)" value={fmtNum(bp?.incremental_rows_read)} />
              ) : (
                <InfoBlock icon={<Database className="h-4 w-4 text-amber-500" />} label="Bronze Rows" value={fmtNum(bp?.bronze_row_count)} />
              )}
              <InfoBlock icon={<Layers className="h-4 w-4 text-emerald-500" />} label="Total Bronze" value={fmtNum(bp?.bronze_row_count)} />
            </div>

            {/* ── Incremental / Upsert Details ── */}
            {isIncremental && (
              <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-xs space-y-2">
                <p className="font-medium text-sm flex items-center gap-1 text-green-800">
                  <ArrowRight className="h-3.5 w-3.5" /> Carga Incremental — Detalhes do MERGE
                </p>
                {/* Watermark column indicator */}
                {(dsCtx?.watermark_info?.column || (typeof dsCtx?.incremental_metadata === 'object' && dsCtx?.incremental_metadata?.watermark_column)) && (
                  <div className="flex items-center gap-2 px-2 py-1.5 bg-green-100 border border-green-300 rounded text-xs">
                    <Clock className="h-3.5 w-3.5 text-green-700 flex-shrink-0" />
                    <span className="text-green-800">
                      <span className="text-muted-foreground">Coluna Delta:</span>{" "}
                      <span className="font-mono font-semibold text-green-900">
                        {dsCtx?.watermark_info?.column || (typeof dsCtx?.incremental_metadata === 'object' && dsCtx.incremental_metadata.watermark_column) || "—"}
                      </span>
                    </span>
                    {dsCtx?.watermark_info?.type && (
                      <Badge variant="outline" className="text-[10px] px-1.5 py-0 border-green-400 text-green-700">
                        {dsCtx.watermark_info.type}
                      </Badge>
                    )}
                  </div>
                )}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                  <div>
                    <span className="text-muted-foreground">Linhas Lidas:</span>{" "}
                    <span className="font-mono font-semibold text-green-700">{fmtNum(bp?.incremental_rows_read)}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Total Bronze:</span>{" "}
                    <span className="font-mono font-semibold">{fmtNum(bp?.bronze_row_count)}</span>
                  </div>
                  {bronzeTd && (
                    <>
                      <div className="flex items-center gap-1">
                        <Plus className="h-3 w-3 text-green-600" />
                        <span className="text-muted-foreground">Inseridas:</span>{" "}
                        <span className="font-mono font-semibold text-green-700">{fmtNum(bronzeTd.inserted_count)}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <RefreshCw className="h-3 w-3 text-amber-600" />
                        <span className="text-muted-foreground">Atualizadas:</span>{" "}
                        <span className="font-mono font-semibold text-amber-700">{fmtNum(bronzeTd.updated_count)}</span>
                      </div>
                    </>
                  )}
                </div>
                {(bp?.watermark_start || bp?.watermark_end) && (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-2 pt-1 border-t border-green-200">
                    {bp.watermark_start && (
                      <div>
                        <span className="text-muted-foreground">Watermark Início:</span>{" "}
                        <span className="font-mono">{bp.watermark_start}</span>
                      </div>
                    )}
                    {bp.watermark_end && (
                      <div>
                        <span className="text-muted-foreground">Watermark Fim:</span>{" "}
                        <span className="font-mono">{bp.watermark_end}</span>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* ── Silver Layer ── */}
            {silverTd && (
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <InfoBlock icon={<Layers className="h-4 w-4 text-emerald-500" />} label="Total Silver" value={fmtNum(bp?.silver_row_count)} />
                <InfoBlock
                  icon={<Plus className="h-4 w-4 text-green-500" />}
                  label="Silver Inseridas"
                  value={silverTd.operation?.toUpperCase() === "OVERWRITE" ? "—" : fmtNum(silverTd.inserted_count)}
                />
                <InfoBlock
                  icon={<RefreshCw className="h-4 w-4 text-amber-500" />}
                  label="Silver Atualizadas"
                  value={silverTd.operation?.toUpperCase() === "OVERWRITE" ? "—" : fmtNum(silverTd.updated_count)}
                />
              </div>
            )}
            {silverTd?.operation?.toUpperCase() === "OVERWRITE" && (
              <p className="text-[11px] text-muted-foreground italic">
                Operação OVERWRITE: tabela substituída integralmente. Inseridas/Atualizadas não se aplicam.
              </p>
            )}

            {/* ── Timestamps ── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
              <div>
                <span className="text-muted-foreground">Início:</span>{" "}
                <span className="font-medium">{formatTs(bp?.started_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Fim:</span>{" "}
                <span className="font-medium">{formatTs(bp?.finished_at)}</span>
              </div>
              {rq && (
                <>
                  <div>
                    <span className="text-muted-foreground">Solicitado:</span>{" "}
                    <span className="font-medium">{formatTs(rq.requested_at)}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Por:</span>{" "}
                    <span className="font-medium">{rq.requested_by || "—"}</span>
                  </div>
                </>
              )}
            </div>

            {/* ── Orchestrator Info ── */}
            {(bp?.orchestrator_job_id || bp?.orchestrator_run_id || bp?.orchestrator_task) && (
              <div className="p-3 bg-muted/30 rounded-lg text-xs space-y-1">
                <p className="font-medium text-sm flex items-center gap-1">
                  <Server className="h-3.5 w-3.5" /> Orchestrator
                </p>
                <div className="grid grid-cols-3 gap-2">
                  {bp.orchestrator_job_id && <div><span className="text-muted-foreground">Job ID:</span> <span className="font-mono">{bp.orchestrator_job_id}</span></div>}
                  {bp.orchestrator_run_id && <div><span className="text-muted-foreground">Run ID:</span> <span className="font-mono">{bp.orchestrator_run_id}</span></div>}
                  {bp.orchestrator_task && <div><span className="text-muted-foreground">Task:</span> <span className="font-mono">{bp.orchestrator_task}</span></div>}
                </div>
              </div>
            )}

            {/* ── Queue Info ── */}
            {rq && (
              <div className="p-3 bg-muted/30 rounded-lg text-xs space-y-1">
                <p className="font-medium text-sm flex items-center gap-1">
                  <Hash className="h-3.5 w-3.5" /> Fila (Run Queue)
                </p>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                  <div><span className="text-muted-foreground">Queue ID:</span> <span className="font-mono">{rq.queue_id?.slice(0, 12)}...</span></div>
                  <div><span className="text-muted-foreground">Trigger:</span> {rq.trigger_type || "—"}</div>
                  <div><span className="text-muted-foreground">Tentativa:</span> {rq.attempt}/{rq.max_retries}</div>
                  <div><span className="text-muted-foreground">Prioridade:</span> {rq.priority ?? "—"}</div>
                  {rq.claim_owner && <div><span className="text-muted-foreground">Claim:</span> <span className="font-mono">{rq.claim_owner}</span></div>}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* ── Table Details (expandable) ──────────────── */}
      {tableDetails.length > 0 && (
        <div className="border rounded-lg overflow-hidden">
          <button
            className="w-full flex items-center justify-between p-3 bg-muted/30 hover:bg-muted/50 transition-colors text-left"
            onClick={() => setShowTableDetails(!showTableDetails)}
          >
            <span className="flex items-center gap-2 text-sm font-medium">
              <BarChart3 className="h-4 w-4 text-blue-500" /> Detalhes por Tabela
              <Badge variant="secondary" className="text-xs">{tableDetails.length}</Badge>
            </span>
            {showTableDetails ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          </button>
          {showTableDetails && (
            <div className="overflow-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/20 text-left">
                    <th className="p-2 font-medium text-muted-foreground">Layer</th>
                    <th className="p-2 font-medium text-muted-foreground">Tabela</th>
                    <th className="p-2 font-medium text-muted-foreground">Operação</th>
                    <th className="p-2 font-medium text-muted-foreground">Status</th>
                    <th className="p-2 font-medium text-muted-foreground text-right">Linhas</th>
                    <th className="p-2 font-medium text-muted-foreground text-right">Inseridas</th>
                    <th className="p-2 font-medium text-muted-foreground text-right">Atualizadas</th>
                    <th className="p-2 font-medium text-muted-foreground text-right">Deletadas</th>
                    <th className="p-2 font-medium text-muted-foreground">Início</th>
                    <th className="p-2 font-medium text-muted-foreground">Fim</th>
                    <th className="p-2 font-medium text-muted-foreground">Erro</th>
                  </tr>
                </thead>
                <tbody>
                  {tableDetails.map((td: any, i: number) => (
                    <tr key={i} className="border-b hover:bg-muted/20">
                      <td className="p-2">
                        <Badge variant={td.layer === "bronze" ? "secondary" : "default"} className="text-xs">
                          {td.layer}
                        </Badge>
                      </td>
                      <td className="p-2 font-mono">{td.table_name || "—"}</td>
                      <td className="p-2">{td.operation || "—"}</td>
                      <td className="p-2">
                        <div className="flex items-center gap-1">
                          {statusIcon(td.status || "UNKNOWN", "h-3 w-3")}
                          <span>{td.status}</span>
                        </div>
                      </td>
                      <td className="p-2 text-right font-mono font-medium">{fmtNum(td.row_count)}</td>
                      <td className="p-2 text-right font-mono text-green-700">{fmtNum(td.inserted_count)}</td>
                      <td className="p-2 text-right font-mono text-amber-700">{fmtNum(td.updated_count)}</td>
                      <td className="p-2 text-right font-mono text-red-700">{fmtNum(td.deleted_count)}</td>
                      <td className="p-2 whitespace-nowrap">{formatTs(td.started_at)}</td>
                      <td className="p-2 whitespace-nowrap">{formatTs(td.finished_at)}</td>
                      <td className="p-2 text-red-700 max-w-[200px] truncate">{td.error_message || ""}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ── Steps Timeline (expandable, lazy loaded) ──────────────── */}
      <div className="border rounded-lg overflow-hidden">
        <button
          className="w-full flex items-center justify-between p-3 bg-muted/30 hover:bg-muted/50 transition-colors text-left"
          onClick={fetchSteps}
        >
          <span className="flex items-center gap-2 text-sm font-medium">
            <FileText className="h-4 w-4 text-purple-500" /> Passos da Execução (Steps)
            {steps.length > 0 && <Badge variant="secondary" className="text-xs">{steps.length}</Badge>}
          </span>
          <div className="flex items-center gap-2">
            {stepsLoading && <Loader2 className="h-4 w-4 animate-spin" />}
            {showSteps ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          </div>
        </button>
        {showSteps && (
          <div className="p-3 space-y-2">
            {steps.length === 0 ? (
              <p className="text-xs text-muted-foreground text-center py-4">
                Nenhum passo registrado para esta execução.
              </p>
            ) : (
              steps.map((step, idx) => {
                const isExpanded = expandedSteps.has(idx);
                const stepDuration = step.started_at && step.finished_at
                  ? Math.round((new Date(step.finished_at).getTime() - new Date(step.started_at).getTime()) / 1000)
                  : (step.step_duration_seconds ?? null);
                const progressPct = step.progress_total
                  ? Math.round((Number(step.progress_current || 0) / Number(step.progress_total)) * 100)
                  : null;
                const iStepFailed = String(step.status).toUpperCase() === "FAILED";

                let parsedDetails: any = null;
                if (step.details) {
                  parsedDetails = step.details;
                } else if (step.details_json) {
                  try { parsedDetails = JSON.parse(step.details_json); } catch { /* ignore */ }
                }

                return (
                  <div
                    key={idx}
                    className={`border rounded-lg overflow-hidden ${iStepFailed ? "border-red-200" : ""}`}
                  >
                    <button
                      className={`w-full flex items-center gap-3 p-3 text-left hover:bg-muted/30 transition-colors ${
                        iStepFailed ? "bg-red-50/50" : ""
                      }`}
                      onClick={() => toggleStepExpand(idx)}
                    >
                      {/* step index */}
                      <span className="flex-shrink-0 w-6 h-6 rounded-full bg-muted flex items-center justify-center text-xs font-bold">
                        {idx + 1}
                      </span>
                      {statusIcon(step.status)}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          {step.phase && (
                            <Badge variant="outline" className="text-xs">{step.phase}</Badge>
                          )}
                          <span className="font-medium text-sm truncate">{step.step_key || step.message || "Step"}</span>
                        </div>
                        {step.message && step.message !== step.step_key && (
                          <p className="text-xs text-muted-foreground mt-0.5 truncate">{step.message}</p>
                        )}
                      </div>
                      <div className="flex items-center gap-3 flex-shrink-0">
                        {progressPct !== null && (
                          <div className="w-20">
                            <Progress value={progressPct} className="h-1.5" />
                            <p className="text-[10px] text-muted-foreground text-right mt-0.5">
                              {fmtNum(step.progress_current)}/{fmtNum(step.progress_total)}
                            </p>
                          </div>
                        )}
                        <span className="text-xs text-muted-foreground w-14 text-right">
                          {formatDuration(stepDuration)}
                        </span>
                        {isExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                      </div>
                    </button>

                    {isExpanded && (
                      <div className="px-4 pb-3 pt-1 border-t space-y-2 text-xs">
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                          <div>
                            <span className="text-muted-foreground">Status:</span>{" "}
                            <Badge variant={statusBadgeVariant(step.status)} className="text-xs">{step.status}</Badge>
                          </div>
                          <div>
                            <span className="text-muted-foreground">Phase:</span>{" "}
                            <span className="font-medium">{step.phase || "—"}</span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">Início:</span>{" "}
                            <span className="font-medium">{formatTs(step.started_at)}</span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">Fim:</span>{" "}
                            <span className="font-medium">{formatTs(step.finished_at)}</span>
                          </div>
                        </div>

                        {step.message && (
                          <div>
                            <span className="text-muted-foreground">Mensagem:</span>
                            <p className="mt-1 p-2 bg-muted/30 rounded font-mono whitespace-pre-wrap">
                              {step.message}
                            </p>
                          </div>
                        )}

                        {progressPct !== null && (
                          <div>
                            <span className="text-muted-foreground">Progresso:</span>{" "}
                            <span className="font-medium">
                              {fmtNum(step.progress_current)} / {fmtNum(step.progress_total)} ({progressPct}%)
                            </span>
                          </div>
                        )}

                        {parsedDetails && (
                          <div>
                            <span className="text-muted-foreground">Detalhes (JSON):</span>
                            <pre className="mt-1 p-2 bg-muted/30 rounded overflow-auto max-h-40 whitespace-pre-wrap font-mono text-[11px]">
                              {JSON.stringify(parsedDetails, null, 2)}
                            </pre>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </div>
        )}
      </div>
    </div>
  );
};

/* ── small helper sub-component ──────────────────── */
const InfoBlock = ({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) => (
  <div className="flex items-center gap-2 p-2.5 bg-muted/30 rounded-lg">
    {icon}
    <div>
      <p className="text-[10px] text-muted-foreground leading-none">{label}</p>
      <p className="text-sm font-semibold mt-0.5">{value}</p>
    </div>
  </div>
);

export { RunDetailPanel };
export default RunDetailPanel;
