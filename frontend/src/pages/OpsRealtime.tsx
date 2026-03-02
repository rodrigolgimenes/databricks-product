import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { AlertTriangle, RefreshCw, ExternalLink, ChevronDown, ChevronRight } from "lucide-react";
import { RunStatusBadge } from "@/components/ops/RunStatusBadge";
import { BaselineComparison, RiskIndicator } from "@/components/ops/OpsMetrics";
import { ViewModeToggle } from "@/components/ops/ViewModeToggle";
import { OpsTooltip, opsTooltips } from "@/components/ops/OpsTooltip";
import { useViewMode } from "@/contexts/ViewModeContext";
import type { OpsRunRealtimeItem } from "@/lib/ops-control-plane";
import * as api from "@/lib/api";

const criticalityWeight: Record<string, number> = { T0: 4, T1: 3, T2: 2, T3: 1 };

const priorityScore = (item: OpsRunRealtimeItem) =>
  (item.needsAttention ? 100 : 0) +
  item.nearMissRiskPercent +
  criticalityWeight[item.criticalityTier] * 10;

const fmtMs = (ms: number) => `${Math.round(ms / 1000)}s`;

const statusMeaning: Record<string, string> = {
  RUNNING: "O processamento está em andamento.",
  SUCCEEDED: "A execução terminou sem problemas.",
  FAILED: "A execução falhou. Pode haver atraso na atualização dos dados.",
  SUCCEEDED_WITH_ISSUES: "A execução terminou, mas houve um comportamento inesperado.",
  ORPHANED: "Execução interrompida inesperadamente.",
  INCONSISTENT: "Divergência entre estado interno e sistema.",
  CANCELLED: "A execução foi cancelada.",
  TIMED_OUT: "A execução excedeu o tempo máximo.",
  QUEUED: "Aguardando para iniciar.",
  STARTING: "Preparando para executar.",
};

/* ── Business card: progressive disclosure layers 1-2 ── */
const RunCardBusiness = ({ run }: { run: OpsRunRealtimeItem }) => {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className={`rounded border p-3 ${run.needsAttention ? "border-red-200 bg-red-50" : ""}`}>
      <div className="flex items-center justify-between gap-2">
        <div>
          <p className="font-semibold text-sm">{run.jobName}</p>
          <p className="text-xs text-muted-foreground">{run.domainId}</p>
        </div>
        <RunStatusBadge status={run.status} showTechnical={false} />
      </div>
      <p className="text-xs text-muted-foreground mt-1">{statusMeaning[run.status] ?? ""}</p>
      {run.nearMissRiskPercent >= 45 && (
        <p className="text-xs text-amber-700 mt-1">⚠ Risco operacional detectado. Monitorar.</p>
      )}
      <button onClick={() => setExpanded(!expanded)} className="mt-2 inline-flex items-center gap-1 text-xs text-blue-700 hover:underline">
        {expanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        Ver detalhes
      </button>
      {expanded && (
        <div className="mt-2 space-y-1 text-xs border-t pt-2">
          <p><OpsTooltip label="Duração" {...opsTooltips.duration} />: <span className="font-semibold">{fmtMs(run.durationMs)}</span></p>
          <p>
            <OpsTooltip label="Watermark" {...opsTooltips.watermark} />:{" "}
            <span className="font-semibold">{run.watermarkBefore} → {run.watermarkAfter}</span>
            {run.watermarkBefore === run.watermarkAfter && <span className="text-amber-600 ml-1">(não avançou)</span>}
          </p>
          <p><OpsTooltip label="Volume" {...opsTooltips.volume} />: <span className="font-semibold">{run.recordsWritten.toLocaleString("pt-BR")} registros</span></p>
          <RiskIndicator riskScore={run.nearMissRiskPercent} />
        </div>
      )}
    </div>
  );
};

/* ── Engineering card: all data + layer 3 diagnostic ── */
const RunCardEngineering = ({ run }: { run: OpsRunRealtimeItem }) => {
  const [showDiag, setShowDiag] = useState(false);
  return (
    <div className={`rounded border p-3 ${run.needsAttention ? "border-red-200 bg-red-50" : ""}`}>
      <div className="flex items-center justify-between gap-2">
        <div>
          <p className="font-semibold text-sm">{run.jobName}</p>
          <p className="text-xs text-muted-foreground">{run.domainId} • {run.criticalityTier} • Run: {run.runId}</p>
        </div>
        <RunStatusBadge status={run.status} showTechnical />
      </div>
      <div className="mt-2 grid grid-cols-2 gap-2">
        <BaselineComparison currentValue={run.durationMs} p95Value={run.p95DurationMs} unit="ms" />
        <div className="text-xs">
          <p><OpsTooltip label="Watermark" {...opsTooltips.watermark} />: <span className="font-semibold">{run.watermarkBefore} → {run.watermarkAfter}</span></p>
          <p>Records: <span className="font-semibold">{run.recordsWritten.toLocaleString("pt-BR")}</span></p>
          <div className="mt-1"><RiskIndicator riskScore={run.nearMissRiskPercent} /></div>
        </div>
      </div>
      <button onClick={() => setShowDiag(!showDiag)} className="mt-2 inline-flex items-center gap-1 text-xs text-blue-700 hover:underline">
        {showDiag ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        Diagnóstico completo
      </button>
      {showDiag && (
        <div className="mt-2 text-xs border-t pt-2 space-y-1 font-mono bg-muted/30 rounded p-2">
          <p>run_id: {run.runId}</p>
          <p>portal_job_id: {run.portalJobId}</p>
          <p>duration_ms: {run.durationMs}</p>
          <p>p95_duration_ms: {run.p95DurationMs}</p>
          <p>watermark_before: {run.watermarkBefore}</p>
          <p>watermark_after: {run.watermarkAfter}</p>
          <p>records_written: {run.recordsWritten}</p>
          <p>near_miss_risk: {run.nearMissRiskPercent}%</p>
          <p>needs_attention: {String(run.needsAttention)}</p>
        </div>
      )}
      {run.sparkUiUrl && (
        <div className="mt-2">
          <a href={run.sparkUiUrl} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-blue-700 hover:underline">
            <ExternalLink className="h-3 w-3" /> Spark UI
          </a>
        </div>
      )}
    </div>
  );
};

const OpsRealtime = () => {
  const { isBusiness } = useViewMode();
  const [runs, setRuns] = useState<OpsRunRealtimeItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const fetchRealtime = async () => {
    setLoading(true);
    setError(false);
    try {
      const res = await api.getOpsRealtimeBoard({ limit: 50 });
      setRuns(res.items || []);
    } catch {
      setRuns([]);
      setError(true);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRealtime();
  }, []);

  const ordered = useMemo(
    () => [...runs].sort((a, b) => priorityScore(b) - priorityScore(a)),
    [runs]
  );
  const attentionNeeded = ordered.filter((r) => r.needsAttention);
  const running = ordered.filter((r) => r.status === "RUNNING");
  const RunCard = isBusiness ? RunCardBusiness : RunCardEngineering;

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">{isBusiness ? "Painel Operacional" : "Ops Realtime Board"}</h1>
          <p className="text-sm text-muted-foreground">
            {isBusiness ? "Status das execuções e alertas que precisam da sua atenção." : "Running now, atenção prioritária e risco operacional em tempo real."}
            {error ? " (erro ao carregar dados)" : ""}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <ViewModeToggle />
          <Button variant="outline" size="sm" onClick={fetchRealtime} disabled={loading}>
            <RefreshCw className={`mr-1 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Atualizar
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Card><CardContent className="p-4"><p className="text-xs text-muted-foreground">{isBusiness ? "Em processamento" : "Running"}</p><p className="text-2xl font-bold">{running.length}</p></CardContent></Card>
        <Card><CardContent className="p-4"><p className="text-xs text-muted-foreground">{isBusiness ? "Precisam atenção" : "Attention Needed"}</p><p className="text-2xl font-bold text-red-700">{attentionNeeded.length}</p></CardContent></Card>
        <Card><CardContent className="p-4"><p className="text-xs text-muted-foreground"><OpsTooltip label={isBusiness ? "Quase falha" : "Near Miss ≥ 60%"} {...opsTooltips.nearMiss} /></p><p className="text-2xl font-bold text-amber-700">{ordered.filter((r) => r.nearMissRiskPercent >= 60).length}</p></CardContent></Card>
        <Card><CardContent className="p-4"><p className="text-xs text-muted-foreground">{isBusiness ? "Jobs mais críticos" : "Jobs críticos T0/T1"}</p><p className="text-2xl font-bold">{ordered.filter((r) => ["T0", "T1"].includes(r.criticalityTier)).length}</p></CardContent></Card>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <Card>
          <CardHeader><CardTitle className="text-base">{isBusiness ? "Requer sua atenção" : "Attention Needed"}</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            {attentionNeeded.length === 0 && (
              <p className="text-sm text-muted-foreground">{isBusiness ? "Tudo certo! Nenhum problema no momento." : "Nenhuma execução crítica no momento."}</p>
            )}
            {attentionNeeded.map((run) => <RunCard key={run.runId} run={run} />)}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">{isBusiness ? "Processando agora" : "Running Now"}</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            {running.length === 0 && <p className="text-sm text-muted-foreground">{isBusiness ? "Nenhuma execução ativa." : "Sem runs ativos."}</p>}
            {running.map((run) => <RunCard key={run.runId} run={run} />)}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardContent className="p-4 text-xs text-muted-foreground">
          {isBusiness ? (
            <p>💡 Os itens são ordenados por importância. Para detalhes técnicos, alterne para <strong>Visão Engenharia</strong>.</p>
          ) : (
            <div className="flex items-start gap-2">
              <AlertTriangle className="h-4 w-4 mt-0.5 text-amber-600" />
              <p>Priorização calculada por <span className="font-semibold">criticidade + risco + necessidade de atenção</span>. Esse board implementa a visão Ops para reduzir MTTR.</p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default OpsRealtime;

