import { useCallback, useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  RefreshCw,
  ShieldCheck,
  AlertTriangle,
  Clock,
  Activity,
  HelpCircle,
  CheckCircle2,
  ArrowRight,
  HeartPulse,
} from "lucide-react";
import * as api from "@/lib/api";

// ─── Types ───────────────────────────────────────────────────────────────────

interface SreDashboardTabProps {
  pollingInterval: number;
  isActive: boolean;
}

type HealthLevel = "healthy" | "warning" | "critical" | "unknown";

interface HealthResult {
  level: HealthLevel;
  label: string;
  reason: string;
}

// ─── Constants ───────────────────────────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  PENDING: "bg-gray-400",
  CLAIMED: "bg-blue-300",
  RUNNING: "bg-blue-600",
  SUCCEEDED: "bg-green-500",
  FAILED: "bg-red-500",
};

const STATUS_TEXT_COLORS: Record<string, string> = {
  PENDING: "text-gray-600",
  CLAIMED: "text-blue-500",
  RUNNING: "text-blue-700",
  SUCCEEDED: "text-green-600",
  FAILED: "text-red-600",
};

const STATUS_TOOLTIPS: Record<string, string> = {
  PENDING: "Tarefas aguardando início. Se este número cresce, pode haver gargalo no processamento.",
  CLAIMED: "Tarefas reservadas por um worker, mas que ainda não iniciaram execução.",
  RUNNING: "Tarefas em execução neste momento.",
  SUCCEEDED: "Tarefas concluídas com sucesso.",
  FAILED: "Tarefas que falharam durante a execução.",
};

const KPI_TOOLTIPS: Record<string, string> = {
  total: "Quantidade total de tarefas na fila de processamento.",
  pending: "Tarefas aguardando início. Se este número cresce, pode haver gargalo no processamento.",
  claimed: "Tarefas reservadas por um worker, mas que ainda não iniciaram execução.",
  running: "Tarefas em execução neste momento.",
  succeeded: "Tarefas concluídas com sucesso.",
  failed: "Tarefas que falharam durante a execução.",
  stale: "Tarefas reservadas há mais de 30 minutos sem progresso. Pode indicar worker travado.",
  claim_success: "Total de tarefas que foram reservadas com sucesso nas últimas 6 horas.",
};

const LATENCY_TOOLTIPS: Record<string, string> = {
  p50_claim: "Metade das tarefas foram reservadas em até este tempo. Indica a velocidade típica.",
  p95_claim: "95% das tarefas foram reservadas em até este tempo. Valores altos indicam lentidão.",
  p50_running: "Metade das tarefas iniciaram execução em até este tempo após reserva.",
  p95_running: "95% das tarefas iniciaram execução em até este tempo após reserva.",
};

const HEALTH_CONFIG: Record<HealthLevel, { bg: string; border: string; text: string; icon: string }> = {
  healthy: { bg: "bg-green-50", border: "border-green-300", text: "text-green-800", icon: "text-green-600" },
  warning: { bg: "bg-yellow-50", border: "border-yellow-300", text: "text-yellow-800", icon: "text-yellow-600" },
  critical: { bg: "bg-red-50", border: "border-red-300", text: "text-red-800", icon: "text-red-600" },
  unknown: { bg: "bg-gray-50", border: "border-gray-300", text: "text-gray-600", icon: "text-gray-500" },
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

const fmtNum = (v: any) => {
  const n = Number(v);
  return Number.isFinite(n) ? n.toLocaleString("pt-BR") : "0";
};

const fmtMs = (v: any): string => {
  const n = Number(v);
  if (!Number.isFinite(n) || v == null) return "--";
  if (n < 1000) return `${Math.round(n)} ms`;
  return `${(n / 1000).toFixed(1)} s`;
};

const fmtDuration = (seconds: number): string => {
  if (!Number.isFinite(seconds) || seconds < 0) return "--";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm}m`;
};

const latencyColor = (ms: any): string => {
  const n = Number(ms);
  if (!Number.isFinite(n) || ms == null) return "text-gray-400";
  if (n < 5000) return "text-green-600";
  if (n < 30000) return "text-yellow-600";
  return "text-red-600";
};

const ageColor = (seconds: number): string => {
  if (seconds < 300) return "bg-yellow-100 text-yellow-800";
  if (seconds < 1800) return "bg-orange-100 text-orange-800";
  return "bg-red-100 text-red-800";
};

function computeHealth(
  staleClaims: number,
  latencyP95: number | null,
  failed: number,
  pending: number,
  running: number,
): HealthResult {
  if (staleClaims > 0)
    return { level: "critical", label: "Crítico", reason: `${staleClaims} tarefa(s) travada(s) (CLAIMED sem progresso)` };
  if (latencyP95 != null && latencyP95 > 60000)
    return { level: "critical", label: "Crítico", reason: `Latência p95 muito alta: ${fmtMs(latencyP95)}` };
  if (failed > 5)
    return { level: "critical", label: "Crítico", reason: `${failed} falhas na fila` };
  if (failed > 0)
    return { level: "warning", label: "Atenção", reason: `${failed} falha(s) na fila` };
  if (latencyP95 != null && latencyP95 > 30000)
    return { level: "warning", label: "Atenção", reason: `Latência p95 elevada: ${fmtMs(latencyP95)}` };
  if (running === 0 && pending > 0)
    return { level: "warning", label: "Atenção", reason: `${pending} tarefa(s) pendente(s) sem worker ativo` };
  if (running > 0 && pending > running * 3)
    return { level: "warning", label: "Atenção", reason: `Fila acumulando: ${pending} pendentes vs ${running} executando` };
  if (latencyP95 == null)
    return { level: "unknown", label: "Sem dados suficientes", reason: "Sem dados de latência para avaliar saúde — aguardando execuções" };
  return { level: "healthy", label: "Saudável", reason: "Fila operando normalmente" };
}

// ─── Sub-components ──────────────────────────────────────────────────────────

function InfoTip({ text }: { text: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <HelpCircle className="h-3.5 w-3.5 text-muted-foreground cursor-help inline-block ml-1" />
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs text-xs">
        {text}
      </TooltipContent>
    </Tooltip>
  );
}

function KpiCard({
  label,
  value,
  tooltip,
  highlight,
}: {
  label: string;
  value: string;
  tooltip: string;
  highlight?: string;
}) {
  return (
    <Card className={highlight || ""}>
      <CardContent className="p-4">
        <p className="text-xs text-muted-foreground flex items-center">
          {label}
          <InfoTip text={tooltip} />
        </p>
        <p className="text-xl font-bold">{value}</p>
      </CardContent>
    </Card>
  );
}

function PipelineStep({
  label,
  count,
  colorClass,
  isLast,
}: {
  label: string;
  count: number;
  colorClass: string;
  isLast?: boolean;
}) {
  return (
    <>
      <div className="flex flex-col items-center gap-1">
        <div className={`w-10 h-10 rounded-full flex items-center justify-center text-white text-sm font-bold ${colorClass}`}>
          {count}
        </div>
        <span className="text-xs text-muted-foreground font-medium">{label}</span>
      </div>
      {!isLast && <ArrowRight className="h-4 w-4 text-muted-foreground mt-[-0.75rem]" />}
    </>
  );
}

// ─── Main Component ──────────────────────────────────────────────────────────

export function SreDashboardTab({ pollingInterval, isActive }: SreDashboardTabProps) {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      setError(null);
      const res = await api.getSreDashboard(120);
      setData(res);
    } catch (e: any) {
      setError(e?.message || "Erro ao carregar dashboard SRE");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!isActive) return;
    setLoading(true);
    fetchData();
  }, [fetchData, isActive]);

  useEffect(() => {
    if (!isActive) return;
    const t = setInterval(fetchData, pollingInterval);
    return () => clearInterval(t);
  }, [fetchData, isActive, pollingInterval]);

  // ─── Derived values (granular deps for memo efficiency) ─────────────────

  const summary = data?.summary || {};
  const latency = data?.latency || {};
  const timeline = data?.transition_timeline || [];
  const staleAll = data?.stale_claim_candidates || [];

  const totalItems = Number(summary.total_items || 0);
  const pendingCount = Number(summary.pending_count || 0);
  const claimedCount = Number(summary.claimed_count || 0);
  const runningCount = Number(summary.running_count || 0);
  const succeededCount = Number(summary.succeeded_count || 0);
  const failedCount = Number(summary.failed_count || 0);
  const staleClaimedCount = Number(summary.stale_claimed_count || 0);

  const p50Claim = latency.p50_time_to_claim_ms;
  const p95Claim = latency.p95_time_to_claim_ms;
  const p50Running = latency.p50_time_claimed_to_running_ms;
  const p95Running = latency.p95_time_claimed_to_running_ms;

  const claimSuccess6h = useMemo(
    () => timeline.reduce((a: number, x: any) => a + Number(x.claim_success_count || 0), 0),
    [timeline],
  );

  const health = useMemo(
    () => computeHealth(staleClaimedCount, p95Claim != null ? Number(p95Claim) : null, failedCount, pendingCount, runningCount),
    [staleClaimedCount, p95Claim, failedCount, pendingCount, runningCount],
  );

  const realtimeByStatus = useMemo(() => {
    const m: Record<string, any> = {};
    for (const r of data?.realtime || []) m[String(r.status || "UNKNOWN")] = r;
    return m;
  }, [data?.realtime]);

  const realtimeTotal = useMemo(
    () => Object.values(realtimeByStatus).reduce((a: number, r: any) => a + Number(r.item_count || 0), 0),
    [realtimeByStatus],
  );

  const stale = useMemo(() => staleAll.slice(0, 50), [staleAll]);

  const hc = HEALTH_CONFIG[health.level];

  return (
    <TooltipProvider delayDuration={200}>
      <div className="space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="text-sm text-muted-foreground">
            Saúde da Fila de Processamento — visão em tempo real
          </div>
          <Button variant="outline" size="sm" onClick={fetchData} disabled={loading}>
            <RefreshCw className={`h-4 w-4 mr-1 ${loading ? "animate-spin" : ""}`} /> Atualizar
          </Button>
        </div>

        {/* Error */}
        {error && (
          <Card className="border-red-200">
            <CardContent className="p-4 text-sm text-red-700">{error}</CardContent>
          </Card>
        )}

        {/* Health Summary Card */}
        <Card className={`${hc.border} ${hc.bg}`}>
          <CardContent className="p-4 flex items-center gap-3">
            <HeartPulse className={`h-6 w-6 ${hc.icon}`} />
            <div>
              <p className={`font-semibold ${hc.text}`}>{health.label}</p>
              <p className={`text-sm ${hc.text} opacity-80`}>{health.reason}</p>
            </div>
            <InfoTip text="Diagnóstico automático baseado em: tarefas travadas, latência p95, falhas na fila e proporção pending/running." />
          </CardContent>
        </Card>

        {/* Pipeline Diagram */}
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-muted-foreground mb-3 flex items-center">
              Fluxo de Vida da Tarefa
              <InfoTip text="Cada tarefa passa por estes estágios: aguardando → reservada → executando → concluída ou falha." />
            </p>
            <div className="flex items-center justify-center gap-3 flex-wrap">
              <PipelineStep label="Pending" count={pendingCount} colorClass="bg-gray-400" />
              <PipelineStep label="Claimed" count={claimedCount} colorClass="bg-blue-400" />
              <PipelineStep label="Running" count={runningCount} colorClass="bg-blue-600" />
              <div className="flex gap-4">
                <div className="flex items-center gap-3">
                  <ArrowRight className="h-4 w-4 text-muted-foreground mt-[-0.75rem]" />
                  <div className="flex flex-col items-center gap-1">
                    <div className="flex gap-2">
                      <div className="flex flex-col items-center gap-1">
                        <div className="w-10 h-10 rounded-full flex items-center justify-center text-white text-sm font-bold bg-green-500">
                          {succeededCount}
                        </div>
                        <span className="text-xs text-muted-foreground font-medium">OK</span>
                      </div>
                      <div className="flex flex-col items-center gap-1">
                        <div className="w-10 h-10 rounded-full flex items-center justify-center text-white text-sm font-bold bg-red-500">
                          {failedCount}
                        </div>
                        <span className="text-xs text-muted-foreground font-medium">Falha</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* KPI Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
          <KpiCard label="Total" value={fmtNum(totalItems)} tooltip={KPI_TOOLTIPS.total} />
          <KpiCard label="Pending" value={fmtNum(pendingCount)} tooltip={KPI_TOOLTIPS.pending} />
          <KpiCard label="Claimed" value={fmtNum(claimedCount)} tooltip={KPI_TOOLTIPS.claimed} />
          <KpiCard label="Running" value={fmtNum(runningCount)} tooltip={KPI_TOOLTIPS.running} />
          <KpiCard label="Succeeded" value={fmtNum(succeededCount)} tooltip={KPI_TOOLTIPS.succeeded} />
          <KpiCard label="Failed" value={fmtNum(failedCount)} tooltip={KPI_TOOLTIPS.failed} highlight={failedCount > 0 ? "border-red-300" : ""} />
          <KpiCard label="Stale CLAIMED" value={fmtNum(staleClaimedCount)} tooltip={KPI_TOOLTIPS.stale} highlight={staleClaimedCount > 0 ? "border-red-300" : ""} />
          <KpiCard label="Claim Success (6h)" value={fmtNum(claimSuccess6h)} tooltip={KPI_TOOLTIPS.claim_success} />
        </div>

        {/* Latency + Realtime */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* Latency */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <ShieldCheck className="h-4 w-4" /> Latência de Claim (24h)
                <InfoTip text="Mede quanto tempo as tarefas levam para serem processadas. Verde = rápido, amarelo = normal, vermelho = lento." />
              </CardTitle>
            </CardHeader>
            <CardContent className="text-sm space-y-2">
              {([
                { key: "p50_claim", label: "p50 time_to_claim", value: p50Claim, tip: LATENCY_TOOLTIPS.p50_claim },
                { key: "p95_claim", label: "p95 time_to_claim", value: p95Claim, tip: LATENCY_TOOLTIPS.p95_claim },
                { key: "p50_running", label: "p50 claimed→running", value: p50Running, tip: LATENCY_TOOLTIPS.p50_running },
                { key: "p95_running", label: "p95 claimed→running", value: p95Running, tip: LATENCY_TOOLTIPS.p95_running },
              ] as const).map(({ key, label, value, tip }) => (
                <div key={key} className="flex justify-between items-center">
                  <span className="flex items-center">
                    {label}
                    <InfoTip text={tip} />
                  </span>
                  <Badge variant="secondary" className={latencyColor(value)}>
                    {fmtMs(value)}
                  </Badge>
                </div>
              ))}
            </CardContent>
          </Card>

          {/* Realtime by Status */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <Activity className="h-4 w-4" /> Realtime por Status
                <InfoTip text="Distribuição atual das tarefas por status. A barra mostra a proporção relativa." />
              </CardTitle>
            </CardHeader>
            <CardContent className="text-sm space-y-3">
              {Object.keys(realtimeByStatus).length === 0 ? (
                <p className="text-muted-foreground">Sem dados</p>
              ) : (
                Object.entries(realtimeByStatus).map(([status, row]: [string, any]) => {
                  const count = Number(row.item_count || 0);
                  const pct = realtimeTotal > 0 ? (count / realtimeTotal) * 100 : 0;
                  return (
                    <Tooltip key={status}>
                      <TooltipTrigger asChild>
                        <div className="space-y-1 cursor-default">
                          <div className="flex items-center justify-between">
                            <span className={`font-medium ${STATUS_TEXT_COLORS[status] || "text-gray-600"}`}>
                              {status}
                            </span>
                            <div className="flex items-center gap-2">
                              <Badge variant="outline">{fmtNum(count)}</Badge>
                              {Number(row.stale_claimed_count || 0) > 0 && (
                                <Badge className="bg-red-600 text-white">stale {fmtNum(row.stale_claimed_count)}</Badge>
                              )}
                            </div>
                          </div>
                          <div className="w-full bg-gray-100 rounded-full h-2">
                            <div
                              className={`h-2 rounded-full transition-all ${STATUS_COLORS[status] || "bg-gray-400"}`}
                              style={{ width: `${Math.max(pct, 1)}%` }}
                            />
                          </div>
                        </div>
                      </TooltipTrigger>
                      <TooltipContent side="top" className="max-w-xs text-xs">
                        {STATUS_TOOLTIPS[status] || `Status: ${status}`} — {pct.toFixed(1)}% do total
                      </TooltipContent>
                    </Tooltip>
                  );
                })
              )}
            </CardContent>
          </Card>
        </div>

        {/* Stale / Orphan Claims */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <AlertTriangle className="h-4 w-4" /> Tarefas Travadas (Status: CLAIMED sem progresso)
              <InfoTip text="Tarefas que foram reservadas por um worker mas não progrediram. Podem precisar de reinício manual." />
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="max-h-72 overflow-auto">
              <table className="w-full text-sm">
                <thead className="bg-muted/50 sticky top-0">
                  <tr>
                    <th className="text-left p-2">
                      Queue ID
                      <InfoTip text="Identificador único da tarefa na fila." />
                    </th>
                    <th className="text-left p-2">
                      Dataset
                      <InfoTip text="Identificador do dataset sendo processado." />
                    </th>
                    <th className="text-left p-2">
                      Claim Owner
                      <InfoTip text="Worker que reservou esta tarefa." />
                    </th>
                    <th className="text-right p-2">
                      Tempo Parado
                      <InfoTip text="Há quanto tempo a tarefa está parada sem progresso." />
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {stale.length === 0 ? (
                    <tr>
                      <td className="p-3 text-muted-foreground" colSpan={4}>
                        <CheckCircle2 className="h-4 w-4 inline mr-1 text-green-500" />
                        Nenhuma tarefa travada no momento
                      </td>
                    </tr>
                  ) : (
                    stale.map((r: any) => {
                      const ageSec = Number(r.claim_age_seconds || 0);
                      return (
                        <tr key={r.queue_id} className="border-t">
                          <td className="p-2 font-mono text-xs">
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span className="cursor-help">{String(r.queue_id).slice(0, 12)}…</span>
                              </TooltipTrigger>
                              <TooltipContent className="text-xs font-mono">{r.queue_id}</TooltipContent>
                            </Tooltip>
                          </td>
                          <td className="p-2 font-mono text-xs">
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span className="cursor-help">{String(r.dataset_id).slice(0, 12)}…</span>
                              </TooltipTrigger>
                              <TooltipContent className="text-xs font-mono">{r.dataset_id}</TooltipContent>
                            </Tooltip>
                          </td>
                          <td className="p-2 text-xs">{r.claim_owner || "—"}</td>
                          <td className="p-2 text-right">
                            <Badge className={ageColor(ageSec)}>
                              <Clock className="h-3 w-3 mr-1" />
                              {fmtDuration(ageSec)}
                            </Badge>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
              {staleAll.length > 50 && (
                <p className="text-xs text-muted-foreground p-2 text-center border-t">
                  Mostrando os 50 mais antigos de {staleAll.length} total
                </p>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </TooltipProvider>
  );
}

