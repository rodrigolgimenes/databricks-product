import {
  RefreshCw, GitMerge, ListPlus, Package, BarChart3,
  ShieldCheck, ShieldAlert, ShieldQuestion, Clock, User,
  AlertTriangle, TrendingDown, TrendingUp, Minus,
} from "lucide-react";
import type {
  RunInterpretation,
  ProcessingIcon,
  RiskLevel,
  VariationSeverity,
} from "@/lib/run-interpretation";

/* ── helpers ─────────────────────────────────────────── */

const fmtNum = (n: number | null | undefined) => {
  if (n == null) return "—";
  return Number(n).toLocaleString("pt-BR");
};

const formatDuration = (seconds: number | null | undefined) => {
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
  ts ? new Date(ts).toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" }) : "—";

const processingIcon = (icon: ProcessingIcon) => {
  switch (icon) {
    case "refresh": return <RefreshCw className="h-4 w-4" />;
    case "delta": return <GitMerge className="h-4 w-4" />;
    case "append": return <ListPlus className="h-4 w-4" />;
  }
};

const riskColor: Record<RiskLevel, { bg: string; border: string; text: string; icon: typeof ShieldCheck }> = {
  none: { bg: "bg-green-50", border: "border-green-200", text: "text-green-700", icon: ShieldCheck },
  low: { bg: "bg-blue-50", border: "border-blue-200", text: "text-blue-700", icon: ShieldQuestion },
  medium: { bg: "bg-amber-50", border: "border-amber-200", text: "text-amber-700", icon: ShieldAlert },
  high: { bg: "bg-red-50", border: "border-red-200", text: "text-red-700", icon: ShieldAlert },
};

const severityColor: Record<VariationSeverity, string> = {
  normal: "text-green-700",
  warning: "text-amber-700",
  critical: "text-red-700",
};

const variationIcon = (pct: number | null) => {
  if (pct == null) return <Minus className="h-3 w-3 text-muted-foreground" />;
  if (pct < 0) return <TrendingDown className="h-3 w-3" />;
  if (pct > 0) return <TrendingUp className="h-3 w-3" />;
  return <Minus className="h-3 w-3" />;
};

/* ── types ────────────────────────────────────────────── */

interface RunExecutiveSummaryProps {
  interpretation: RunInterpretation;
  durationSeconds?: number | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  triggerType?: string | null;
  requestedBy?: string | null;
}

/* ── component ───────────────────────────────────────── */

const RunExecutiveSummary = ({
  interpretation,
  durationSeconds,
  startedAt,
  finishedAt,
  triggerType,
  requestedBy,
}: RunExecutiveSummaryProps) => {
  const { processingType, result, impact, risk } = interpretation;
  const rc = riskColor[risk.level];
  const RiskIcon = rc.icon;

  return (
    <div className="rounded-lg border bg-card p-4 space-y-4">
      {/* ── Processing Type header ── */}
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 mt-0.5 p-2 rounded-lg bg-primary/10 text-primary">
          {processingIcon(processingType.icon)}
        </div>
        <div className="min-w-0">
          <p className="font-semibold text-sm">{processingType.label}</p>
          <p className="text-xs text-muted-foreground mt-0.5">{processingType.reason}</p>
        </div>
      </div>

      {/* ── 3-column cards: Resultado, Impacto, Risco ── */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {/* Resultado */}
        <div className="p-3 rounded-lg bg-muted/30 space-y-1">
          <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
            <Package className="h-3.5 w-3.5" />
            Resultado
          </div>
          <p className="text-sm font-semibold">{result.summary}</p>
          <p className="text-xs text-muted-foreground">{result.detail}</p>
          {result.countNote && (
            <p className="text-[11px] text-muted-foreground/70 italic">{result.countNote}</p>
          )}
        </div>

        {/* Impacto */}
        <div className="p-3 rounded-lg bg-muted/30 space-y-1">
          <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
            <BarChart3 className="h-3.5 w-3.5" />
            Impacto de Dados
          </div>
          {impact.previousCount != null ? (
            <>
              <div className="flex items-baseline gap-2 text-xs">
                <span className="text-muted-foreground">Anterior:</span>
                <span className="font-mono font-medium">{fmtNum(impact.previousCount)}</span>
              </div>
              <div className="flex items-baseline gap-2 text-xs">
                <span className="text-muted-foreground">Atual:</span>
                <span className="font-mono font-medium">{fmtNum(impact.currentCount)}</span>
              </div>
              <div className={`flex items-center gap-1 text-xs font-semibold ${severityColor[impact.variationSeverity]}`}>
                {variationIcon(impact.variationPercent)}
                Variação: {impact.variationLabel}
              </div>
            </>
          ) : (
            <p className="text-xs text-muted-foreground">{impact.variationLabel}</p>
          )}
        </div>

        {/* Risco */}
        <div className={`p-3 rounded-lg ${rc.bg} ${rc.border} border space-y-1`}>
          <div className={`flex items-center gap-1.5 text-xs font-medium ${rc.text}`}>
            <RiskIcon className="h-3.5 w-3.5" />
            Risco Operacional
          </div>
          <p className={`text-sm font-semibold ${rc.text}`}>{risk.label}</p>
          {risk.details.length > 0 && (
            <ul className="space-y-0.5">
              {risk.details.map((d, i) => (
                <li key={i} className={`text-[11px] ${rc.text} flex items-start gap-1`}>
                  <AlertTriangle className="h-3 w-3 flex-shrink-0 mt-0.5" />
                  {d}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      {/* ── Footer: duração + timestamps + trigger ── */}
      <div className="flex items-center gap-4 text-xs text-muted-foreground flex-wrap">
        {durationSeconds != null && (
          <span className="flex items-center gap-1">
            <Clock className="h-3 w-3" />
            {formatDuration(durationSeconds)}
          </span>
        )}
        {startedAt && finishedAt && (
          <span>{formatTs(startedAt)} → {formatTs(finishedAt)}</span>
        )}
        {(triggerType || requestedBy) && (
          <span className="flex items-center gap-1">
            <User className="h-3 w-3" />
            {triggerType === "MANUAL" ? "Manual" : triggerType === "SCHEDULE" ? "Agendado" : triggerType || "—"}
            {requestedBy ? ` / ${requestedBy}` : ""}
          </span>
        )}
      </div>
    </div>
  );
};

export { RunExecutiveSummary };
export default RunExecutiveSummary;
