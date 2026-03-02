import { useState } from "react";
import { Brain, Lightbulb, Clock, ChevronDown, ChevronRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { DiagnosticInterpretation } from "@/lib/run-interpretation";

/* ── types ────────────────────────────────────────────── */

interface RunDiagnosticCardProps {
  diagnostic: DiagnosticInterpretation;
  /** Show strategy_decision_log detail (engineering mode) */
  showDecisionLog?: boolean;
}

/* ── helpers ──────────────────────────────────────────── */

const formatLogTs = (ts: string) => {
  try {
    return new Date(ts).toLocaleString("pt-BR", {
      day: "2-digit", month: "2-digit", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch {
    return ts;
  }
};

const statusBadgeVariant = (status: string) => {
  const s = status.toLowerCase();
  if (s.includes("aguardando") || s.includes("pending")) return "secondary" as const;
  if (s.includes("confirmado") || s.includes("confirmed")) return "default" as const;
  if (s.includes("rejeitado") || s.includes("rejected")) return "destructive" as const;
  return "outline" as const;
};

/* ── component ───────────────────────────────────────── */

const RunDiagnosticCard = ({ diagnostic, showDecisionLog = false }: RunDiagnosticCardProps) => {
  const [logExpanded, setLogExpanded] = useState(false);
  const hasReasons = diagnostic.reasons.length > 0;
  const hasSuggestion = !!diagnostic.suggestion;
  const hasLog = showDecisionLog && diagnostic.decisionLog && diagnostic.decisionLog.length > 0;

  // Não renderizar se não há conteúdo
  if (!hasReasons && !hasSuggestion) return null;

  return (
    <div className="rounded-lg border border-blue-200 bg-blue-50/50 p-4 space-y-3">
      {/* ── Header ── */}
      <div className="flex items-center gap-2">
        <Brain className="h-4 w-4 text-blue-600" />
        <span className="font-semibold text-sm text-blue-800">Diagnóstico Automático</span>
        <Badge variant={statusBadgeVariant(diagnostic.status)} className="text-[10px] ml-auto">
          {diagnostic.status}
        </Badge>
      </div>

      {/* ── Reasons ── */}
      {hasReasons && (
        <div>
          <p className="text-xs text-blue-700 mb-1.5">
            Esta tabela não suporta carga incremental porque:
          </p>
          <ul className="space-y-1">
            {diagnostic.reasons.map((r, i) => (
              <li key={i} className="text-xs text-blue-800 flex items-start gap-2">
                <span className="text-blue-400 mt-0.5">•</span>
                {r}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* ── Suggestion ── */}
      {hasSuggestion && (
        <div className="flex items-start gap-2 pt-2 border-t border-blue-200">
          <Lightbulb className="h-3.5 w-3.5 text-amber-500 flex-shrink-0 mt-0.5" />
          <p className="text-xs text-blue-800">{diagnostic.suggestion}</p>
        </div>
      )}

      {/* ── Decision Log (engineering only) ── */}
      {hasLog && (
        <div className="pt-2 border-t border-blue-200">
          <button
            className="flex items-center gap-1.5 text-xs font-medium text-blue-600 hover:text-blue-800 transition-colors"
            onClick={() => setLogExpanded(!logExpanded)}
          >
            <Clock className="h-3 w-3" />
            Decision Log ({diagnostic.decisionLog!.length})
            {logExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
          </button>

          {logExpanded && (
            <div className="mt-2 space-y-2">
              {diagnostic.decisionLog!.map((entry, i) => (
                <div key={i} className="p-2 bg-blue-100/50 rounded text-[11px] font-mono space-y-0.5">
                  <div className="flex items-center gap-2 text-blue-800">
                    <span className="font-semibold">{formatLogTs(entry.timestamp)}</span>
                    <span className="text-blue-600">{entry.event}</span>
                    <span>→</span>
                    <Badge variant="outline" className="text-[10px]">{entry.strategy}</Badge>
                  </div>
                  {entry.inputs && (
                    <div className="text-blue-700 space-x-3">
                      {entry.inputs.pk_cols != null && (
                        <span>pk_cols=[{Array.isArray(entry.inputs.pk_cols) ? entry.inputs.pk_cols.join(", ") : String(entry.inputs.pk_cols)}]</span>
                      )}
                      {entry.inputs.pk_confidence != null && (
                        <span>pk_confidence={entry.inputs.pk_confidence}</span>
                      )}
                      {entry.inputs.watermark_valid != null && (
                        <span>watermark_valid={String(entry.inputs.watermark_valid)}</span>
                      )}
                      {entry.inputs.table_size_rows != null && (
                        <span>table_size_rows={Number(entry.inputs.table_size_rows).toLocaleString("pt-BR")}</span>
                      )}
                    </div>
                  )}
                  {entry.reason && (
                    <div className="text-blue-600 italic">{entry.reason}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export { RunDiagnosticCard };
export default RunDiagnosticCard;
