import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { RunStatus } from "@/lib/ops-control-plane";

const statusMap: Record<
  RunStatus,
  { label: string; className: string; businessLabel: string }
> = {
  QUEUED: { label: "QUEUED", businessLabel: "Aguardando início", className: "bg-slate-200 text-slate-900" },
  STARTING: { label: "STARTING", businessLabel: "Iniciando", className: "bg-blue-100 text-blue-800" },
  RUNNING: { label: "RUNNING", businessLabel: "Processando", className: "bg-blue-200 text-blue-900" },
  SUCCEEDED: { label: "SUCCEEDED", businessLabel: "Concluído com sucesso", className: "bg-green-200 text-green-900" },
  FAILED: { label: "FAILED", businessLabel: "Falhou", className: "bg-red-200 text-red-900" },
  CANCELLED: { label: "CANCELLED", businessLabel: "Cancelado", className: "bg-zinc-200 text-zinc-900" },
  TIMED_OUT: { label: "TIMED_OUT", businessLabel: "Tempo excedido", className: "bg-orange-200 text-orange-900" },
  SUCCEEDED_WITH_ISSUES: {
    label: "SUCCEEDED_WITH_ISSUES",
    businessLabel: "Concluído com alerta",
    className: "bg-amber-200 text-amber-900",
  },
  ORPHANED: { label: "ORPHANED", businessLabel: "Execução órfã", className: "bg-orange-200 text-orange-900" },
  INCONSISTENT: { label: "INCONSISTENT", businessLabel: "Estado divergente", className: "bg-violet-200 text-violet-900" },
};

export function RunStatusBadge({ status, showTechnical = true }: { status: RunStatus; showTechnical?: boolean }) {
  const cfg = statusMap[status];
  return (
    <Badge className={cn("font-medium", cfg.className)}>
      {cfg.businessLabel}
      {showTechnical ? ` (${cfg.label})` : ""}
    </Badge>
  );
}

