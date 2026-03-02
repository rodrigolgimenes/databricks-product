import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

export function RiskIndicator({ riskScore }: { riskScore: number }) {
  const level = riskScore >= 75 ? "Alto" : riskScore >= 45 ? "Médio" : "Baixo";
  const color =
    riskScore >= 75 ? "text-red-700 bg-red-100" : riskScore >= 45 ? "text-amber-700 bg-amber-100" : "text-green-700 bg-green-100";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={`inline-flex items-center rounded px-2 py-1 text-xs font-semibold ${color}`}>
          Risk {riskScore} ({level})
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">
        <p className="text-xs">O que é: score de risco operacional atual.</p>
        <p className="text-xs">Por que importa: ajuda a priorizar triagem e decidir replay.</p>
        <p className="text-xs">Quando agir: risco médio/alto em job crítico.</p>
      </TooltipContent>
    </Tooltip>
  );
}

export function BaselineComparison({
  currentValue,
  p95Value,
  unit,
}: {
  currentValue: number;
  p95Value: number;
  unit: "ms" | "rows";
}) {
  const deviation = p95Value > 0 ? ((currentValue - p95Value) / p95Value) * 100 : 0;
  const trend = deviation > 0 ? "+" : "";
  const warning = deviation > 15 ? "text-amber-700" : "text-green-700";
  return (
    <div className="text-xs">
      <p>
        Atual: <span className="font-semibold">{Math.round(currentValue).toLocaleString("pt-BR")} {unit}</span>
      </p>
      <p className="text-muted-foreground">
        Baseline p95: {Math.round(p95Value).toLocaleString("pt-BR")} {unit}
      </p>
      <p className={warning}>Desvio: {trend}{deviation.toFixed(1)}%</p>
    </div>
  );
}

