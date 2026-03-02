import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { HelpCircle } from "lucide-react";
import type { ReactNode } from "react";

interface OpsTooltipProps {
  /** Short label shown next to the icon */
  label?: string;
  /** What is this field? */
  whatIs: string;
  /** Why does it matter? */
  whyMatters: string;
  /** When should the user worry? */
  whenToWorry: string;
  /** Wrap around children or show inline icon */
  children?: ReactNode;
}

export function OpsTooltip({ label, whatIs, whyMatters, whenToWorry, children }: OpsTooltipProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        {children ?? (
          <span className="inline-flex items-center gap-1 cursor-help text-xs text-muted-foreground">
            {label && <span>{label}</span>}
            <HelpCircle className="h-3 w-3" />
          </span>
        )}
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs space-y-1.5 p-3">
        <p className="text-xs font-semibold">O que é</p>
        <p className="text-xs text-muted-foreground">{whatIs}</p>
        <p className="text-xs font-semibold mt-1">Por que importa</p>
        <p className="text-xs text-muted-foreground">{whyMatters}</p>
        <p className="text-xs font-semibold mt-1">Quando se preocupar</p>
        <p className="text-xs text-muted-foreground">{whenToWorry}</p>
      </TooltipContent>
    </Tooltip>
  );
}

/** Predefined tooltip definitions for common Ops fields */
export const opsTooltips = {
  duration: {
    whatIs: "Tempo total entre o início e o término do processamento.",
    whyMatters: "Se a duração aumentar muito, pode indicar lentidão ou risco de atraso na entrega dos dados.",
    whenToWorry: "Se ultrapassar o valor esperado (p95), pode haver risco de falha ou atraso.",
  },
  watermark: {
    whatIs: "A data mais recente de dado processado.",
    whyMatters: "Garante que os dados estão sendo atualizados corretamente.",
    whenToWorry: "Se o valor não avançar após uma execução, pode indicar falha silenciosa.",
  },
  nearMiss: {
    whatIs: "A execução terminou com sucesso, mas apresentou comportamento fora do padrão esperado.",
    whyMatters: "Exemplo: demorou muito mais que o normal ou processou volume incomum.",
    whenToWorry: "Monitorar. Se ocorrer com frequência, investigar.",
  },
  riskScore: {
    whatIs: "Score de risco operacional calculado com base em criticidade, baseline e anomalias.",
    whyMatters: "Ajuda a priorizar triagem e decidir se replay é necessário.",
    whenToWorry: "Risco médio/alto (>45) em job crítico (T0/T1).",
  },
  volume: {
    whatIs: "Quantidade de registros escritos na execução.",
    whyMatters: "Volume fora do esperado pode indicar falha na origem ou filtro incorreto.",
    whenToWorry: "Se estiver muito abaixo do mínimo ou acima do máximo esperado.",
  },
  severity: {
    whatIs: "Classificação do impacto do incidente. S0=Crítico, S1=Alto, S2=Médio, S3=Baixo, S4=Info.",
    whyMatters: "Define a prioridade de triagem e o SLA de resposta.",
    whenToWorry: "S0 e S1 requerem ação imediata.",
  },
  sla: {
    whatIs: "Tempo máximo aceitável para entrega dos dados processados.",
    whyMatters: "Atraso no SLA pode impactar relatórios e decisões de negócio.",
    whenToWorry: "Quando a execução ultrapassa o tempo esperado de entrega.",
  },
} as const;
