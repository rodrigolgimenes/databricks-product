import { useState } from "react";
import { CheckCircle2, AlertTriangle, XCircle, ChevronDown, ChevronRight } from "lucide-react";
import type { GuardrailResultItem } from "@/lib/ops-control-plane";
import { useViewMode } from "@/contexts/ViewModeContext";

const iconByStatus = {
  PASS: <CheckCircle2 className="h-4 w-4 text-green-600" />,
  WARN: <AlertTriangle className="h-4 w-4 text-amber-600" />,
  FAIL: <XCircle className="h-4 w-4 text-red-600" />,
} as const;

const checkExplanation: Record<string, {
  businessLabel: string;
  possibleCauses: string[];
  recommendedAction: string;
}> = {
  watermark: {
    businessLabel: "Atualização dos dados",
    possibleCauses: [
      "Falha na origem dos dados",
      "Filtro incorreto no processamento",
      "Ausência de novos dados na fonte",
    ],
    recommendedAction: "Verificar se a fonte está gerando dados e se o processamento está correto.",
  },
  volume: {
    businessLabel: "Volume de registros",
    possibleCauses: [
      "Falha na origem",
      "Filtro incorreto",
      "Ausência de novos dados",
      "Duplicação de registros",
    ],
    recommendedAction: "Comparar com volumes históricos e verificar a fonte de dados.",
  },
  dq: {
    businessLabel: "Qualidade dos dados",
    possibleCauses: [
      "Dados corrompidos na origem",
      "Mudança de formato sem aviso",
      "Valores inválidos em campos obrigatórios",
    ],
    recommendedAction: "Inspecionar os registros que falharam e notificar o time responsável pela origem.",
  },
  silent_failure: {
    businessLabel: "Falha silenciosa",
    possibleCauses: [
      "Processamento terminou sem processar nenhum dado",
      "Condição de filtro removeu todos os registros",
      "Fonte vazia por erro upstream",
    ],
    recommendedAction: "Investigar imediatamente. Dados podem estar desatualizados sem alerta.",
  },
};

function GuardrailCheckItem({ check }: { check: GuardrailResultItem }) {
  const { isBusiness } = useViewMode();
  const [expanded, setExpanded] = useState(false);
  const explanation = checkExplanation[check.check];

  return (
    <div className="rounded border p-2">
      <div className="flex items-start gap-2">
        {iconByStatus[check.status]}
        <div className="flex-1">
          <p className="text-sm font-medium">
            {isBusiness && explanation
              ? `${explanation.businessLabel}: ${check.status === "PASS" ? "OK" : check.status === "FAIL" ? "Problema detectado" : "Atenção"}`
              : `${check.check.replace("_", " ")}: ${check.status}`}
          </p>
          <p className="text-xs text-muted-foreground">{check.message}</p>

          {check.status !== "PASS" && explanation && (
            <>
              <button
                onClick={() => setExpanded(!expanded)}
                className="mt-1 inline-flex items-center gap-1 text-xs text-blue-700 hover:underline"
              >
                {expanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                {isBusiness ? "O que pode ter acontecido?" : "Possíveis causas"}
              </button>
              {expanded && (
                <div className="mt-1.5 space-y-1 text-xs border-t pt-1.5">
                  <p className="font-medium">Possíveis causas:</p>
                  <ul className="list-disc list-inside text-muted-foreground space-y-0.5">
                    {explanation.possibleCauses.map((cause) => (
                      <li key={cause}>{cause}</li>
                    ))}
                  </ul>
                  <p className="font-medium mt-1">Ação recomendada:</p>
                  <p className="text-muted-foreground">{explanation.recommendedAction}</p>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export function GuardrailChecklist({ checks }: { checks: GuardrailResultItem[] }) {
  return (
    <div className="space-y-2">
      {checks.map((check) => (
        <GuardrailCheckItem key={check.check} check={check} />
      ))}
    </div>
  );
}

