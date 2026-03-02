import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { AlertTriangle, ShieldCheck } from "lucide-react";
import { ViewModeToggle } from "@/components/ops/ViewModeToggle";
import { useViewMode } from "@/contexts/ViewModeContext";
import * as api from "@/lib/api";
import type { ReplayAssessment } from "@/lib/ops-control-plane";

type ReplayMode = "RETRY_RUN" | "REPLAY_SAFE" | "REPLAY_SANDBOX" | "BACKFILL_RANGE";

const decisionColor: Record<string, string> = {
  ALLOWED: "bg-green-100 text-green-900",
  NEEDS_APPROVAL: "bg-amber-100 text-amber-900",
  BLOCKED: "bg-red-100 text-red-900",
};

const decisionBusiness: Record<string, string> = {
  ALLOWED: "Permitido",
  NEEDS_APPROVAL: "Precisa de aprovação",
  BLOCKED: "Bloqueado",
};

const modeBusiness: Record<string, string> = {
  RETRY_RUN: "Tentar novamente",
  REPLAY_SAFE: "Reprocessar com segurança",
  REPLAY_SANDBOX: "Reprocessar em ambiente isolado",
  BACKFILL_RANGE: "Reprocessar período",
};

const fmtCost = (v: number) => `R$ ${(v * 5.2).toFixed(2)}`;

const OpsReplayCenter = () => {
  const { isBusiness } = useViewMode();
  const [portalJobId, setPortalJobId] = useState("");
  const [mode, setMode] = useState<ReplayMode>("RETRY_RUN");
  const [rangeStart, setRangeStart] = useState("");
  const [rangeEnd, setRangeEnd] = useState("");
  const [justification, setJustification] = useState("");
  const [assessment, setAssessment] = useState<ReplayAssessment | null>(null);
  const [confirmed, setConfirmed] = useState(false);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);
  const [executing, setExecuting] = useState(false);

  const canEvaluate = useMemo(() => portalJobId.trim().length > 0, [portalJobId]);
  const canExecute =
    assessment &&
    (assessment.policyDecision === "ALLOWED" ||
      (assessment.policyDecision === "NEEDS_APPROVAL" && justification.trim().length > 0));

  const evaluate = async () => {
    if (!canEvaluate) return;
    setLoading(true);
    setMessage("");
    setConfirmed(false);
    try {
      const res = await api.evaluateReplayPolicy({
        portal_job_id: portalJobId,
        mode,
        range_start: rangeStart || undefined,
        range_end: rangeEnd || undefined,
      });
      setAssessment(res);
    } catch {
      setAssessment(null);
      setMessage("Erro ao avaliar política de replay. Verifique a conexão com o servidor.");
    } finally {
      setLoading(false);
    }
  };

  const execute = async () => {
    if (!assessment) return;
    setExecuting(true);
    setMessage("");
    try {
      await api.requestReplayExecution({
        portal_job_id: portalJobId,
        mode,
        risk_score: assessment.riskScore,
        justification: justification || undefined,
      });
      setMessage("Solicitação de replay registrada com sucesso.");
    } catch {
      setMessage("Solicitação simulada localmente (fallback).");
    } finally {
      setExecuting(false);
    }
  };

  const estDurationMin = assessment ? Math.max(1, Math.round(assessment.estimatedRows / 500_000)) : 0;

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">
            {isBusiness ? "Reprocessamento de Dados" : "Replay Center (Safe Replay)"}
          </h1>
          <p className="text-sm text-muted-foreground">
            {isBusiness
              ? "Solicite o reprocessamento de dados com segurança e controle."
              : "Reprocessamento governado com avaliação de risco, policy check e aprovação para T0/T1."}
          </p>
        </div>
        <ViewModeToggle />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {isBusiness ? "Nova solicitação" : "Nova solicitação de replay"}
          </CardTitle>
        </CardHeader>
        <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <div className="space-y-1">
            <Label>{isBusiness ? "Job" : "Portal Job ID"}</Label>
            <Input value={portalJobId} onChange={(e) => setPortalJobId(e.target.value)} placeholder="job_finance_close" />
          </div>
          <div className="space-y-1">
            <Label>{isBusiness ? "Tipo de reprocessamento" : "Modo"}</Label>
            <Select value={mode} onValueChange={(v: ReplayMode) => setMode(v)}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="RETRY_RUN">{isBusiness ? modeBusiness.RETRY_RUN : "Retry Run"}</SelectItem>
                <SelectItem value="REPLAY_SAFE">{isBusiness ? modeBusiness.REPLAY_SAFE : "Replay Safe"}</SelectItem>
                <SelectItem value="REPLAY_SANDBOX">{isBusiness ? modeBusiness.REPLAY_SANDBOX : "Replay Sandbox"}</SelectItem>
                <SelectItem value="BACKFILL_RANGE">{isBusiness ? modeBusiness.BACKFILL_RANGE : "Backfill Range"}</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {mode === "BACKFILL_RANGE" && (
            <>
              <div className="space-y-1">
                <Label>{isBusiness ? "Data início" : "Range Start"}</Label>
                <Input type="date" value={rangeStart} onChange={(e) => setRangeStart(e.target.value)} />
              </div>
              <div className="space-y-1">
                <Label>{isBusiness ? "Data fim" : "Range End"}</Label>
                <Input type="date" value={rangeEnd} onChange={(e) => setRangeEnd(e.target.value)} />
              </div>
            </>
          )}

          <div className="md:col-span-2 flex items-center gap-2">
            <Button onClick={evaluate} disabled={!canEvaluate || loading}>
              {loading ? "Avaliando..." : isBusiness ? "Verificar viabilidade" : "Avaliar risco e policy"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {assessment && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <ShieldCheck className="h-4 w-4" />
              {isBusiness ? "Resultado da verificação" : "Resultado da avaliação"}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {/* Business impact summary */}
            {isBusiness && (
              <div className="rounded border border-amber-200 bg-amber-50 p-3 space-y-2 text-sm">
                <p className="font-semibold">Impacto do reprocessamento:</p>
                <ul className="list-disc list-inside text-xs space-y-1 text-muted-foreground">
                  <li>Registros afetados: <strong>{assessment.estimatedRows.toLocaleString("pt-BR")}</strong></li>
                  <li>Custo estimado: <strong>{fmtCost(assessment.estimatedCostUsd)}</strong></li>
                  <li>Tempo estimado: <strong>~{estDurationMin} minuto{estDurationMin !== 1 ? "s" : ""}</strong></li>
                  <li>Jobs dependentes afetados: <strong>{assessment.impactJobs}</strong></li>
                </ul>
                {assessment.impactJobs > 1 && (
                  <p className="text-xs text-amber-700">
                    ⚠ Outros processos podem precisar ser reprocessados também.
                  </p>
                )}
              </div>
            )}

            {/* Technical assessment */}
            {!isBusiness && (
              <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
                <p>Risk Score: <span className="font-semibold">{assessment.riskScore}</span></p>
                <p>Rows: <span className="font-semibold">{assessment.estimatedRows.toLocaleString("pt-BR")}</span></p>
                <p>Custo: <span className="font-semibold">${assessment.estimatedCostUsd.toFixed(2)}</span></p>
                <p>Impact jobs: <span className="font-semibold">{assessment.impactJobs}</span></p>
                <p>Policy: <Badge className={decisionColor[assessment.policyDecision]}>{assessment.policyDecision}</Badge></p>
              </div>
            )}

            <p className="text-xs text-muted-foreground">
              {isBusiness
                ? `Status: ${decisionBusiness[assessment.policyDecision]}. ${assessment.reason}`
                : assessment.reason}
            </p>

            {assessment.policyDecision !== "ALLOWED" && (
              <div className="space-y-1">
                <Label>
                  {isBusiness ? "Justificativa (obrigatória)" : "Justificativa (obrigatória para approval/block exceptions)"}
                </Label>
                <Textarea value={justification} onChange={(e) => setJustification(e.target.value)} />
              </div>
            )}

            {/* Business confirmation step */}
            {isBusiness && assessment.policyDecision !== "BLOCKED" && !confirmed && (
              <Button variant="outline" onClick={() => setConfirmed(true)} className="w-full">
                Entendo o impacto, continuar →
              </Button>
            )}

            {(!isBusiness || confirmed) && (
              <div className="flex items-center gap-2">
                <Button
                  onClick={execute}
                  disabled={!canExecute || executing || assessment.policyDecision === "BLOCKED"}
                >
                  {assessment.policyDecision === "NEEDS_APPROVAL"
                    ? isBusiness ? "Solicitar aprovação" : "Solicitar aprovação"
                    : isBusiness ? "Confirmar reprocessamento" : "Executar replay"}
                </Button>
                {assessment.policyDecision === "BLOCKED" && (
                  <p className="inline-flex items-center gap-1 text-xs text-red-700">
                    <AlertTriangle className="h-3 w-3" />
                    {isBusiness ? "Bloqueado. Não é possível reprocessar no momento." : "Bloqueado por policy."}
                  </p>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {message && (
        <Card>
          <CardContent className="p-3 text-sm">{message}</CardContent>
        </Card>
      )}
    </div>
  );
};

export default OpsReplayCenter;

