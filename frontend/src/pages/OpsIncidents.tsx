import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { RefreshCw, UserCheck, CheckCircle2, AlertTriangle, ChevronDown, ChevronRight } from "lucide-react";
import { ViewModeToggle } from "@/components/ops/ViewModeToggle";
import { OpsTooltip, opsTooltips } from "@/components/ops/OpsTooltip";
import { useViewMode } from "@/contexts/ViewModeContext";
import * as api from "@/lib/api";
import type { OpsIncidentItem } from "@/lib/ops-control-plane";

const severityColor: Record<string, string> = {
  S0: "bg-red-700 text-white",
  S1: "bg-red-500 text-white",
  S2: "bg-amber-500 text-white",
  S3: "bg-yellow-400 text-yellow-950",
  S4: "bg-slate-200 text-slate-900",
};

const severityBusiness: Record<string, string> = {
  S0: "Crítico",
  S1: "Alto",
  S2: "Médio",
  S3: "Baixo",
  S4: "Informativo",
};

const statusBusiness: Record<string, string> = {
  OPEN: "Aberto — aguardando triagem",
  ACK: "Reconhecido — em análise",
  INVESTIGATING: "Em investigação",
  RESOLVED: "Resolvido",
};

const categoryBusiness: Record<string, string> = {
  "Schema Drift": "Estrutura do dado mudou",
  "Duration Anomaly": "Tempo de execução fora do padrão",
  "DQ Failure": "Regra de qualidade falhou",
  "SLA Breach": "Atraso na entrega",
  "Silent Failure": "Falha silenciosa detectada",
};

const OpsIncidents = () => {
  const { isBusiness } = useViewMode();
  const [incidents, setIncidents] = useState<OpsIncidentItem[]>([]);
  const [ownerInput, setOwnerInput] = useState<Record<string, string>>({});
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const loadIncidents = async () => {
    setLoading(true);
    setError(false);
    try {
      const res = await api.getOpsIncidents({ limit: 100 });
      setIncidents(res.items || []);
    } catch {
      setIncidents([]);
      setError(true);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadIncidents();
  }, []);

  const updateIncident = (incidentId: string, patch: Partial<OpsIncidentItem>) =>
    setIncidents((prev) => prev.map((it) => (it.incidentId === incidentId ? { ...it, ...patch } : it)));

  const handleAck = async (incidentId: string) => {
    try { await api.ackOpsIncident(incidentId, { actor: "ops-user" }); } catch { /* fallback */ }
    updateIncident(incidentId, { status: "ACK" });
  };

  const handleAssign = async (incidentId: string) => {
    const owner = ownerInput[incidentId] || "oncall";
    try { await api.assignOpsIncident(incidentId, { actor: "ops-user", owner }); } catch { /* fallback */ }
    updateIncident(incidentId, { owner, status: "INVESTIGATING" });
  };

  const handleResolve = async (incidentId: string) => {
    try { await api.resolveOpsIncident(incidentId, { actor: "ops-user", resolution_notes: "Resolvido via replay seguro" }); } catch { /* fallback */ }
    updateIncident(incidentId, { status: "RESOLVED" });
  };

  const toggleExpand = (id: string) =>
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">
            {isBusiness ? "Problemas Detectados" : "Incident Center (Ops)"}
          </h1>
          <p className="text-sm text-muted-foreground">
            {isBusiness
              ? "Problemas encontrados nas execuções, organizados por prioridade."
              : "Incidentes deduplicados por assinatura com fluxo de triagem operacional."}
            {error ? " (erro ao carregar dados)" : ""}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <ViewModeToggle />
          <Button variant="outline" size="sm" onClick={loadIncidents} disabled={loading}>
            <RefreshCw className={`mr-1 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Atualizar
          </Button>
        </div>
      </div>

      {!isBusiness && (
        <Card>
          <CardContent className="p-4 text-xs text-muted-foreground">
            <div className="flex items-start gap-2">
              <AlertTriangle className="h-4 w-4 mt-0.5 text-amber-600" />
              <p>Fluxo: <span className="font-semibold">OPEN → ACK → INVESTIGATING → RESOLVED</span>, com ações de ack/assign/resolve.</p>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="space-y-3">
        {incidents.map((incident) => {
          const isOpen = expanded[incident.incidentId] ?? false;
          return (
            <Card key={incident.incidentId}>
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between gap-2">
                  <div>
                    <CardTitle className="text-base">{incident.title}</CardTitle>
                    <p className="text-xs text-muted-foreground mt-1">
                      {isBusiness
                        ? categoryBusiness[incident.rootCategory] || incident.rootCategory
                        : `${incident.incidentId} • ${incident.rootCategory}`}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <OpsTooltip {...opsTooltips.severity}>
                      <Badge className={severityColor[incident.severity]}>
                        {isBusiness ? severityBusiness[incident.severity] : incident.severity}
                      </Badge>
                    </OpsTooltip>
                    <Badge variant="outline">
                      {isBusiness ? statusBusiness[incident.status] || incident.status : incident.status}
                    </Badge>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-3">
                {isBusiness && (
                  <p className="text-xs text-muted-foreground">
                    {incident.status === "OPEN"
                      ? "Este problema precisa ser avaliado. Clique em 'Reconhecer' para iniciar a triagem."
                      : incident.status === "RESOLVED"
                        ? "Problema resolvido. Nenhuma ação necessária."
                        : "Em investigação pelo time responsável."}
                  </p>
                )}

                <button onClick={() => toggleExpand(incident.incidentId)} className="inline-flex items-center gap-1 text-xs text-blue-700 hover:underline">
                  {isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                  {isBusiness ? "Ver detalhes" : "Detalhes técnicos"}
                </button>

                {isOpen && (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs border-t pt-2">
                    <p>{isBusiness ? "Detectado em" : "First seen"}: <span className="font-medium">{new Date(incident.firstSeenAt).toLocaleString("pt-BR")}</span></p>
                    <p>{isBusiness ? "Última ocorrência" : "Last seen"}: <span className="font-medium">{new Date(incident.lastSeenAt).toLocaleString("pt-BR")}</span></p>
                    <p>{isBusiness ? "Vezes que ocorreu" : "Occurrences"}: <span className="font-medium">{incident.occurrenceCount}</span></p>
                    <p>{isBusiness ? "Responsável" : "Owner"}: <span className="font-medium">{incident.owner || "não atribuído"}</span></p>
                  </div>
                )}

                <div className="flex flex-wrap items-center gap-2">
                  <Button size="sm" variant="outline" onClick={() => handleAck(incident.incidentId)} disabled={incident.status !== "OPEN"}>
                    <CheckCircle2 className="mr-1 h-3 w-3" /> {isBusiness ? "Reconhecer" : "Ack"}
                  </Button>
                  <Input
                    className="h-8 w-44"
                    placeholder={isBusiness ? "responsável" : "owner"}
                    value={ownerInput[incident.incidentId] || ""}
                    onChange={(e) => setOwnerInput((prev) => ({ ...prev, [incident.incidentId]: e.target.value }))}
                  />
                  <Button size="sm" variant="outline" onClick={() => handleAssign(incident.incidentId)} disabled={incident.status === "RESOLVED"}>
                    <UserCheck className="mr-1 h-3 w-3" /> {isBusiness ? "Atribuir" : "Assign"}
                  </Button>
                  <Button size="sm" onClick={() => handleResolve(incident.incidentId)} disabled={incident.status === "RESOLVED"}>
                    Resolver
                  </Button>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
};

export default OpsIncidents;

