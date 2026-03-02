import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Database, CheckCircle, AlertCircle, PauseCircle, RefreshCw, Cog, Clock,
} from "lucide-react";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import * as api from "@/lib/api";

const Dashboard = () => {
  const [summary, setSummary] = useState<any>(null);
  const [orchStatus, setOrchStatus] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const fetchData = async () => {
    setLoading(true);
    setError("");
    try {
      const [summaryData, orchData] = await Promise.all([
        api.getDashboardSummary(),
        api.getOrchestratorStatus().catch(() => null),
      ]);
      setSummary(summaryData);
      setOrchStatus(orchData);
    } catch (e: any) {
      setError(e.message || "Erro ao carregar dashboard");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <LoadingSpinner size="lg" text="Carregando dashboard..." />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-muted-foreground mt-1">Visão geral da plataforma de ingestão</p>
        </div>
        <Card className="border-destructive/50">
          <CardContent className="p-6">
            <div className="flex items-center gap-3">
              <AlertCircle className="h-5 w-5 text-destructive" />
              <p className="text-destructive">{error}</p>
            </div>
            <Button variant="outline" size="sm" className="mt-4" onClick={fetchData}>
              <RefreshCw className="h-4 w-4 mr-2" /> Tentar novamente
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Parse dataset states
  const dsStates = summary?.dataset_states || [];
  const stateMap: Record<string, number> = {};
  dsStates.forEach((s: any) => {
    stateMap[String(s.execution_state || "").toUpperCase()] = Number(s.n || 0);
  });
  const totalDatasets = dsStates.reduce((acc: number, s: any) => acc + Number(s.n || 0), 0);
  const activeCount = stateMap["ACTIVE"] || 0;
  const pausedCount = stateMap["PAUSED"] || 0;
  const blockedCount = stateMap["BLOCKED_SCHEMA_CHANGE"] || 0;
  const failedCount = (summary?.recent_failures || []).length;

  // Run queue states
  const rqStates = summary?.run_queue_states || [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-muted-foreground mt-1">Visão geral da plataforma de ingestão</p>
        </div>
        <Button variant="outline" size="sm" onClick={fetchData}>
          <RefreshCw className="h-4 w-4 mr-2" /> Atualizar
        </Button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <Card className="hover:shadow-md transition-shadow">
          <CardContent className="flex items-center p-6">
            <div className="flex items-center space-x-4">
              <div className="p-2 bg-blue-100 rounded-lg">
                <Database className="h-6 w-6 text-blue-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-muted-foreground">Total Datasets</p>
                <p className="text-2xl font-bold">{totalDatasets}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="hover:shadow-md transition-shadow">
          <CardContent className="flex items-center p-6">
            <div className="flex items-center space-x-4">
              <div className="p-2 bg-green-100 rounded-lg">
                <CheckCircle className="h-6 w-6 text-green-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-muted-foreground">Ativos</p>
                <p className="text-2xl font-bold text-green-700">{activeCount}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="hover:shadow-md transition-shadow">
          <CardContent className="flex items-center p-6">
            <div className="flex items-center space-x-4">
              <div className="p-2 bg-yellow-100 rounded-lg">
                <PauseCircle className="h-6 w-6 text-yellow-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-muted-foreground">Pausados</p>
                <p className="text-2xl font-bold text-yellow-700">{pausedCount}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="hover:shadow-md transition-shadow">
          <CardContent className="flex items-center p-6">
            <div className="flex items-center space-x-4">
              <div className="p-2 bg-red-100 rounded-lg">
                <AlertCircle className="h-6 w-6 text-red-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-muted-foreground">Bloqueados/Falhas</p>
                <p className="text-2xl font-bold text-red-700">{blockedCount + failedCount}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Orchestrator + Queue */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Orchestrator Status */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Cog className="h-5 w-5" /> Status do Orchestrator
            </CardTitle>
          </CardHeader>
          <CardContent>
            {orchStatus ? (
              <div className="space-y-4">
                <div className="flex items-center gap-3">
                  <div
                    className={`h-3 w-3 rounded-full ${
                      orchStatus.orchestrator_status?.likely_active
                        ? "bg-green-500 animate-pulse"
                        : "bg-yellow-500"
                    }`}
                  />
                  <span className="font-medium">
                    {orchStatus.orchestrator_status?.likely_active ? "Ativo" : "Possivelmente inativo"}
                  </span>
                </div>
                {orchStatus.orchestrator_status?.warning && (
                  <p className="text-sm text-yellow-600 bg-yellow-50 p-3 rounded-lg">
                    {orchStatus.orchestrator_status.warning}
                  </p>
                )}
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div className="bg-muted/50 p-3 rounded-lg">
                    <p className="text-muted-foreground">Pendentes na fila</p>
                    <p className="text-xl font-bold">{orchStatus.queue_stats?.pending || 0}</p>
                  </div>
                  <div className="bg-muted/50 p-3 rounded-lg">
                    <p className="text-muted-foreground">Em execução</p>
                    <p className="text-xl font-bold">{orchStatus.queue_stats?.running || 0}</p>
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-muted-foreground">Não foi possível obter status do orchestrator.</p>
            )}
          </CardContent>
        </Card>

        {/* Run Queue States */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="h-5 w-5" /> Run Queue
            </CardTitle>
          </CardHeader>
          <CardContent>
            {rqStates.length > 0 ? (
              <div className="space-y-3">
                {rqStates.map((s: any, i: number) => (
                  <div key={i} className="flex items-center justify-between p-3 bg-muted/30 rounded-lg">
                    <Badge
                      variant={
                        s.status === "SUCCEEDED"
                          ? "default"
                          : s.status === "FAILED"
                          ? "destructive"
                          : "secondary"
                      }
                    >
                      {s.status}
                    </Badge>
                    <span className="text-lg font-bold">{s.n}</span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-muted-foreground">Nenhum dado na fila.</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Recent Failures */}
      {(summary?.recent_failures || []).length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-destructive">
              <AlertCircle className="h-5 w-5" /> Falhas Recentes
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {summary.recent_failures.slice(0, 10).map((f: any, i: number) => (
                <div key={i} className="flex items-center justify-between p-4 border rounded-lg hover:bg-muted/30 transition-colors">
                  <div>
                    <p className="font-medium">{f.dataset_id?.slice(0, 8)}...</p>
                    <p className="text-sm text-muted-foreground">
                      {f.error_class || "UNKNOWN"}: {(f.error_message || "").slice(0, 80)}
                    </p>
                  </div>
                  <div className="text-right text-sm text-muted-foreground">
                    <p>{f.run_id?.slice(0, 8)}</p>
                    <p>{f.finished_at ? new Date(f.finished_at).toLocaleString("pt-BR") : "-"}</p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default Dashboard;
