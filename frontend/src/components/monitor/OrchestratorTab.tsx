import { useState, useEffect, useCallback } from "react";
import { Badge } from "@/components/ui/badge";
import { StatusBadge } from "@/components/ui/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Server, Zap, Settings2, ExternalLink, Loader2, RefreshCw,
} from "lucide-react";
import * as api from "@/lib/api";

const formatDuration = (ms: number) => {
  if (!ms || ms <= 0) return "—";
  const s = Math.round(ms / 1000);
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
};

interface OrchestratorTabProps {
  pollingInterval: number;
  isActive: boolean;
}

export function OrchestratorTab({ pollingInterval, isActive }: OrchestratorTabProps) {
  const [dbConfig, setDbConfig] = useState<any>(null);
  const [jobRuns, setJobRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const cfg = await api.getDatabricksConfig().catch(() => null);
      setDbConfig(cfg);

      if (cfg?.orchestrator_job_id) {
        const d = await api.getDatabricksJobRuns(cfg.orchestrator_job_id, 10).catch(() => ({ runs: [] }));
        setJobRuns(d.runs || []);
      }
    } catch (e) {
      console.error("OrchestratorTab fetch error:", e);
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
    const interval = setInterval(fetchData, pollingInterval);
    return () => clearInterval(interval);
  }, [fetchData, pollingInterval, isActive]);

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground justify-center">
        <Loader2 className="h-4 w-4 animate-spin" /> Carregando configuração...
      </div>
    );
  }

  if (!dbConfig?.rest_api_configured) {
    return (
      <div className="text-center py-12 space-y-2">
        <Settings2 className="h-10 w-10 text-muted-foreground mx-auto" />
        <p className="text-muted-foreground text-sm">Databricks REST API não configurado.</p>
        <p className="text-xs text-muted-foreground">Configure DATABRICKS_HOST e DATABRICKS_TOKEN no .env</p>
      </div>
    );
  }

  if (!dbConfig?.orchestrator_job_id) {
    return (
      <div className="text-center py-12 space-y-2">
        <Zap className="h-10 w-10 text-yellow-500 mx-auto" />
        <p className="text-muted-foreground text-sm">Orchestrator Job ID não configurado.</p>
        <p className="text-xs text-muted-foreground">
          Adicione <code className="bg-muted px-1 rounded">DATABRICKS_ORCHESTRATOR_JOB_ID=&lt;job_id&gt;</code> no .env
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Config Summary */}
      <div className="flex items-center justify-between p-4 bg-muted/30 rounded-lg">
        <div className="text-sm">
          <p className="font-medium flex items-center gap-2">
            <Server className="h-4 w-4" />
            Job ID: <span className="font-mono">{dbConfig.orchestrator_job_id}</span>
          </p>
          <p className="text-xs text-muted-foreground mt-1">Host: {dbConfig.host}</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="default" className="text-xs">
            <Zap className="h-3 w-3 mr-1" /> On-demand ativo
          </Badge>
          <Button variant="outline" size="sm" onClick={fetchData}>
            <RefreshCw className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>

      {/* Job Runs */}
      {jobRuns.length === 0 ? (
        <p className="text-xs text-muted-foreground text-center py-6">Nenhuma execução recente do job.</p>
      ) : (
        <div className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">Últimas execuções do Databricks Job:</p>
          {jobRuns.map((run: any, i: number) => {
            const state = run.state?.life_cycle_state || "UNKNOWN";
            const result = run.state?.result_state || "";
            const isActive = ["PENDING", "RUNNING", "QUEUED"].includes(state);
            const isFailed = result === "FAILED";
            const isSuccess = result === "SUCCESS";
            const params = run.overriding_parameters?.notebook_params || {};

            return (
              <Card
                key={i}
                className={
                  isActive ? "border-blue-200 bg-blue-50/30" :
                  isFailed ? "border-red-200 bg-red-50/30" : ""
                }
              >
                <CardContent className="flex items-center justify-between p-3">
                  <div className="flex items-center gap-3">
                    <StatusBadge status={isActive ? "RUNNING" : isSuccess ? "SUCCEEDED" : isFailed ? "FAILED" : "PENDING"} />
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-xs">#{run.run_id}</span>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        {run.start_time ? new Date(run.start_time).toLocaleString("pt-BR") : "—"}
                        {" "}· {formatDuration(run.execution_duration || 0)}
                        {params.target_dataset_id && (
                          <span className="ml-1">· dataset: <span className="font-mono">{params.target_dataset_id.slice(0, 12)}...</span></span>
                        )}
                      </p>
                    </div>
                  </div>
                  {run.run_page_url && (
                    <a
                      href={run.run_page_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-xs text-blue-600 hover:underline flex items-center gap-1"
                    >
                      <ExternalLink className="h-3 w-3" /> Databricks
                    </a>
                  )}
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}
