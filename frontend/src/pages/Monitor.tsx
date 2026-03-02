import { useState, useEffect, useCallback } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Activity, AlertCircle, Briefcase, Database, Server, ShieldCheck } from "lucide-react";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import { MonitorKPIs } from "@/components/monitor/MonitorKPIs";
import { JobExecutionsTab } from "@/components/monitor/JobExecutionsTab";
import { ExecutionsTab } from "@/components/monitor/ExecutionsTab";
import { FailuresTab } from "@/components/monitor/FailuresTab";
import { QueueTab } from "@/components/monitor/QueueTab";
import { OrchestratorTab } from "@/components/monitor/OrchestratorTab";
import { SreDashboardTab } from "@/components/monitor/SreDashboardTab";
import * as api from "@/lib/api";

const Monitor = () => {
  const [stats, setStats] = useState<Record<string, number>>({});
  const [metrics24h, setMetrics24h] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(new Date());
  const [activeTab, setActiveTab] = useState("jobs");
  const [pollingInterval, setPollingInterval] = useState(30000);
  const [isAutoRefreshing, setIsAutoRefreshing] = useState(false);

  const fetchKPIs = useCallback(async () => {
    try {
      const qs = await api.getQueueStats().catch(() => ({ stats: {}, metrics_24h: null }));
      setStats(qs.stats || {});
      setMetrics24h(qs.metrics_24h || null);
      setLastRefresh(new Date());

      // Smart polling: faster when jobs are running
      const running = (qs.stats?.RUNNING || 0) + (qs.stats?.CLAIMED || 0);
      if (running > 0) {
        setPollingInterval(5000);
        setIsAutoRefreshing(true);
      } else {
        setPollingInterval(30000);
        setIsAutoRefreshing(false);
      }
    } catch (e) {
      console.error("KPI fetch error:", e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchKPIs();
  }, [fetchKPIs]);

  useEffect(() => {
    const interval = setInterval(fetchKPIs, pollingInterval);
    return () => clearInterval(interval);
  }, [fetchKPIs, pollingInterval]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <LoadingSpinner size="lg" text="Carregando monitoramento..." />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold flex items-center gap-2">
          <Activity className="h-6 w-6" /> Monitoramento
        </h1>
        <p className="text-muted-foreground mt-1 text-sm">
          Acompanhe execuções em tempo real · Atualizado: {lastRefresh.toLocaleTimeString("pt-BR")}
          {isAutoRefreshing && (
            <span className="ml-2 inline-flex items-center gap-1 text-blue-600 text-xs font-medium">
              <span className="h-2 w-2 bg-blue-600 rounded-full animate-pulse" />
              Auto-refresh ativo (5s)
            </span>
          )}
        </p>
      </div>

      {/* KPI Cards */}
      <MonitorKPIs stats={stats} metrics24h={metrics24h} />

      {/* Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="jobs" className="gap-1.5">
            <Briefcase className="h-3.5 w-3.5" /> Jobs
          </TabsTrigger>
          <TabsTrigger value="executions" className="gap-1.5">
            <Activity className="h-3.5 w-3.5" /> Execuções
          </TabsTrigger>
          <TabsTrigger value="failures" className="gap-1.5">
            <AlertCircle className="h-3.5 w-3.5" /> Falhas
            {(stats.FAILED || 0) > 0 && (
              <span className="ml-1 bg-red-100 text-red-700 text-xs font-bold px-1.5 py-0.5 rounded-full">
                {stats.FAILED}
              </span>
            )}
          </TabsTrigger>
          <TabsTrigger value="queue" className="gap-1.5">
            <Database className="h-3.5 w-3.5" /> Fila
          </TabsTrigger>
          <TabsTrigger value="orchestrator" className="gap-1.5">
            <Server className="h-3.5 w-3.5" /> Orchestrator
          </TabsTrigger>
          <TabsTrigger value="sre" className="gap-1.5">
            <ShieldCheck className="h-3.5 w-3.5" /> SRE
          </TabsTrigger>
        </TabsList>

        <TabsContent value="jobs" className="mt-4">
          <JobExecutionsTab pollingInterval={pollingInterval} isActive={activeTab === "jobs"} />
        </TabsContent>

        <TabsContent value="executions" className="mt-4">
          <ExecutionsTab pollingInterval={pollingInterval} isActive={activeTab === "executions"} />
        </TabsContent>

        <TabsContent value="failures" className="mt-4">
          <FailuresTab pollingInterval={pollingInterval} isActive={activeTab === "failures"} />
        </TabsContent>

        <TabsContent value="queue" className="mt-4">
          <QueueTab pollingInterval={pollingInterval} isActive={activeTab === "queue"} />
        </TabsContent>

        <TabsContent value="orchestrator" className="mt-4">
          <OrchestratorTab pollingInterval={pollingInterval} isActive={activeTab === "orchestrator"} />
        </TabsContent>

        <TabsContent value="sre" className="mt-4">
          <SreDashboardTab pollingInterval={pollingInterval} isActive={activeTab === "sre"} />
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default Monitor;
