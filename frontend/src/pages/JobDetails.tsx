import { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Button } from '@/components/ui/button';
import { Loader2 } from 'lucide-react';
import * as api from '@/lib/api';
import { Job, JobMetrics } from '@/components/jobs/helpers';
import { JobHeader } from '@/components/jobs/JobHeader';
import { JobMetricsCards } from '@/components/jobs/JobMetricsCards';
import { JobOverviewTab } from '@/components/jobs/JobOverviewTab';
import { JobDatasetsTab } from '@/components/jobs/JobDatasetsTab';
import { JobHistoryTab } from '@/components/jobs/JobHistoryTab';
import { JobFlowDiagram } from '@/components/jobs/JobFlowDiagram';
import { RunNowDialog } from '@/components/jobs/RunNowDialog';
import { DeleteJobDialog } from '@/components/jobs/DeleteJobDialog';
import { ReplayDialog } from '@/components/jobs/ReplayDialog';
import {
  calculateOperationalRisk,
  calculateDurationVariance,
  getStatusDistribution,
  buildSparklineData,
} from '@/lib/job-health';
import { toast } from 'sonner';

const JobDetails = () => {
  const navigate = useNavigate();
  const { jobId } = useParams();
  const [job, setJob] = useState<Job | null>(null);
  const [runs, setRuns] = useState<any[]>([]);
  const [activeQueue, setActiveQueue] = useState<any[]>([]);
  const [recentExecutions, setRecentExecutions] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const [runNowOpen, setRunNowOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [replayOpen, setReplayOpen] = useState(false);
  const [replayExecutionId, setReplayExecutionId] = useState('');

  /* ── Data fetching ─────────────────────────────────── */

  const fetchJobDetails = useCallback(async () => {
    if (!jobId) return;
    try {
      const data = await api.getJob(jobId);
      const jobData = data.job || data;
      if (data.datasets) jobData.datasets = data.datasets;
      jobData.dataset_count = data.datasets?.length ?? jobData.dataset_count ?? 0;
      setJob(jobData);
      setActiveQueue(data.active_queue || []);
      setRecentExecutions(data.recent_executions || []);
    } catch (error) {
      console.error('Error fetching job:', error);
    }
  }, [jobId]);

  const fetchJobRuns = useCallback(async () => {
    if (!jobId) return;
    try {
      const data = await api.getJobRuns(jobId, { limit: 50 });
      setRuns(data.runs || data.executions || []);
    } catch (error) {
      console.error('Error fetching runs:', error);
    }
  }, [jobId]);

  const loadData = useCallback(async () => {
    setLoading(true);
    await Promise.all([fetchJobDetails(), fetchJobRuns()]);
    setLoading(false);
  }, [fetchJobDetails, fetchJobRuns]);

  useEffect(() => { loadData(); }, [loadData]);

  // Auto-refresh while there are active executions
  useEffect(() => {
    const hasActiveExecution = recentExecutions.some(
      (e) => e.status === 'RUNNING' || e.status === 'PENDING'
    ) || activeQueue.length > 0;

    if (!hasActiveExecution || loading) return;

    const timer = setInterval(() => {
      fetchJobDetails();
      fetchJobRuns();
    }, 10000);

    return () => clearInterval(timer);
  }, [recentExecutions, activeQueue, loading, fetchJobDetails, fetchJobRuns]);

  /* ── Handlers ────────────────────────────────────── */

  const handleRefresh = async () => {
    setRefreshing(true);
    await loadData();
    setRefreshing(false);
  };

  const handleToggle = async () => {
    if (!jobId) return;
    try {
      await api.toggleJob(jobId);
      await fetchJobDetails();
    } catch (error) {
      console.error('Error toggling job:', error);
      alert('Erro ao alternar estado do job');
    }
  };

  const handleRunNow = async () => {
    if (!jobId) return;
    try {
      const result = await api.runJobNow(jobId);
      toast.success(`Job iniciado com sucesso! ${result.message || ''}`);
      setRunNowOpen(false);
      setActiveTab('overview');
      setTimeout(handleRefresh, 2000);
    } catch (error: any) {
      console.error('Error running job:', error);
      if (error.message?.includes('em andamento')) {
        toast.warning(error.message);
      } else {
        toast.error(`Erro ao executar job: ${error.message}`);
      }
      throw error; // re-throw so RunNowDialog keeps dialog open
    }
  };

  const handleCancel = async () => {
    if (!jobId) return;
    setCancelling(true);
    try {
      const result = await api.cancelJob(jobId);
      toast.success(result.message || 'Job cancelado com sucesso!');
      setTimeout(handleRefresh, 2000);
    } catch (error: any) {
      console.error('Error cancelling job:', error);
      toast.error(`Erro ao cancelar job: ${error.message}`);
    } finally {
      setCancelling(false);
    }
  };

  const handleSync = async () => {
    if (!jobId) return;
    setSyncing(true);
    try {
      const result = await api.syncJob(jobId);
      alert(result.message || 'Job sincronizado com sucesso!');
      await fetchJobDetails();
    } catch (error: any) {
      console.error('Error syncing job:', error);
      alert(`Erro ao sincronizar: ${error.message}`);
    } finally {
      setSyncing(false);
    }
  };

  const handleDelete = async () => {
    if (!jobId) return;
    setDeleting(true);
    try {
      await api.deleteJob(jobId);
      setDeleteOpen(false);
      navigate('/jobs');
    } catch (error: any) {
      console.error('Error deleting job:', error);
      alert(`Erro ao excluir job: ${error.message}`);
      setDeleting(false);
    }
  };

  const handleRemoveDataset = async (datasetId: string) => {
    if (!jobId || !confirm('Remover este dataset do job?')) return;
    try {
      await api.removeJobDataset(jobId, datasetId);
      await fetchJobDetails();
    } catch (error) {
      console.error('Error removing dataset:', error);
      alert('Erro ao remover dataset');
    }
  };

  /* ── Derived data (memoized) ─────────────────────── */

  const statusDist = useMemo(() => getStatusDistribution(runs), [runs]);
  const durationAnalysis = useMemo(() => calculateDurationVariance(runs), [runs]);
  const sparkline = useMemo(() => buildSparklineData(runs), [runs]);
  const risk = useMemo(
    () => calculateOperationalRisk(runs, job?.enabled ?? true),
    [runs, job?.enabled]
  );

  const metrics: JobMetrics = useMemo(() => {
    if (runs.length === 0) return { successRate: 0, avgDuration: 0, totalRuns: 0 };
    const successRate = statusDist.total > 0
      ? Math.round((statusDist.succeeded / statusDist.total) * 100)
      : 0;
    return { successRate, avgDuration: durationAnalysis.avgMs, totalRuns: runs.length };
  }, [runs, statusDist, durationAnalysis]);

  /* ── Loading / Error states ──────────────────────── */

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!job) {
    return (
      <div className="text-center py-12">
        <p className="text-muted-foreground">Job não encontrado</p>
        <Button variant="outline" className="mt-4" onClick={() => navigate('/jobs')}>
          Voltar para Jobs
        </Button>
      </div>
    );
  }

  /* ── Render ──────────────────────────────────────── */

  return (
    <div className="space-y-4">
      <JobHeader
        job={job}
        recentExecutions={recentExecutions}
        activeQueue={activeQueue}
        refreshing={refreshing}
        syncing={syncing}
        deleting={deleting}
        cancelling={cancelling}
        riskLevel={risk.level}
        riskReasons={risk.reasons}
        onRefresh={handleRefresh}
        onRunNow={() => setRunNowOpen(true)}
        onToggle={handleToggle}
        onSync={handleSync}
        onDelete={() => setDeleteOpen(true)}
        onEdit={() => navigate(`/jobs/${jobId}/edit`)}
        onBack={() => navigate('/jobs')}
        onCancel={handleCancel}
      />

      <JobMetricsCards
        metrics={metrics}
        datasetCount={job.dataset_count || 0}
        statusDist={statusDist}
        durationAnalysis={durationAnalysis}
        sparkline={sparkline}
      />

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="overview">Visão Geral</TabsTrigger>
          <TabsTrigger value="pipeline">Pipeline</TabsTrigger>
          <TabsTrigger value="datasets">Datasets ({job.dataset_count || 0})</TabsTrigger>
          <TabsTrigger value="history">Histórico ({metrics.totalRuns})</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-4">
          <JobOverviewTab job={job} />
        </TabsContent>

        <TabsContent value="pipeline">
          <JobFlowDiagram job={job} activeQueue={activeQueue} />
        </TabsContent>

        <TabsContent value="datasets">
          <JobDatasetsTab
            job={job}
            onRemoveDataset={handleRemoveDataset}
            onEdit={() => navigate(`/jobs/${jobId}/edit`)}
          />
        </TabsContent>

        <TabsContent value="history">
          <JobHistoryTab
            runs={runs}
            onReplay={(executionId) => {
              setReplayExecutionId(executionId);
              setReplayOpen(true);
            }}
          />
        </TabsContent>
      </Tabs>

      {/* Dialogs */}
      <RunNowDialog
        open={runNowOpen}
        onOpenChange={setRunNowOpen}
        jobName={job.job_name}
        hasActiveExecution={recentExecutions.some(e => e.status === 'RUNNING')}
        maxConcurrentRuns={job.max_concurrent_runs}
        onConfirm={handleRunNow}
      />
      <DeleteJobDialog
        open={deleteOpen}
        onOpenChange={setDeleteOpen}
        jobName={job.job_name}
        onConfirm={handleDelete}
      />
      {replayOpen && jobId && (
        <ReplayDialog
          open={replayOpen}
          onOpenChange={setReplayOpen}
          jobId={jobId}
          jobName={job.job_name}
          executionId={replayExecutionId}
          onSuccess={() => {
            setActiveTab('overview');
            setTimeout(handleRefresh, 2000);
          }}
        />
      )}
    </div>
  );
};

export default JobDetails;
