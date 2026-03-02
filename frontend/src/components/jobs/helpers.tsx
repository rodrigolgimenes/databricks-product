import { Badge } from '@/components/ui/badge';
import { CheckCircle, XCircle, RefreshCw, Clock } from 'lucide-react';

/* ── Types ─────────────────────────────────────────── */

export interface Job {
  job_id: string;
  job_name: string;
  description: string;
  schedule_type: string;
  cron_expression: string;
  timezone: string;
  enabled: boolean;
  databricks_job_id: number;
  databricks_job_state: string;
  project_id: string;
  area_id: string;
  dataset_count: number;
  last_run_at: string;
  last_run_status: string;
  last_run_duration_ms: number;
  next_run_at: string;
  max_concurrent_runs: number;
  timeout_seconds: number;
  retry_on_timeout: boolean;
  created_at: string;
  created_by: string;
  updated_at: string;
  updated_by: string;
  datasets: any[];
}

export interface JobMetrics {
  successRate: number;
  avgDuration: number;
  totalRuns: number;
}

/* ── Formatters ────────────────────────────────────── */

export const formatDateTime = (dateStr: string) => {
  if (!dateStr) return '—';
  return new Date(dateStr).toLocaleString('pt-BR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
};

export const formatDuration = (ms: number | string | undefined) => {
  const v = Number(ms);
  if (!v || isNaN(v)) return '—';
  const seconds = Math.floor(v / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
};

/* ── Status renderers ──────────────────────────────── */

export const getStatusIcon = (status: string) => {
  if (!status) return <Clock className="h-4 w-4 text-gray-400" />;

  switch (status.toUpperCase()) {
    case 'SUCCESS':
    case 'SUCCEEDED':
      return <CheckCircle className="h-4 w-4 text-green-600" />;
    case 'FAILED':
      return <XCircle className="h-4 w-4 text-red-600" />;
    case 'RUNNING':
      return <RefreshCw className="h-4 w-4 text-blue-600 animate-spin" />;
    default:
      return <Clock className="h-4 w-4 text-gray-400" />;
  }
};

export const getStatusBadge = (status: string) => {
  if (!status) return <Badge variant="outline">Sem execuções</Badge>;

  switch (status.toUpperCase()) {
    case 'SUCCESS':
    case 'SUCCEEDED':
      return <Badge className="bg-green-600">Sucesso</Badge>;
    case 'FAILED':
      return <Badge className="bg-red-600">Falha</Badge>;
    case 'RUNNING':
    case 'CLAIMED':
      return <Badge className="bg-blue-600">Executando</Badge>;
    case 'PENDING':
      return <Badge className="bg-yellow-500">Na Fila</Badge>;
    case 'CANCELLED':
      return <Badge variant="outline">Cancelado</Badge>;
    default:
      return <Badge variant="outline">{status}</Badge>;
  }
};
