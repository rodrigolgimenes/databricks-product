import { useNavigate } from 'react-router-dom';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { DataTable, DataTableColumn } from '@/components/ui/data-table';
import {
  Database, Edit, ShieldCheck, ShieldAlert, ShieldX,
  ArrowDownUp, ArrowDown, Copy, Clock,
} from 'lucide-react';
import { Job } from './helpers';
import { InfoTooltip } from './InfoTooltip';
import { FIELD_EXPLANATIONS } from '@/lib/field-explanations';

interface JobDatasetsTabProps {
  job: Job;
  onRemoveDataset: (datasetId: string) => void;
  onEdit: () => void;
}

/* ── Strategy Badge ────────────────────────────── */

const STRATEGY_CONFIG: Record<string, { icon: typeof Copy; color: string; label: string }> = {
  FULL:        { icon: Copy,        color: 'bg-blue-100 text-blue-800 border-blue-300',   label: 'Full' },
  INCREMENTAL: { icon: ArrowDown,   color: 'bg-green-100 text-green-800 border-green-300', label: 'Incremental' },
  SNAPSHOT:    { icon: ArrowDownUp, color: 'bg-purple-100 text-purple-800 border-purple-300', label: 'Snapshot' },
};

function StrategyBadge({ strategy }: { strategy?: string }) {
  const s = (strategy || '').toUpperCase();
  const cfg = STRATEGY_CONFIG[s];
  if (!cfg) return <Badge variant="outline" className="text-xs">{strategy || '—'}</Badge>;
  const Icon = cfg.icon;

  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-[11px] font-medium cursor-help ${cfg.color}`}>
            <Icon className="h-3 w-3" />
            {cfg.label}
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs text-xs">
          {FIELD_EXPLANATIONS.load_strategy}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/* ── Health Indicator ─────────────────────────── */

function DatasetHealth({ dataset }: { dataset: any }) {
  const status = (dataset.last_execution_status || dataset.status || '').toUpperCase();
  const isFailed = status === 'FAILED';
  const isRunning = status === 'RUNNING' || status === 'CLAIMED';

  // Simple heuristics based on available data
  let level: 'good' | 'warning' | 'critical' = 'good';
  let reason = 'Dataset operando normalmente';

  if (isFailed) {
    level = 'critical';
    reason = 'Última execução falhou';
  } else if (isRunning) {
    level = 'good';
    reason = 'Execução em andamento';
  } else if (dataset.bronze_row_count === 0 && status === 'SUCCEEDED') {
    level = 'warning';
    reason = 'Nenhuma linha processada na última execução (volume zero)';
  }

  const cfg = {
    good:     { icon: ShieldCheck, cls: 'text-green-600', label: 'Saudável' },
    warning:  { icon: ShieldAlert, cls: 'text-amber-600', label: 'Atenção' },
    critical: { icon: ShieldX,     cls: 'text-red-600',   label: 'Crítico' },
  }[level];
  const Icon = cfg.icon;

  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={`inline-flex items-center gap-1 text-xs cursor-help ${cfg.cls}`}>
            <Icon className="h-3.5 w-3.5" />
            {cfg.label}
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="text-xs max-w-[200px]">{reason}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/* ── Volume Display ───────────────────────────── */

function formatRows(count: number | undefined): string {
  if (count == null) return '—';
  if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
  if (count >= 1_000) return `${(count / 1_000).toFixed(1)}K`;
  return String(count);
}

/* ── Watermark Display ────────────────────────── */

function WatermarkValue({ dataset }: { dataset: any }) {
  const wm = dataset.watermark_value || dataset.watermark_end;
  if (!wm) return <span className="text-xs text-muted-foreground">—</span>;

  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="flex items-center gap-1 text-xs font-mono cursor-help">
            <Clock className="h-3 w-3 text-muted-foreground" />
            {String(wm).length > 16 ? String(wm).substring(0, 16) + '…' : wm}
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="text-xs max-w-xs">
          <p className="font-mono">{wm}</p>
          <p className="text-muted-foreground mt-1">{FIELD_EXPLANATIONS.watermark}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/* ── Main Component ─────────────────────────────── */

export function JobDatasetsTab({ job, onRemoveDataset, onEdit }: JobDatasetsTabProps) {
  const navigate = useNavigate();

  const columns: DataTableColumn[] = [
    {
      key: 'dataset_name',
      header: 'Dataset',
      render: (row) => (
        <button
          className="font-medium text-sm text-left hover:underline text-blue-600"
          onClick={(e) => { e.stopPropagation(); navigate(`/datasets/${row.dataset_id}`); }}
        >
          {row.dataset_name}
        </button>
      ),
    },
    {
      key: 'bronze_table',
      header: 'Tabela Bronze',
      render: (row) => (
        <code className="text-xs">{row.bronze_table || '—'}</code>
      ),
    },
    {
      key: 'load_strategy',
      header: 'Estratégia',
      render: (row) => <StrategyBadge strategy={row.load_strategy || row.load_type} />,
    },
    {
      key: 'source_type',
      header: 'Fonte',
      render: (row) => (
        <Badge variant="outline" className="text-xs">{row.source_type || '—'}</Badge>
      ),
    },
    {
      key: 'volume',
      header: 'Volume',
      render: (row) => {
        const bronze = row.bronze_row_count;
        const silver = row.silver_row_count;
        if (bronze == null && silver == null)
          return <span className="text-xs text-muted-foreground">—</span>;
        return (
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-xs font-mono cursor-help">
                  {formatRows(bronze)} / {formatRows(silver)}
                </span>
              </TooltipTrigger>
              <TooltipContent side="top" className="text-xs">
                <p>Bronze: {bronze?.toLocaleString('pt-BR') ?? '—'} linhas</p>
                <p>Silver: {silver?.toLocaleString('pt-BR') ?? '—'} linhas</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        );
      },
    },
    {
      key: 'health',
      header: 'Saúde',
      render: (row) => <DatasetHealth dataset={row} />,
    },
    {
      key: 'watermark',
      header: 'Watermark',
      render: (row) => {
        const strategy = (row.load_strategy || row.load_type || '').toUpperCase();
        if (strategy !== 'INCREMENTAL')
          return <span className="text-xs text-muted-foreground">—</span>;
        return <WatermarkValue dataset={row} />;
      },
    },
    {
      key: 'status',
      header: 'Status',
      render: (row) => (
        <Badge variant="outline" className="text-xs">{row.status || '—'}</Badge>
      ),
    },
    {
      key: 'actions',
      header: '',
      className: 'text-right',
      render: (row) => (
        <div onClick={(e) => e.stopPropagation()}>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onRemoveDataset(row.dataset_id)}
            className="text-red-600 hover:text-red-700"
          >
            Remover
          </Button>
        </div>
      ),
    },
  ];

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            Datasets Associados
            <InfoTooltip fieldKey="dataset_count" />
          </CardTitle>
          <Button variant="outline" size="sm" onClick={onEdit}>
            <Edit className="h-4 w-4 mr-1" /> Gerenciar Datasets
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <DataTable
          columns={columns}
          data={job.datasets || []}
          rowKey={(row) => row.dataset_id}
          emptyIcon={<Database className="h-10 w-10 opacity-50" />}
          emptyMessage="Nenhum dataset associado a este job"
        />
      </CardContent>
    </Card>
  );
}
