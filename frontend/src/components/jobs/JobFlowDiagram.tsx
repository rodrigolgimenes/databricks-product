import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import {
  Database, ArrowRight, Download, HardDrive, Layers,
  CheckCircle2, XCircle, Loader2, Clock, AlertTriangle,
  ArrowDown, Copy, ArrowDownUp, Workflow,
} from 'lucide-react';
import { Job } from './helpers';
import { InfoTooltip } from './InfoTooltip';
import { FIELD_EXPLANATIONS } from '@/lib/field-explanations';

interface JobFlowDiagramProps {
  job: Job;
  activeQueue?: any[];
}

/* ── Types ──────────────────────────────────────── */

interface PipelineStep {
  key: string;
  label: string;
  icon: typeof Download;
  description: string;
}

const PIPELINE_STEPS: PipelineStep[] = [
  {
    key: 'READ_SOURCE',
    label: 'Leitura',
    icon: Download,
    description: 'Extração dos dados da fonte de origem (Oracle, SharePoint, etc.)',
  },
  {
    key: 'WRITE_BRONZE',
    label: 'Bronze',
    icon: HardDrive,
    description: 'Escrita na camada Bronze — cópia fiel dos dados da origem, sem transformação.',
  },
  {
    key: 'WRITE_SILVER',
    label: 'Silver',
    icon: Layers,
    description: 'Escrita na camada Silver — dados limpos, padronizados e prontos para análise.',
  },
];

/* ── Status Helpers ─────────────────────────────── */

type StepStatus = 'success' | 'running' | 'failed' | 'pending' | 'idle';

const STATUS_STYLES: Record<StepStatus, {
  bg: string; border: string; text: string; iconCls: string; icon: typeof CheckCircle2;
}> = {
  success: { bg: 'bg-green-50', border: 'border-green-300', text: 'text-green-700', iconCls: 'text-green-600', icon: CheckCircle2 },
  running: { bg: 'bg-blue-50', border: 'border-blue-300', text: 'text-blue-700', iconCls: 'text-blue-600 animate-spin', icon: Loader2 },
  failed:  { bg: 'bg-red-50', border: 'border-red-300', text: 'text-red-700', iconCls: 'text-red-600', icon: XCircle },
  pending: { bg: 'bg-yellow-50', border: 'border-yellow-200', text: 'text-yellow-700', iconCls: 'text-yellow-500', icon: Clock },
  idle:    { bg: 'bg-gray-50', border: 'border-gray-200', text: 'text-gray-500', iconCls: 'text-gray-400', icon: Clock },
};

function getDatasetStepStatus(dataset: any, activeQueue?: any[]): StepStatus {
  // Check active queue for running/pending status
  const queueItem = activeQueue?.find((q) => q.dataset_id === dataset.dataset_id);
  if (queueItem) {
    const qs = (queueItem.status || '').toUpperCase();
    if (qs === 'RUNNING' || qs === 'CLAIMED') return 'running';
    if (qs === 'PENDING') return 'pending';
    if (qs === 'FAILED') return 'failed';
  }

  const status = (dataset.last_execution_status || dataset.status || '').toUpperCase();
  if (status === 'SUCCEEDED' || status === 'SUCCESS') return 'success';
  if (status === 'FAILED') return 'failed';
  if (status === 'RUNNING' || status === 'CLAIMED') return 'running';
  if (status === 'PENDING' || status === 'QUEUED') return 'pending';
  return 'idle';
}

/* ── Strategy Badge ─────────────────────────────── */

const STRATEGY_ICONS: Record<string, typeof Copy> = {
  FULL: Copy,
  INCREMENTAL: ArrowDown,
  SNAPSHOT: ArrowDownUp,
};

function StrategyBadge({ strategy }: { strategy?: string }) {
  const s = (strategy || '').toUpperCase();
  const Icon = STRATEGY_ICONS[s] || Copy;
  const colors: Record<string, string> = {
    FULL: 'bg-blue-100 text-blue-800 border-blue-200',
    INCREMENTAL: 'bg-green-100 text-green-800 border-green-200',
    SNAPSHOT: 'bg-purple-100 text-purple-800 border-purple-200',
  };

  return (
    <span className={`inline-flex items-center gap-1 px-1.5 py-0 rounded border text-[10px] font-medium ${colors[s] || 'bg-gray-100 text-gray-700 border-gray-200'}`}>
      <Icon className="h-2.5 w-2.5" />
      {s || '—'}
    </span>
  );
}

/* ── Arrow connector ────────────────────────────── */

function FlowArrow() {
  return (
    <div className="flex items-center justify-center px-1 flex-shrink-0">
      <ArrowRight className="h-4 w-4 text-muted-foreground/40" />
    </div>
  );
}

/* ── Step Node ──────────────────────────────────── */

function StepNode({ step, status, operation }: {
  step: PipelineStep;
  status: StepStatus;
  operation?: string;
}) {
  const style = STATUS_STYLES[status];
  const StepIcon = step.icon;
  const StatusIcon = style.icon;

  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className={`flex flex-col items-center gap-1 p-2.5 rounded-lg border cursor-help transition-colors min-w-[76px] ${style.bg} ${style.border}`}>
            <div className="flex items-center gap-1.5">
              <StepIcon className={`h-4 w-4 ${style.text}`} />
              <StatusIcon className={`h-3 w-3 ${style.iconCls}`} />
            </div>
            <span className={`text-[10px] font-medium ${style.text}`}>{step.label}</span>
            {operation && (
              <span className="text-[9px] text-muted-foreground font-mono">{operation}</span>
            )}
          </div>
        </TooltipTrigger>
        <TooltipContent side="bottom" className="text-xs max-w-[220px]">
          <p className="font-medium">{step.label}</p>
          <p className="text-muted-foreground mt-0.5">{step.description}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/* ── Dataset Pipeline Card ──────────────────────── */

function DatasetPipelineCard({ dataset, activeQueue }: {
  dataset: any;
  activeQueue?: any[];
}) {
  const overallStatus = getDatasetStepStatus(dataset, activeQueue);
  const borderColor = STATUS_STYLES[overallStatus].border;
  const strategy = (dataset.load_strategy || dataset.load_type || '').toUpperCase();
  const bronzeMode = (dataset.bronze_mode || '').toUpperCase();

  // Determine operation type for bronze write
  const bronzeOp = strategy === 'INCREMENTAL' ? (bronzeMode || 'MERGE') : 'OVERWRITE';

  // For the step statuses, if dataset succeeded all steps are success.
  // If failed, we simulate the step that likely failed based on error patterns.
  const stepStatuses: StepStatus[] = (() => {
    if (overallStatus === 'success') return ['success', 'success', 'success'];
    if (overallStatus === 'running') return ['success', 'running', 'idle'];
    if (overallStatus === 'pending') return ['pending', 'idle', 'idle'];
    if (overallStatus === 'failed') {
      // Heuristic: if bronze_row_count exists, READ succeeded
      if (dataset.bronze_row_count > 0) return ['success', 'success', 'failed'];
      if (dataset.bronze_row_count === 0) return ['success', 'failed', 'idle'];
      return ['failed', 'idle', 'idle'];
    }
    return ['idle', 'idle', 'idle'];
  })();

  return (
    <div className={`relative flex items-center gap-0 p-3 rounded-xl border-2 bg-white ${borderColor}`}>
      {/* Dataset info */}
      <div className="flex flex-col gap-1 mr-3 min-w-[120px] max-w-[160px]">
        <div className="flex items-center gap-1.5">
          <Database className={`h-4 w-4 flex-shrink-0 ${STATUS_STYLES[overallStatus].text}`} />
          <span className="text-sm font-semibold truncate" title={dataset.dataset_name}>
            {dataset.dataset_name}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <StrategyBadge strategy={strategy} />
          {dataset.source_type && (
            <span className="text-[10px] text-muted-foreground">{dataset.source_type}</span>
          )}
        </div>
      </div>

      {/* Flow: READ → BRONZE → SILVER */}
      <div className="flex items-center gap-0">
        <StepNode step={PIPELINE_STEPS[0]} status={stepStatuses[0]} operation={dataset.source_type} />
        <FlowArrow />
        <StepNode step={PIPELINE_STEPS[1]} status={stepStatuses[1]} operation={bronzeOp} />
        <FlowArrow />
        <StepNode step={PIPELINE_STEPS[2]} status={stepStatuses[2]} operation="DELTA" />
      </div>

      {/* Error indicator */}
      {overallStatus === 'failed' && (
        <div className="absolute -top-2 -right-2">
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="flex items-center justify-center h-5 w-5 rounded-full bg-red-600 cursor-help">
                  <AlertTriangle className="h-3 w-3 text-white" />
                </div>
              </TooltipTrigger>
              <TooltipContent side="top" className="text-xs">
                Última execução deste dataset falhou
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      )}
    </div>
  );
}

/* ── Main Component ─────────────────────────────── */

export function JobFlowDiagram({ job, activeQueue }: JobFlowDiagramProps) {
  const datasets = job.datasets || [];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Workflow className="h-5 w-5" />
          Pipeline de Execução
          <InfoTooltip fieldKey="pipeline_diagram" />
        </CardTitle>
      </CardHeader>
      <CardContent>
        {datasets.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">
            <Database className="h-10 w-10 mx-auto mb-2 opacity-50" />
            <p className="text-sm">Nenhum dataset associado para visualizar o pipeline</p>
          </div>
        ) : (
          <div className="space-y-4">
            {/* Legend */}
            <div className="flex items-center gap-4 text-[10px] text-muted-foreground border-b pb-3">
              <span className="font-medium">Legenda:</span>
              <span className="flex items-center gap-1">
                <span className="w-2.5 h-2.5 rounded-full bg-green-500" /> Sucesso
              </span>
              <span className="flex items-center gap-1">
                <span className="w-2.5 h-2.5 rounded-full bg-blue-500 animate-pulse" /> Executando
              </span>
              <span className="flex items-center gap-1">
                <span className="w-2.5 h-2.5 rounded-full bg-yellow-500" /> Na Fila
              </span>
              <span className="flex items-center gap-1">
                <span className="w-2.5 h-2.5 rounded-full bg-red-500" /> Erro
              </span>
              <span className="flex items-center gap-1">
                <span className="w-2.5 h-2.5 rounded-full bg-gray-400" /> Inativo
              </span>
            </div>

            {/* Job header node */}
            <div className="flex items-center gap-3 mb-2">
              <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-primary/5 border border-primary/20">
                <Workflow className="h-5 w-5 text-primary" />
                <div>
                  <p className="text-sm font-bold">{job.job_name}</p>
                  <p className="text-[10px] text-muted-foreground">
                    {datasets.length} dataset{datasets.length !== 1 ? 's' : ''} no pipeline
                  </p>
                </div>
              </div>
            </div>

            {/* Dataset pipeline cards */}
            <div className="space-y-3 pl-6 border-l-2 border-primary/20 ml-3">
              {datasets.map((dataset: any) => (
                <div key={dataset.dataset_id} className="relative">
                  {/* Connector dot */}
                  <div className="absolute -left-[31px] top-1/2 -translate-y-1/2 w-3 h-3 rounded-full bg-primary/20 border-2 border-primary/40" />
                  {/* Connector line */}
                  <div className="absolute -left-[19px] top-1/2 w-[19px] h-[2px] bg-primary/20" />
                  <DatasetPipelineCard dataset={dataset} activeQueue={activeQueue} />
                </div>
              ))}
            </div>

            {/* Summary badges */}
            <div className="flex items-center gap-3 pt-3 border-t text-xs text-muted-foreground">
              <span>Estratégias:</span>
              {['FULL', 'INCREMENTAL', 'SNAPSHOT'].map((s) => {
                const count = datasets.filter((d: any) =>
                  (d.load_strategy || d.load_type || '').toUpperCase() === s
                ).length;
                if (count === 0) return null;
                return (
                  <Badge key={s} variant="outline" className="text-[10px]">
                    {s}: {count}
                  </Badge>
                );
              })}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
