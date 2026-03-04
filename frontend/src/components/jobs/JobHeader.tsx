import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import {
  ArrowLeft, Play, Pause, Edit, RefreshCw, Trash2,
  Loader2, AlertTriangle, ExternalLink, Zap, Timer,
  ShieldCheck, ShieldAlert, ShieldX, Square,
} from 'lucide-react';
import { Job, getStatusIcon, getStatusBadge } from './helpers';
import { RiskLevel } from '@/lib/job-health';
import { FIELD_EXPLANATIONS } from '@/lib/field-explanations';

interface JobHeaderProps {
  job: Job;
  recentExecutions: any[];
  activeQueue: any[];
  refreshing: boolean;
  syncing: boolean;
  deleting: boolean;
  cancelling?: boolean;
  riskLevel?: RiskLevel;
  riskReasons?: string[];
  onRefresh: () => void;
  onRunNow: () => void;
  onToggle: () => void;
  onSync: () => void;
  onDelete: () => void;
  onEdit: () => void;
  onBack: () => void;
  onCancel?: () => void;
}

const RISK_CONFIG: Record<RiskLevel, { icon: typeof ShieldCheck; color: string; badgeClass: string; label: string }> = {
  stable:   { icon: ShieldCheck, color: 'text-green-600', badgeClass: 'bg-green-100 text-green-800 border-green-300', label: 'Estável' },
  unstable: { icon: ShieldAlert, color: 'text-amber-600', badgeClass: 'bg-amber-100 text-amber-800 border-amber-300', label: 'Instável' },
  critical: { icon: ShieldX,     color: 'text-red-600',   badgeClass: 'bg-red-100 text-red-800 border-red-300',     label: 'Crítico' },
};

function RiskBadge({ level, reasons }: { level: RiskLevel; reasons: string[] }) {
  const cfg = RISK_CONFIG[level];
  const Icon = cfg.icon;
  const explanationKey = `risk_${level}` as keyof typeof FIELD_EXPLANATIONS;

  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs font-medium cursor-help ${cfg.badgeClass}`}>
            <Icon className={`h-3.5 w-3.5 ${cfg.color}`} />
            {cfg.label}
          </span>
        </TooltipTrigger>
        <TooltipContent side="bottom" className="max-w-xs text-xs space-y-1">
          <p>{FIELD_EXPLANATIONS[explanationKey]}</p>
          {reasons.length > 0 && (
            <ul className="list-disc pl-3 mt-1">
              {reasons.map((r, i) => <li key={i}>{r}</li>)}
            </ul>
          )}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

function ActionButton({
  tooltip, children, ...props
}: { tooltip: string } & React.ComponentProps<typeof Button>) {
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button {...props}>{children}</Button>
        </TooltipTrigger>
        <TooltipContent side="bottom" className="text-xs max-w-[200px]">
          {tooltip}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

export function JobHeader({
  job, recentExecutions, activeQueue,
  refreshing, syncing, deleting, cancelling,
  riskLevel, riskReasons,
  onRefresh, onRunNow, onToggle, onSync, onDelete, onEdit, onBack, onCancel,
}: JobHeaderProps) {
  const hasRunningExecution = recentExecutions.some((e) => e.status === 'RUNNING');
  return (
    <>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="sm" onClick={onBack}>
            <ArrowLeft className="h-4 w-4 mr-1" /> Voltar
          </Button>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-2xl font-bold">{job.job_name}</h1>
              {!job.enabled && (
                <Badge variant="outline" className="bg-gray-100">
                  Inativo
                </Badge>
              )}
              {riskLevel && (
                <RiskBadge level={riskLevel} reasons={riskReasons || []} />
              )}
            </div>
            {job.description && (
              <p className="text-sm text-muted-foreground mt-0.5">{job.description}</p>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <ActionButton
            variant="outline" size="sm" onClick={onRefresh} disabled={refreshing}
            tooltip={FIELD_EXPLANATIONS.action_run_now ? 'Atualizar dados da página' : 'Atualizar'}
          >
            <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
          </ActionButton>
          {hasRunningExecution && onCancel ? (
            <ActionButton
              variant="outline" size="sm" onClick={onCancel} disabled={cancelling}
              tooltip="Cancelar a execução em andamento"
              className="border-red-300 text-red-700 hover:bg-red-50"
            >
              {cancelling ? (
                <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Cancelando...</>
              ) : (
                <><Square className="h-4 w-4 mr-1" /> Cancelar Execução</>
              )}
            </ActionButton>
          ) : (
            <ActionButton
              variant="outline" size="sm" onClick={onRunNow}
              tooltip={FIELD_EXPLANATIONS.action_run_now}
            >
              <Play className="h-4 w-4 mr-1" /> Executar Agora
            </ActionButton>
          )}
          <ActionButton
            variant="outline" size="sm" onClick={onToggle}
            tooltip={FIELD_EXPLANATIONS.action_toggle}
          >
            {job.enabled ? (
              <><Pause className="h-4 w-4 mr-1" /> Desativar</>
            ) : (
              <><Play className="h-4 w-4 mr-1" /> Ativar</>
            )}
          </ActionButton>
          <ActionButton size="sm" onClick={onEdit} tooltip={FIELD_EXPLANATIONS.action_edit}>
            <Edit className="h-4 w-4 mr-1" /> Editar
          </ActionButton>
          <ActionButton
            variant="destructive" size="sm" onClick={onDelete} disabled={deleting}
            tooltip={FIELD_EXPLANATIONS.action_delete}
          >
            {deleting ? (
              <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Excluindo...</>
            ) : (
              <><Trash2 className="h-4 w-4 mr-1" /> Excluir</>
            )}
          </ActionButton>
        </div>
      </div>

      {/* Active Execution Banner */}
      {recentExecutions.some((e) => e.status === 'RUNNING') && (
        <Card className="border-blue-300 bg-blue-50">
          <CardContent className="p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <Zap className="h-5 w-5 text-blue-600" />
                <span className="font-medium text-blue-900">Execução em Andamento</span>
              </div>
              {recentExecutions.find((e) => e.status === 'RUNNING')?.run_page_url && (
                <a
                  href={recentExecutions.find((e) => e.status === 'RUNNING')!.run_page_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-sm text-blue-700 hover:text-blue-900 underline"
                >
                  <ExternalLink className="h-3 w-3" /> Ver no Databricks
                </a>
              )}
            </div>
            {activeQueue.length > 0 ? (
              <div className="space-y-2">
                <div className="flex items-center gap-4 text-xs text-blue-800 mb-2">
                  <span>Datasets na fila: <strong>{activeQueue.filter((q) => q.status === 'PENDING').length}</strong> pendentes</span>
                  <span><strong>{activeQueue.filter((q) => q.status === 'RUNNING' || q.status === 'CLAIMED').length}</strong> executando</span>
                </div>
                {activeQueue.map((q) => (
                  <div key={q.queue_id} className="flex items-center gap-2 bg-white rounded px-3 py-2 border border-blue-200">
                    {getStatusIcon(q.status)}
                    <span className="text-sm font-medium flex-1">{q.dataset_name}</span>
                    {getStatusBadge(q.status)}
                    {q.last_error_message && (
                      <span className="text-xs text-red-600 truncate max-w-[200px]" title={q.last_error_message}>
                        {q.last_error_message}
                      </span>
                    )}
                  </div>
                ))}
                <p className="text-xs text-blue-600 mt-1">
                  <Timer className="h-3 w-3 inline mr-1" />Atualizando automaticamente a cada 10s
                </p>
              </div>
            ) : (
              <p className="text-sm text-blue-700">
                Aguardando o orchestrator processar os datasets...
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Sync Warning Banner */}
      {!job.databricks_job_id && (
        <div className="bg-yellow-50 border border-yellow-300 rounded-lg p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <AlertTriangle className="h-5 w-5 text-yellow-600" />
            <div>
              <p className="text-sm font-medium text-yellow-900">
                Job não sincronizado com Databricks
              </p>
              <p className="text-xs text-yellow-700">
                O job foi criado no portal mas não foi registrado no Databricks. Clique em Sincronizar para tentar novamente.
              </p>
            </div>
          </div>
          <Button
            size="sm"
            onClick={onSync}
            disabled={syncing}
            className="bg-yellow-600 hover:bg-yellow-700"
          >
            {syncing ? (
              <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Sincronizando...</>
            ) : (
              <><RefreshCw className="h-4 w-4 mr-1" /> Sincronizar</>
            )}
          </Button>
        </div>
      )}
    </>
  );
}
