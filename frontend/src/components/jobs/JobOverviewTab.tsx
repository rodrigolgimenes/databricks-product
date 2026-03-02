import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Job, formatDateTime, formatDuration, getStatusIcon } from './helpers';
import { LabelWithHelp } from './InfoTooltip';
import { CronHumanReadable } from './CronHumanReadable';

interface JobOverviewTabProps {
  job: Job;
}

export function JobOverviewTab({ job }: JobOverviewTabProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Configuração do Job</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <LabelWithHelp label="ID do Job" fieldKey="job_id" className="text-xs text-muted-foreground mb-1" />
            <code className="text-xs bg-muted px-2 py-1 rounded block mt-1">{job.job_id}</code>
          </div>
          <div>
            <LabelWithHelp label="Databricks Job ID" fieldKey="databricks_job_id" className="text-xs text-muted-foreground mb-1" />
            <code className="text-xs bg-muted px-2 py-1 rounded block mt-1">{job.databricks_job_id || '—'}</code>
          </div>
          <div>
            <LabelWithHelp label="Tipo de Agendamento" fieldKey="schedule_type" className="text-xs text-muted-foreground mb-1" />
            <Badge variant="outline" className="mt-1">{job.schedule_type}</Badge>
          </div>
          <div>
            <LabelWithHelp label="Agendamento" fieldKey="cron_expression" className="text-xs text-muted-foreground mb-1" />
            <div className="mt-1">
              <CronHumanReadable expression={job.cron_expression} />
            </div>
          </div>
          <div>
            <LabelWithHelp label="Fuso Horário" fieldKey="timezone" className="text-xs text-muted-foreground mb-1" />
            <p className="text-sm mt-1">{job.timezone}</p>
          </div>
          <div>
            <LabelWithHelp label="Próxima Execução" fieldKey="next_run_at" className="text-xs text-muted-foreground mb-1" />
            <p className="text-sm text-blue-600 font-medium mt-1">{formatDateTime(job.next_run_at)}</p>
          </div>
          <div>
            <LabelWithHelp label="Última Execução" fieldKey="last_run_at" className="text-xs text-muted-foreground mb-1" />
            <div className="flex items-center gap-2 mt-1">
              {getStatusIcon(job.last_run_status)}
              <p className="text-sm">{formatDateTime(job.last_run_at)}</p>
            </div>
          </div>
          <div>
            <LabelWithHelp label="Duração Última Exec." fieldKey="last_run_duration" className="text-xs text-muted-foreground mb-1" />
            <p className="text-sm mt-1">{formatDuration(job.last_run_duration_ms)}</p>
          </div>
          <div>
            <LabelWithHelp label="Execuções Paralelas" fieldKey="max_concurrent_runs" className="text-xs text-muted-foreground mb-1" />
            <p className="text-sm mt-1">{job.max_concurrent_runs}</p>
          </div>
          <div>
            <LabelWithHelp label="Timeout" fieldKey="timeout_seconds" className="text-xs text-muted-foreground mb-1" />
            <p className="text-sm mt-1">{job.timeout_seconds}s</p>
          </div>
          <div>
            <LabelWithHelp label="Retry em Timeout" fieldKey="retry_on_timeout" className="text-xs text-muted-foreground mb-1" />
            <Badge variant={job.retry_on_timeout ? 'default' : 'outline'} className="mt-1">
              {job.retry_on_timeout ? 'Sim' : 'Não'}
            </Badge>
          </div>
          <div>
            <LabelWithHelp label="Status" fieldKey="job_status" className="text-xs text-muted-foreground mb-1" />
            <Badge variant={job.enabled ? 'default' : 'secondary'} className="mt-1">
              {job.enabled ? 'Ativo' : 'Inativo'}
            </Badge>
          </div>
        </div>

        <div className="mt-6 pt-4 border-t">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <LabelWithHelp label="Criado em" fieldKey="created_at" className="text-xs text-muted-foreground mb-1" />
              <p className="text-sm mt-1">{formatDateTime(job.created_at)}</p>
              <p className="text-xs text-muted-foreground">por {job.created_by}</p>
            </div>
            <div>
              <LabelWithHelp label="Última atualização" fieldKey="updated_at" className="text-xs text-muted-foreground mb-1" />
              <p className="text-sm mt-1">{formatDateTime(job.updated_at)}</p>
              {job.updated_by && (
                <p className="text-xs text-muted-foreground">por {job.updated_by}</p>
              )}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
