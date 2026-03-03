import { useState, useEffect, useMemo } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { DataTable, DataTableColumn } from '@/components/ui/data-table';
import { FilterBar, FilterDefinition } from '@/components/ui/filter-bar';
import { StatusBadge } from '@/components/ui/status-badge';
import {
  Activity, Play, Calendar, ExternalLink, AlertTriangle,
  CheckCircle2, XCircle, Info, Database, Repeat, Clock, RotateCcw,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { formatDateTime, formatDuration } from './helpers';
import { InfoTooltip } from './InfoTooltip';
import { getErrorSuggestion, classifyFailureSpeed } from '@/lib/error-suggestions';

interface JobHistoryTabProps {
  runs: any[];
  onReplay?: (executionId: string) => void;
}

/* ── Error class recurrence map ─────────────────── */

function buildErrorRecurrence(runs: any[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const r of runs) {
    const cls = r.error_class;
    if (cls) counts.set(cls, (counts.get(cls) || 0) + 1);
  }
  return counts;
}

/* ── Expanded Row ─────────────────────────────── */

function ExpandedRunDetails({ run }: { run: any }) {
  const suggestion = getErrorSuggestion(run.error_message, run.error_class);
  const failureSpeed = run.status === 'FAILED' ? classifyFailureSpeed(run.duration_ms) : null;

  return (
    <div className="space-y-3 p-2">
      {/* IDs & timestamps */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
        <div>
          <p className="text-muted-foreground">ID Execução</p>
          <p className="font-mono">{run.execution_id || '—'}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Run ID Databricks</p>
          <p className="font-mono">{run.databricks_run_id || '—'}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Início</p>
          <p>{formatDateTime(run.started_at)}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Fim</p>
          <p>{formatDateTime(run.finished_at)}</p>
        </div>
      </div>

      {/* Dataset breakdown */}
      {run.datasets_total != null && (
        <div className="flex items-center gap-4 p-3 bg-muted/30 rounded-lg text-xs">
          <div className="flex items-center gap-1.5">
            <Database className="h-4 w-4 text-muted-foreground" />
            <span className="font-medium">Datasets:</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="flex items-center gap-1 text-green-700">
              <CheckCircle2 className="h-3.5 w-3.5" /> {run.datasets_processed ?? 0} processados
            </span>
            {(run.datasets_failed ?? 0) > 0 && (
              <span className="flex items-center gap-1 text-red-700">
                <XCircle className="h-3.5 w-3.5" /> {run.datasets_failed} com erro
              </span>
            )}
            <span className="text-muted-foreground">Total: {run.datasets_total}</span>
          </div>
        </div>
      )}

      {/* Error details */}
      {run.error_message && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg space-y-2">
          <div className="flex items-center gap-2 text-red-700 text-xs font-medium">
            <AlertTriangle className="h-4 w-4" />
            {run.error_class || 'Erro na Execução'}
          </div>
          <p className="text-xs text-red-600 whitespace-pre-wrap font-mono">{run.error_message}</p>
        </div>
      )}

      {/* Failure speed classification */}
      {failureSpeed && (
        <div className="p-3 bg-orange-50 border border-orange-200 rounded-lg flex items-start gap-2">
          <Clock className="h-4 w-4 text-orange-600 mt-0.5 flex-shrink-0" />
          <div>
            <p className="text-xs font-medium text-orange-700">Tempo até Falha</p>
            <p className="text-xs text-orange-600 mt-0.5">
              {formatDuration(run.duration_ms)} — {failureSpeed}
            </p>
          </div>
        </div>
      )}

      {/* Suggested action */}
      {suggestion && (run.status === 'FAILED' || run.status === 'TIMEOUT') && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-start gap-2">
          <Info className="h-4 w-4 text-amber-600 mt-0.5 flex-shrink-0" />
          <div>
            <p className="text-xs font-medium text-amber-700">Ação Sugerida</p>
            <p className="text-xs text-amber-600 mt-0.5">{suggestion}</p>
          </div>
        </div>
      )}

      {/* Databricks link */}
      {run.run_page_url && (
        <div className="flex justify-end">
          <a
            href={run.run_page_url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-xs text-blue-600 hover:underline"
          >
            <ExternalLink className="h-3.5 w-3.5" /> Abrir no Databricks
          </a>
        </div>
      )}
    </div>
  );
}

/* ── Main Component ─────────────────────────────── */

export function JobHistoryTab({ runs, onReplay }: JobHistoryTabProps) {
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const errorRecurrence = useMemo(() => buildErrorRecurrence(runs), [runs]);

  // Filter runs
  const filtered = useMemo(() => {
    let result = [...runs];
    if (filters.status && filters.status !== 'all') {
      result = result.filter((r) => r.status === filters.status);
    }
    if (filters.triggered_by && filters.triggered_by !== 'all') {
      result = result.filter((r) => r.triggered_by === filters.triggered_by);
    }
    return result;
  }, [runs, filters]);

  const paginated = filtered.slice((page - 1) * pageSize, page * pageSize);

  // Reset page when filters change
  useEffect(() => { setPage(1); }, [filters]);

  const filterDefs: FilterDefinition[] = [
    {
      key: 'status',
      label: 'Status',
      options: [
        { value: 'SUCCEEDED', label: 'Sucesso' },
        { value: 'FAILED', label: 'Falha' },
        { value: 'RUNNING', label: 'Executando' },
        { value: 'PENDING', label: 'Pendente' },
        { value: 'CANCELLED', label: 'Cancelado' },
        { value: 'PARTIAL', label: 'Parcial' },
      ],
    },
    {
      key: 'triggered_by',
      label: 'Disparado por',
      options: [
        { value: 'SCHEDULE', label: 'Agendado' },
        { value: 'MANUAL', label: 'Manual' },
      ],
    },
  ];

  const columns: DataTableColumn[] = [
    {
      key: 'status',
      header: 'Status',
      render: (row) => {
        const normalized = row.status === 'SUCCESS' ? 'SUCCEEDED' : row.status;
        return <StatusBadge status={normalized} />;
      },
    },
    {
      key: 'triggered_by',
      header: 'Disparado por',
      render: (row) => (
        <div className="text-xs">
          <div className="flex items-center gap-1">
            {row.triggered_by === 'MANUAL' ? (
              <><Play className="h-3 w-3" /> Manual</>
            ) : (
              <><Calendar className="h-3 w-3" /> Agendado</>
            )}
          </div>
          {row.triggered_by_user && (
            <span className="text-muted-foreground">por {row.triggered_by_user}</span>
          )}
        </div>
      ),
    },
    {
      key: 'started_at',
      header: 'Início',
      sortable: true,
      render: (row) => (
        <span className="text-xs whitespace-nowrap">{formatDateTime(row.started_at)}</span>
      ),
    },
    {
      key: 'duration_ms',
      header: 'Duração',
      sortable: true,
      className: 'text-right',
      render: (row) => {
        const isRunning = ['RUNNING', 'PENDING'].includes(row.status);
        return (
          <span className={`text-xs font-mono ${isRunning ? 'text-blue-600 animate-pulse' : 'text-muted-foreground'}`}>
            {formatDuration(row.duration_ms)}
          </span>
        );
      },
    },
    {
      key: 'datasets',
      header: 'Datasets',
      render: (row) => {
        const total = row.datasets_total;
        const ok = row.datasets_processed;
        const fail = row.datasets_failed;
        if (total == null && ok == null && fail == null)
          return <span className="text-xs text-muted-foreground">—</span>;
        return (
          <div className="flex items-center gap-1.5 text-xs">
            {ok != null && ok > 0 && (
              <span className="flex items-center gap-0.5 text-green-700">
                <CheckCircle2 className="h-3 w-3" /> {ok}
              </span>
            )}
            {fail != null && fail > 0 && (
              <span className="flex items-center gap-0.5 text-red-700">
                <XCircle className="h-3 w-3" /> {fail}
              </span>
            )}
            {total != null && (
              <span className="text-muted-foreground">/ {total}</span>
            )}
          </div>
        );
      },
    },
    {
      key: 'error',
      header: 'Erro',
      render: (row) => {
        if (!row.error_message && !row.error_class)
          return <span className="text-xs text-muted-foreground">—</span>;

        const recurrenceCount = row.error_class ? (errorRecurrence.get(row.error_class) || 0) : 0;

        return (
          <div className="flex items-center gap-1.5">
            <TooltipProvider delayDuration={200}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="flex items-center gap-1 text-red-600 cursor-help max-w-[180px]">
                    <AlertTriangle className="h-3 w-3 flex-shrink-0" />
                    <span className="text-xs truncate">{row.error_class || row.error_message}</span>
                  </div>
                </TooltipTrigger>
                <TooltipContent className="max-w-sm">
                  {row.error_class && <p className="text-xs font-medium">{row.error_class}</p>}
                  <p className="text-xs">{row.error_message}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            {recurrenceCount >= 2 && (
              <TooltipProvider delayDuration={200}>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="inline-flex items-center gap-0.5 px-1.5 py-0 rounded-full bg-red-100 text-red-700 text-[10px] font-medium cursor-help">
                      <Repeat className="h-2.5 w-2.5" />
                      {recurrenceCount}x
                    </span>
                  </TooltipTrigger>
                  <TooltipContent className="text-xs">
                    Este tipo de erro ({row.error_class}) ocorreu {recurrenceCount} vezes no histórico
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </div>
        );
      },
    },
    {
      key: 'actions',
      header: '',
      className: 'text-right',
      render: (row) => {
        const canReplay = onReplay && row.execution_id &&
          ['FAILED', 'PARTIAL', 'CANCELLED', 'SUCCEEDED'].includes(row.status) &&
          !['RUNNING', 'PENDING'].includes(row.status);
        return (
          <div className="flex items-center gap-1.5 justify-end" onClick={(e) => e.stopPropagation()}>
            {canReplay && (
              <Button
                variant="ghost"
                size="sm"
                className="h-6 px-2 text-xs gap-1"
                onClick={() => onReplay(row.execution_id)}
              >
                <RotateCcw className="h-3 w-3" /> Retomar
              </Button>
            )}
            {row.run_page_url && (
              <a
                href={row.run_page_url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-xs text-blue-600 hover:underline"
              >
                <ExternalLink className="h-3 w-3" /> Databricks
              </a>
            )}
          </div>
        );
      },
    },
  ];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          Histórico de Execuções
          <InfoTooltip fieldKey="total_runs" />
        </CardTitle>
      </CardHeader>
      <CardContent>
        <FilterBar
          filters={filterDefs}
          filterValues={filters}
          onFilterChange={(key, value) => setFilters((prev) => ({ ...prev, [key]: value }))}
        />
        <DataTable
          columns={columns}
          data={paginated}
          total={filtered.length}
          page={page}
          pageSize={pageSize}
          onPageChange={setPage}
          onPageSizeChange={setPageSize}
          pageSizeOptions={[10, 25, 50]}
          rowKey={(row) => row.execution_id || row.run_id || String(Math.random())}
          emptyIcon={<Activity className="h-10 w-10 opacity-50" />}
          emptyMessage="Nenhuma execução encontrada com os filtros selecionados"
          expandableRow={(row) => <ExpandedRunDetails run={row} />}
        />
      </CardContent>
    </Card>
  );
}
