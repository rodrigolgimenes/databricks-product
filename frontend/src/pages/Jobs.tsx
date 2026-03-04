import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  Calendar,
  Play,
  Pause,
  Edit,
  Trash2,
  MoreVertical,
  Plus,
  Search,
  RefreshCw,
  CheckCircle,
  XCircle,
  Clock,
  Database,
  Filter,
  Loader2,
  AlertTriangle,
  Square,
} from 'lucide-react';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import * as api from '@/lib/api';
import { toast } from 'sonner';

interface Job {
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
  created_at: string;
  created_by: string;
  latest_execution_status?: string;
}

const Jobs = () => {
  const navigate = useNavigate();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [filterEnabled, setFilterEnabled] = useState<string>('all');
  const [filterProject, setFilterProject] = useState<string>('');
  const [filterArea, setFilterArea] = useState<string>('');
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const pageSize = 25;

  const [projects, setProjects] = useState<any[]>([]);
  const [areas, setAreas] = useState<any[]>([]);

  // Run-now loading state
  const [runningJobId, setRunningJobId] = useState<string | null>(null);
  // Cancel loading state
  const [cancellingJobId, setCancellingJobId] = useState<string | null>(null);

  // Delete confirmation state
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; name: string } | null>(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState('');
  const [deleting, setDeleting] = useState(false);

  // Resizable columns
  const [colWidths, setColWidths] = useState([250, 120, 80, 150, 155, 155, 110]);
  const dragRef = useRef<{ idx: number; startX: number; startW: number } | null>(null);

  const onColResize = useCallback((idx: number, e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragRef.current = { idx, startX: e.clientX, startW: colWidths[idx] };

    const onMove = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      const { idx: ci, startX, startW } = dragRef.current;
      setColWidths(prev => {
        const next = [...prev];
        next[ci] = Math.max(60, startW + (ev.clientX - startX));
        return next;
      });
    };

    const onUp = () => {
      dragRef.current = null;
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  }, [colWidths]);

  useEffect(() => {
    api.getProjects().then((d) => setProjects(d.items || [])).catch(console.error);
  }, []);

  useEffect(() => {
    if (filterProject) {
      api.getAreas(filterProject).then((d) => setAreas(d.items || [])).catch(console.error);
    } else {
      setAreas([]);
    }
  }, [filterProject]);

  const fetchJobs = async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const params: any = { page, page_size: pageSize };
      if (filterEnabled !== 'all') params.enabled = filterEnabled;
      if (filterProject) params.project_id = filterProject;
      if (filterArea) params.area_id = filterArea;

      const data = await api.getJobs(params);
      setJobs(data.jobs || []);
      setTotal(data.total || 0);
    } catch (error) {
      console.error('Error fetching jobs:', error);
    } finally {
      if (!silent) setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, [page, filterEnabled, filterProject, filterArea]);

  // Auto-refresh when jobs are running
  useEffect(() => {
    const hasRunning = jobs.some(j => {
      const s = (j.latest_execution_status || j.last_run_status || '').toUpperCase();
      return s === 'RUNNING';
    });
    if (!hasRunning) return;
    const id = setTimeout(() => fetchJobs(true), 8000);
    return () => clearTimeout(id);
  }, [jobs]);

  const handleToggleJob = async (jobId: string, currentState: boolean) => {
    try {
      await api.toggleJob(jobId);
      fetchJobs();
    } catch (error) {
      console.error('Error toggling job:', error);
      alert('Erro ao ativar/desativar job');
    }
  };

  const handleRunNow = async (jobId: string) => {
    if (runningJobId) return; // already processing a run
    setRunningJobId(jobId);
    try {
      const result = await api.runJobNow(jobId);
      toast.success(`Job iniciado com sucesso! ${result.message || ''}`);
      fetchJobs();
    } catch (error: any) {
      console.error('Error running job:', error);
      if (error.message?.includes('em andamento')) {
        toast.warning(error.message);
      } else {
        toast.error(`Erro ao executar job: ${error.message}`);
      }
    } finally {
      setRunningJobId(null);
    }
  };

  const handleCancelJob = async (jobId: string) => {
    if (cancellingJobId) return;
    setCancellingJobId(jobId);
    try {
      const result = await api.cancelJob(jobId);
      toast.success(result.message || 'Job cancelado com sucesso!');
      fetchJobs();
    } catch (error: any) {
      console.error('Error cancelling job:', error);
      toast.error(`Erro ao cancelar job: ${error.message}`);
    } finally {
      setCancellingJobId(null);
    }
  };

  const openDeleteDialog = (jobId: string, jobName: string) => {
    setDeleteTarget({ id: jobId, name: jobName });
    setDeleteConfirmText('');
  };

  const handleDelete = async () => {
    if (!deleteTarget || deleteConfirmText !== 'EXCLUIR') return;
    setDeleting(true);
    try {
      await api.deleteJob(deleteTarget.id);
      setDeleteTarget(null);
      fetchJobs();
    } catch (error: any) {
      console.error('Error deleting job:', error);
      alert(`Erro ao deletar job: ${error.message}`);
    } finally {
      setDeleting(false);
    }
  };

  const getStatusDisplay = (job: Job) => {
    const status = (job.latest_execution_status || job.last_run_status || '').toUpperCase();
    switch (status) {
      case 'RUNNING':
        return { icon: <Loader2 className="h-3.5 w-3.5 animate-spin" />, label: 'Executando', color: 'text-blue-700', bg: 'bg-blue-50 border border-blue-200' };
      case 'SUCCESS':
      case 'SUCCEEDED':
        return { icon: <CheckCircle className="h-3.5 w-3.5" />, label: 'Sucesso', color: 'text-green-700', bg: 'bg-green-50 border border-green-200' };
      case 'FAILED':
        return { icon: <XCircle className="h-3.5 w-3.5" />, label: 'Erro', color: 'text-red-700', bg: 'bg-red-50 border border-red-200' };
      case 'CANCELLED':
      case 'CANCELED':
        return { icon: <XCircle className="h-3.5 w-3.5" />, label: 'Cancelado', color: 'text-orange-700', bg: 'bg-orange-50 border border-orange-200' };
      case 'TIMED_OUT':
        return { icon: <AlertTriangle className="h-3.5 w-3.5" />, label: 'Timeout', color: 'text-orange-700', bg: 'bg-orange-50 border border-orange-200' };
      default:
        return { icon: <Clock className="h-3.5 w-3.5" />, label: 'Agendado', color: 'text-gray-500', bg: 'bg-gray-50 border border-gray-200' };
    }
  };

  const formatSchedule = (scheduleType: string, cronExpression: string) => {
    if (scheduleType === 'DAILY') return 'Diário';
    if (scheduleType === 'WEEKLY') return 'Semanal';
    if (scheduleType === 'MONTHLY') return 'Mensal';
    if (scheduleType === 'ONCE') return 'Única vez';
    if (scheduleType === 'CRON') return cronExpression || 'Cron';
    return scheduleType;
  };

  const formatDuration = (ms: number) => {
    if (!ms) return '—';
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
  };

  const formatDateTime = (dateStr: string) => {
    if (!dateStr) return '—';
    return new Date(dateStr).toLocaleString('pt-BR', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const filteredJobs = jobs.filter(job => 
    job.job_name.toLowerCase().includes(search.toLowerCase()) ||
    job.description?.toLowerCase().includes(search.toLowerCase())
  );

  const activeFilters = [filterEnabled !== 'all', filterProject, filterArea].filter(Boolean).length;

  return (
    <TooltipProvider delayDuration={300}>
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Calendar className="h-6 w-6" />
            Jobs Agendados
          </h1>
          <p className="text-muted-foreground text-sm mt-0.5">
            {total} job{total !== 1 && 's'} • Página {page} de {Math.ceil(total / pageSize)}
          </p>
        </div>
        <Button size="sm" onClick={() => navigate('/jobs/create')}>
          <Plus className="h-4 w-4 mr-1" /> Criar Job
        </Button>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center gap-3 flex-wrap">
            <Filter className="h-4 w-4 text-muted-foreground" />
            
            {/* Search */}
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Buscar jobs..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-10 h-9"
              />
            </div>

            {/* Status Filter */}
            <Select value={filterEnabled} onValueChange={setFilterEnabled}>
              <SelectTrigger className="w-[140px] h-9">
                <SelectValue placeholder="Status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos</SelectItem>
                <SelectItem value="true">Ativos</SelectItem>
                <SelectItem value="false">Inativos</SelectItem>
              </SelectContent>
            </Select>

            {/* Project Filter */}
            <Select value={filterProject} onValueChange={(v) => { setFilterProject(v === '__all__' ? '' : v); setFilterArea(''); }}>
              <SelectTrigger className="w-[160px] h-9">
                <SelectValue placeholder="Projeto" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">Todos Projetos</SelectItem>
                {projects.map(p => (
                  <SelectItem key={p.project_id} value={p.project_id}>
                    {p.project_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {/* Area Filter */}
            {filterProject && (
              <Select value={filterArea} onValueChange={(v) => setFilterArea(v === '__all__' ? '' : v)}>
                <SelectTrigger className="w-[140px] h-9">
                  <SelectValue placeholder="Área" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">Todas Áreas</SelectItem>
                  {areas.map(a => (
                    <SelectItem key={a.area_id} value={a.area_id}>
                      {a.area_name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}

            {/* Active Filters Badge */}
            {activeFilters > 0 && (
              <Badge variant="secondary" className="h-7">
                {activeFilters} filtro{activeFilters > 1 && 's'}
              </Badge>
            )}

            {/* Refresh */}
            <Button
              variant="outline"
              size="sm"
              onClick={() => fetchJobs()}
              disabled={loading}
              className="h-9"
            >
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Jobs Table */}
      <Card>
        <CardContent className="p-0">
          <Table style={{ tableLayout: 'fixed' }}>
            <colgroup>
              {colWidths.map((w, i) => (
                <col key={i} style={{ width: w }} />
              ))}
            </colgroup>
            <TableHeader>
              <TableRow>
                {['Job', 'Agendamento', 'Datasets', 'Status', 'Última Execução', 'Próxima Execução', 'Ações'].map((label, idx) => (
                  <TableHead
                    key={idx}
                    className={`relative select-none overflow-hidden ${idx === 2 ? 'text-center' : ''} ${idx === 6 ? 'text-right' : ''}`}
                  >
                    <span className="truncate block">{label}</span>
                    {idx < 6 && (
                      <div
                        className="absolute right-0 top-0 h-full w-1.5 cursor-col-resize hover:bg-blue-400/50 z-10"
                        onMouseDown={(e) => onColResize(idx, e)}
                      />
                    )}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={7} className="text-center py-12">
                    <RefreshCw className="h-6 w-6 animate-spin mx-auto text-muted-foreground" />
                    <p className="text-sm text-muted-foreground mt-2">Carregando jobs...</p>
                  </TableCell>
                </TableRow>
              ) : filteredJobs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="text-center py-12">
                    <Calendar className="h-10 w-10 mx-auto text-muted-foreground mb-2" />
                    <p className="text-sm text-muted-foreground">
                      {search ? 'Nenhum job encontrado com esse filtro' : 'Nenhum job criado ainda'}
                    </p>
                    {!search && (
                      <Button variant="outline" size="sm" className="mt-3" onClick={() => navigate('/jobs/create')}>
                        <Plus className="h-4 w-4 mr-1" /> Criar primeiro job
                      </Button>
                    )}
                  </TableCell>
                </TableRow>
              ) : (
                filteredJobs.map((job) => {
                  const statusInfo = getStatusDisplay(job);
                  const isJobRunning = (job.latest_execution_status || job.last_run_status || '').toUpperCase() === 'RUNNING';
                  return (
                    <TableRow
                      key={job.job_id}
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => navigate(`/jobs/${job.job_id}`)}
                    >
                      {/* Job Name */}
                      <TableCell className="overflow-hidden">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <div className="min-w-0">
                              <div className="flex items-center gap-2">
                                <span className="font-medium text-sm truncate">{job.job_name}</span>
                                {!job.enabled && (
                                  <Badge variant="outline" className="text-xs bg-gray-100 flex-shrink-0">Inativo</Badge>
                                )}
                              </div>
                              {job.description && (
                                <p className="text-xs text-muted-foreground truncate mt-0.5">{job.description}</p>
                              )}
                            </div>
                          </TooltipTrigger>
                          <TooltipContent side="bottom" align="start" className="max-w-sm">
                            <p className="font-medium">{job.job_name}</p>
                            {job.description && <p className="text-xs text-muted-foreground mt-1">{job.description}</p>}
                          </TooltipContent>
                        </Tooltip>
                      </TableCell>

                      {/* Agendamento */}
                      <TableCell className="overflow-hidden">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span>
                              <Badge variant="outline" className="text-xs">
                                {formatSchedule(job.schedule_type, job.cron_expression)}
                              </Badge>
                            </span>
                          </TooltipTrigger>
                          <TooltipContent>Cron: {job.cron_expression || formatSchedule(job.schedule_type, job.cron_expression)}</TooltipContent>
                        </Tooltip>
                      </TableCell>

                      {/* Datasets */}
                      <TableCell className="overflow-hidden text-center">
                        <div className="flex items-center justify-center gap-1">
                          <Database className="h-3 w-3 text-muted-foreground" />
                          <span className="text-sm font-medium">{job.dataset_count || 0}</span>
                        </div>
                      </TableCell>

                      {/* Status */}
                      <TableCell className="overflow-hidden">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <div className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${statusInfo.bg} ${statusInfo.color}`}>
                              {statusInfo.icon}
                              <span>{statusInfo.label}</span>
                            </div>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>{statusInfo.label}</p>
                            {job.last_run_duration_ms > 0 && (
                              <p className="text-xs text-muted-foreground">Duração: {formatDuration(job.last_run_duration_ms)}</p>
                            )}
                          </TooltipContent>
                        </Tooltip>
                      </TableCell>

                      {/* Última Execução */}
                      <TableCell className="overflow-hidden">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="text-xs truncate block whitespace-nowrap">{formatDateTime(job.last_run_at)}</span>
                          </TooltipTrigger>
                          <TooltipContent>{formatDateTime(job.last_run_at)}</TooltipContent>
                        </Tooltip>
                      </TableCell>

                      {/* Próxima Execução */}
                      <TableCell className="overflow-hidden">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="text-xs truncate block whitespace-nowrap text-blue-600">{formatDateTime(job.next_run_at)}</span>
                          </TooltipTrigger>
                          <TooltipContent>{formatDateTime(job.next_run_at)}</TooltipContent>
                        </Tooltip>
                      </TableCell>

                      {/* Ações */}
                      <TableCell className="overflow-hidden text-right">
                        <div className="flex items-center justify-end gap-1" onClick={(e) => e.stopPropagation()}>
                          {isJobRunning ? (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0 text-red-600 hover:text-red-700 hover:bg-red-50"
                                  onClick={() => handleCancelJob(job.job_id)}
                                  disabled={cancellingJobId === job.job_id}
                                >
                                  {cancellingJobId === job.job_id ? (
                                    <Loader2 className="h-3 w-3 animate-spin" />
                                  ) : (
                                    <Square className="h-3 w-3" />
                                  )}
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent>Cancelar execução</TooltipContent>
                            </Tooltip>
                          ) : (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0"
                                  onClick={() => handleRunNow(job.job_id)}
                                  disabled={!!runningJobId}
                                >
                                  {runningJobId === job.job_id ? (
                                    <Loader2 className="h-3 w-3 animate-spin" />
                                  ) : (
                                    <Play className="h-3 w-3" />
                                  )}
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent>Executar agora</TooltipContent>
                            </Tooltip>
                          )}
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => handleToggleJob(job.job_id, job.enabled)}>
                                {job.enabled ? <Pause className="h-3 w-3" /> : <Play className="h-3 w-3" />}
                              </Button>
                            </TooltipTrigger>
                            <TooltipContent>{job.enabled ? 'Desativar' : 'Ativar'}</TooltipContent>
                          </Tooltip>
                          <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                              <Button variant="ghost" size="sm" className="h-7 w-7 p-0">
                                <MoreVertical className="h-3 w-3" />
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              <DropdownMenuItem onClick={() => navigate(`/jobs/${job.job_id}`)}>
                                <Edit className="h-3 w-3 mr-2" /> Editar
                              </DropdownMenuItem>
                              <DropdownMenuItem className="text-red-600" onClick={() => openDeleteDialog(job.job_id, job.job_name)}>
                                <Trash2 className="h-3 w-3 mr-2" /> Deletar
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Pagination */}
      {total > pageSize && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            Exibindo {Math.min((page - 1) * pageSize + 1, total)} a {Math.min(page * pageSize, total)} de {total}
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
            >
              Anterior
            </Button>
            <span className="text-sm px-3">
              Página {page} de {Math.ceil(total / pageSize)}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(p => Math.min(Math.ceil(total / pageSize), p + 1))}
              disabled={page >= Math.ceil(total / pageSize)}
            >
              Próxima
            </Button>
          </div>
        </div>
      )}

      {/* Delete Confirmation Dialog */}
      <Dialog open={!!deleteTarget} onOpenChange={(v) => { if (!v) { setDeleteTarget(null); setDeleteConfirmText(''); } }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-red-700">
              <Trash2 className="h-5 w-5" />
              Excluir Job Permanentemente
            </DialogTitle>
            <DialogDescription>
              Esta ação é <strong>irreversível</strong>. O job será removido do portal e do Databricks.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
            <div className="flex items-start gap-3 p-3 bg-red-50 border border-red-200 rounded-lg">
              <AlertTriangle className="h-5 w-5 text-red-600 mt-0.5 flex-shrink-0" />
              <div className="text-xs text-red-700 space-y-1">
                <p>Job: <strong>{deleteTarget?.name}</strong></p>
                <p>• A configuração do job será apagada permanentemente</p>
                <p>• O job será removido do Databricks</p>
                <p>• O histórico de execuções será perdido</p>
              </div>
            </div>

            <div>
              <p className="text-sm mb-2">
                Para confirmar, digite <strong className="font-mono text-red-700">EXCLUIR</strong>
              </p>
              <Input
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
                placeholder="Digite EXCLUIR"
                className={deleteConfirmText === 'EXCLUIR' ? 'border-red-500 focus-visible:ring-red-500' : ''}
                autoComplete="off"
              />
            </div>
          </div>

          <DialogFooter className="gap-2 sm:gap-0">
            <Button variant="outline" onClick={() => setDeleteTarget(null)} disabled={deleting}>
              Cancelar
            </Button>
            <Button variant="destructive" onClick={handleDelete} disabled={deleteConfirmText !== 'EXCLUIR' || deleting}>
              {deleting ? (
                <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Excluindo...</>
              ) : (
                <><Trash2 className="h-4 w-4 mr-1" /> Excluir Permanentemente</>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
    </TooltipProvider>
  );
};

export default Jobs;
