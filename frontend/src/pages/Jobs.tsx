import { useState, useEffect } from 'react';
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
} from 'lucide-react';
import * as api from '@/lib/api';

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

  const fetchJobs = async () => {
    setLoading(true);
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
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, [page, filterEnabled, filterProject, filterArea]);

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
    if (!confirm('Tem certeza que deseja executar este job agora?')) return;
    
    try {
      const result = await api.runJobNow(jobId);
      alert(`Job iniciado! Run ID: ${result.databricks_run_id}`);
      fetchJobs();
    } catch (error: any) {
      console.error('Error running job:', error);
      alert(`Erro ao executar job: ${error.message}`);
    }
  };

  const handleDelete = async (jobId: string, jobName: string) => {
    if (!confirm(`Tem certeza que deseja deletar o job "${jobName}"? Esta ação não pode ser desfeita.`)) return;
    
    try {
      // TODO: Add delete endpoint
      alert('Funcionalidade de deletar será implementada');
      fetchJobs();
    } catch (error) {
      console.error('Error deleting job:', error);
      alert('Erro ao deletar job');
    }
  };

  const getStatusIcon = (status: string) => {
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
              onClick={fetchJobs}
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
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Job</TableHead>
                <TableHead>Agendamento</TableHead>
                <TableHead className="text-center">Datasets</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Última Execução</TableHead>
                <TableHead>Próxima Execução</TableHead>
                <TableHead className="text-right">Ações</TableHead>
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
                      <Button
                        variant="outline"
                        size="sm"
                        className="mt-3"
                        onClick={() => navigate('/jobs/create')}
                      >
                        <Plus className="h-4 w-4 mr-1" /> Criar primeiro job
                      </Button>
                    )}
                  </TableCell>
                </TableRow>
              ) : (
                filteredJobs.map((job) => (
                  <TableRow
                    key={job.job_id}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => navigate(`/jobs/${job.job_id}`)}
                  >
                    <TableCell>
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <p className="font-medium text-sm truncate">{job.job_name}</p>
                          {!job.enabled && (
                            <Badge variant="outline" className="text-xs bg-gray-100">
                              Inativo
                            </Badge>
                          )}
                        </div>
                        {job.description && (
                          <p className="text-xs text-muted-foreground truncate mt-0.5">
                            {job.description}
                          </p>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline" className="text-xs">
                        {formatSchedule(job.schedule_type, job.cron_expression)}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-center">
                      <div className="flex items-center justify-center gap-1">
                        <Database className="h-3 w-3 text-muted-foreground" />
                        <span className="text-sm font-medium">{job.dataset_count || 0}</span>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        {getStatusIcon(job.last_run_status)}
                        {job.last_run_status && (
                          <span className="text-xs text-muted-foreground">
                            {formatDuration(job.last_run_duration_ms)}
                          </span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <span className="text-xs whitespace-nowrap">
                        {formatDateTime(job.last_run_at)}
                      </span>
                    </TableCell>
                    <TableCell>
                      <span className="text-xs whitespace-nowrap text-blue-600">
                        {formatDateTime(job.next_run_at)}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex items-center justify-end gap-1" onClick={(e) => e.stopPropagation()}>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 w-7 p-0"
                          onClick={() => handleRunNow(job.job_id)}
                          title="Executar agora"
                        >
                          <Play className="h-3 w-3" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 w-7 p-0"
                          onClick={() => handleToggleJob(job.job_id, job.enabled)}
                          title={job.enabled ? 'Desativar' : 'Ativar'}
                        >
                          {job.enabled ? (
                            <Pause className="h-3 w-3" />
                          ) : (
                            <Play className="h-3 w-3" />
                          )}
                        </Button>
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
                            <DropdownMenuItem
                              className="text-red-600"
                              onClick={() => handleDelete(job.job_id, job.job_name)}
                            >
                              <Trash2 className="h-3 w-3 mr-2" /> Deletar
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
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
    </div>
  );
};

export default Jobs;
