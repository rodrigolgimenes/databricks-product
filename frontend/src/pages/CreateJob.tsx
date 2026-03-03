import { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Checkbox } from '@/components/ui/checkbox';
import {
  ArrowLeft,
  ArrowRight,
  CheckCircle,
  Calendar,
  Loader2,
  Database,
  Settings,
  Save,
  AlertCircle,
  Search,
  X,
} from 'lucide-react';
import * as api from '@/lib/api';

const STEPS = ['Projeto', 'Agendamento', 'Datasets', 'Configuração', 'Revisão'];

const SCHEDULE_TYPES = [
  { value: 'DAILY', label: 'Diário', description: 'Executa uma vez por dia' },
  { value: 'WEEKLY', label: 'Semanal', description: 'Executa uma vez por semana' },
  { value: 'MONTHLY', label: 'Mensal', description: 'Executa uma vez por mês' },
  { value: 'CRON', label: 'Expressão Cron', description: 'Agendamento personalizado' },
];

const TIMEZONES = [
  { value: 'America/Sao_Paulo', label: 'São Paulo (BRT/BRST)' },
  { value: 'America/New_York', label: 'New York (EST/EDT)' },
  { value: 'America/Los_Angeles', label: 'Los Angeles (PST/PDT)' },
  { value: 'Europe/London', label: 'London (GMT/BST)' },
  { value: 'UTC', label: 'UTC' },
];

const CreateJob = () => {
  const navigate = useNavigate();
  const { jobId } = useParams();
  const isEditing = !!jobId;

  const [step, setStep] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  // Data for selects
  const [projects, setProjects] = useState<any[]>([]);
  const [areas, setAreas] = useState<any[]>([]);
  const [datasets, setDatasets] = useState<any[]>([]);
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [datasetSearch, setDatasetSearch] = useState('');

  // Form state
  const [jobName, setJobName] = useState('');
  const [description, setDescription] = useState('');
  const [projectId, setProjectId] = useState('');
  const [areaId, setAreaId] = useState('');
  const [scheduleType, setScheduleType] = useState('DAILY');
  const [cronExpression, setCronExpression] = useState('');
  const [timezone, setTimezone] = useState('America/Sao_Paulo');
  const [dailyHour, setDailyHour] = useState('02');
  const [dailyMinute, setDailyMinute] = useState('00');
  const [weeklyDay, setWeeklyDay] = useState('0'); // 0=Sunday
  const [monthlyDay, setMonthlyDay] = useState('1');
  const [maxConcurrentRuns, setMaxConcurrentRuns] = useState('1');
  const [timeoutSeconds, setTimeoutSeconds] = useState('3600');
  const [retryOnTimeout, setRetryOnTimeout] = useState(false);
  const [enabled, setEnabled] = useState(true);

  // Load initial data
  useEffect(() => {
    api.getProjects().then((d) => setProjects(d.items || [])).catch(console.error);
  }, []);

  useEffect(() => {
    if (projectId) {
      api.getAreas(projectId).then((d) => setAreas(d.items || [])).catch(console.error);
    } else {
      setAreas([]);
      setAreaId('');
    }
  }, [projectId]);

  useEffect(() => {
    if (projectId && areaId) {
      api
        .getDatasets({ project_id: projectId, area_id: areaId, page_size: 1000 })
        .then((d) => setDatasets(d.items || []))
        .catch(console.error);
    } else {
      setDatasets([]);
    }
  }, [projectId, areaId]);

  // Load existing job for editing
  useEffect(() => {
    if (isEditing && jobId) {
      setLoading(true);
      api
        .getJob(jobId)
        .then((data) => {
          // API returns { ok, job: {...}, datasets: [...] } — extract the job object
          const j = data.job || data;
          const ds = data.datasets || j.datasets || [];
          setJobName(j.job_name);
          setDescription(j.description || '');
          setProjectId(j.project_id);
          setAreaId(j.area_id);
          setScheduleType(j.schedule_type);
          setCronExpression(j.cron_expression || '');
          setTimezone(j.timezone || 'America/Sao_Paulo');
          setMaxConcurrentRuns(String(j.max_concurrent_runs || 1));
          setTimeoutSeconds(String(j.timeout_seconds || 3600));
          setRetryOnTimeout(j.retry_on_timeout || false);
          setEnabled(j.enabled);
          setSelectedDatasets(ds.map((d: any) => d.dataset_id) || []);

          // Parse schedule for UI
          if (j.cron_expression) {
            const parts = j.cron_expression.split(' ');
            if (j.schedule_type === 'DAILY' && parts.length >= 2) {
              setDailyMinute(parts[0].padStart(2, '0'));
              setDailyHour(parts[1].padStart(2, '0'));
            } else if (j.schedule_type === 'WEEKLY' && parts.length >= 5) {
              setDailyMinute(parts[0].padStart(2, '0'));
              setDailyHour(parts[1].padStart(2, '0'));
              setWeeklyDay(parts[4]);
            } else if (j.schedule_type === 'MONTHLY' && parts.length >= 3) {
              setDailyMinute(parts[0].padStart(2, '0'));
              setDailyHour(parts[1].padStart(2, '0'));
              setMonthlyDay(parts[2]);
            }
          }
        })
        .catch((err) => {
          console.error('Error loading job:', err);
          setError('Erro ao carregar job');
        })
        .finally(() => setLoading(false));
    }
  }, [jobId, isEditing]);

  const buildCronExpression = () => {
    if (scheduleType === 'CRON') return cronExpression;
    if (scheduleType === 'DAILY') return `${dailyMinute} ${dailyHour} * * *`;
    if (scheduleType === 'WEEKLY') return `${dailyMinute} ${dailyHour} * * ${weeklyDay}`;
    if (scheduleType === 'MONTHLY') return `${dailyMinute} ${dailyHour} ${monthlyDay} * *`;
    return '';
  };

  const canNext = () => {
    if (step === 0) return projectId && areaId && jobName.trim();
    if (step === 1) {
      if (scheduleType === 'CRON') return cronExpression.trim();
      return true;
    }
    if (step === 2) return selectedDatasets.length > 0;
    return true;
  };

  const handleNext = () => {
    if (canNext()) {
      setStep((s) => Math.min(STEPS.length - 1, s + 1));
      setError('');
    }
  };

  const handleBack = () => {
    setStep((s) => Math.max(0, s - 1));
    setError('');
  };

  const handleSubmit = async () => {
    setSubmitting(true);
    setError('');

    try {
      const payload = {
        job_name: jobName.trim(),
        description: description.trim() || undefined,
        project_id: projectId,
        area_id: areaId,
        schedule_type: scheduleType,
        cron_expression: buildCronExpression(),
        timezone,
        dataset_ids: selectedDatasets,
        max_concurrent_runs: parseInt(maxConcurrentRuns, 10),
        timeout_seconds: parseInt(timeoutSeconds, 10),
        retry_on_timeout: retryOnTimeout,
      };

      if (isEditing) {
        await api.updateJob(jobId, payload);
        navigate(`/jobs/${jobId}`);
      } else {
        const result = await api.createJob(payload);
        navigate(`/jobs/${result.job_id}`);
      }
    } catch (err: any) {
      console.error('Error saving job:', err);
      setError(err.message || 'Erro ao salvar job');
    } finally {
      setSubmitting(false);
    }
  };

  const toggleDataset = (datasetId: string) => {
    setSelectedDatasets((prev) =>
      prev.includes(datasetId) ? prev.filter((id) => id !== datasetId) : [...prev, datasetId]
    );
  };

  const filteredDatasets = datasets.filter((d) => {
    if (!datasetSearch.trim()) return true;
    const q = datasetSearch.toLowerCase();
    return (
      d.dataset_name?.toLowerCase().includes(q) ||
      d.bronze_table?.toLowerCase().includes(q) ||
      d.source_type?.toLowerCase().includes(q)
    );
  });

  const selectAllDatasets = () => {
    const filteredIds = filteredDatasets.map((d) => d.dataset_id);
    const allFilteredSelected = filteredIds.every((id) => selectedDatasets.includes(id));
    if (allFilteredSelected) {
      setSelectedDatasets((prev) => prev.filter((id) => !filteredIds.includes(id)));
    } else {
      setSelectedDatasets((prev) => [...new Set([...prev, ...filteredIds])]);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto space-y-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" onClick={() => navigate('/jobs')}>
          <ArrowLeft className="h-4 w-4 mr-1" /> Voltar
        </Button>
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Calendar className="h-6 w-6" />
            {isEditing ? 'Editar Job' : 'Criar Job Agendado'}
          </h1>
        </div>
      </div>

      {/* Stepper */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            {STEPS.map((label, idx) => (
              <div key={idx} className="flex items-center flex-1">
                <div className="flex flex-col items-center flex-1">
                  <div
                    className={`h-8 w-8 rounded-full flex items-center justify-center text-sm font-medium transition-colors ${
                      idx < step
                        ? 'bg-green-600 text-white'
                        : idx === step
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-600'
                    }`}
                  >
                    {idx < step ? <CheckCircle className="h-4 w-4" /> : idx + 1}
                  </div>
                  <span
                    className={`text-xs mt-1 font-medium ${
                      idx === step ? 'text-blue-600' : 'text-muted-foreground'
                    }`}
                  >
                    {label}
                  </span>
                </div>
                {idx < STEPS.length - 1 && (
                  <div
                    className={`h-0.5 flex-1 transition-colors ${
                      idx < step ? 'bg-green-600' : 'bg-gray-200'
                    }`}
                  />
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Error Message */}
      {error && (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="p-4 flex items-center gap-2 text-red-800">
            <AlertCircle className="h-5 w-5 flex-shrink-0" />
            <p className="text-sm">{error}</p>
          </CardContent>
        </Card>
      )}

      {/* Step 0: Projeto */}
      {step === 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Informações Básicas</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label htmlFor="jobName">Nome do Job *</Label>
              <Input
                id="jobName"
                value={jobName}
                onChange={(e) => setJobName(e.target.value)}
                placeholder="Ex: Ingestão Diária - Vendas"
                maxLength={100}
              />
            </div>

            <div>
              <Label htmlFor="description">Descrição</Label>
              <Textarea
                id="description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Descrição opcional do propósito deste job"
                rows={3}
              />
            </div>

            <div>
              <Label htmlFor="project">Projeto *</Label>
              <Select value={projectId} onValueChange={setProjectId}>
                <SelectTrigger id="project">
                  <SelectValue placeholder="Selecione o projeto" />
                </SelectTrigger>
                <SelectContent>
                  {projects.map((p) => (
                    <SelectItem key={p.project_id} value={p.project_id}>
                      {p.project_name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {projectId && (
              <div>
                <Label htmlFor="area">Área *</Label>
                <Select value={areaId} onValueChange={setAreaId}>
                  <SelectTrigger id="area">
                    <SelectValue placeholder="Selecione a área" />
                  </SelectTrigger>
                  <SelectContent>
                    {areas.map((a) => (
                      <SelectItem key={a.area_id} value={a.area_id}>
                        {a.area_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Step 1: Agendamento */}
      {step === 1 && (
        <Card>
          <CardHeader>
            <CardTitle>Configurar Agendamento</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label htmlFor="scheduleType">Tipo de Agendamento *</Label>
              <Select value={scheduleType} onValueChange={setScheduleType}>
                <SelectTrigger id="scheduleType">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {SCHEDULE_TYPES.map((t) => (
                    <SelectItem key={t.value} value={t.value}>
                      <div>
                        <div className="font-medium">{t.label}</div>
                        <div className="text-xs text-muted-foreground">{t.description}</div>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {scheduleType === 'DAILY' && (
              <div className="flex gap-3">
                <div className="flex-1">
                  <Label htmlFor="dailyHour">Hora *</Label>
                  <Input
                    id="dailyHour"
                    type="number"
                    min="0"
                    max="23"
                    value={dailyHour}
                    onChange={(e) => setDailyHour(e.target.value.padStart(2, '0'))}
                  />
                </div>
                <div className="flex-1">
                  <Label htmlFor="dailyMinute">Minuto *</Label>
                  <Input
                    id="dailyMinute"
                    type="number"
                    min="0"
                    max="59"
                    value={dailyMinute}
                    onChange={(e) => setDailyMinute(e.target.value.padStart(2, '0'))}
                  />
                </div>
              </div>
            )}

            {scheduleType === 'WEEKLY' && (
              <>
                <div>
                  <Label htmlFor="weeklyDay">Dia da Semana *</Label>
                  <Select value={weeklyDay} onValueChange={setWeeklyDay}>
                    <SelectTrigger id="weeklyDay">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">Domingo</SelectItem>
                      <SelectItem value="1">Segunda</SelectItem>
                      <SelectItem value="2">Terça</SelectItem>
                      <SelectItem value="3">Quarta</SelectItem>
                      <SelectItem value="4">Quinta</SelectItem>
                      <SelectItem value="5">Sexta</SelectItem>
                      <SelectItem value="6">Sábado</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex gap-3">
                  <div className="flex-1">
                    <Label htmlFor="weeklyHour">Hora *</Label>
                    <Input
                      id="weeklyHour"
                      type="number"
                      min="0"
                      max="23"
                      value={dailyHour}
                      onChange={(e) => setDailyHour(e.target.value.padStart(2, '0'))}
                    />
                  </div>
                  <div className="flex-1">
                    <Label htmlFor="weeklyMinute">Minuto *</Label>
                    <Input
                      id="weeklyMinute"
                      type="number"
                      min="0"
                      max="59"
                      value={dailyMinute}
                      onChange={(e) => setDailyMinute(e.target.value.padStart(2, '0'))}
                    />
                  </div>
                </div>
              </>
            )}

            {scheduleType === 'MONTHLY' && (
              <>
                <div>
                  <Label htmlFor="monthlyDay">Dia do Mês *</Label>
                  <Input
                    id="monthlyDay"
                    type="number"
                    min="1"
                    max="31"
                    value={monthlyDay}
                    onChange={(e) => setMonthlyDay(e.target.value)}
                  />
                </div>
                <div className="flex gap-3">
                  <div className="flex-1">
                    <Label htmlFor="monthlyHour">Hora *</Label>
                    <Input
                      id="monthlyHour"
                      type="number"
                      min="0"
                      max="23"
                      value={dailyHour}
                      onChange={(e) => setDailyHour(e.target.value.padStart(2, '0'))}
                    />
                  </div>
                  <div className="flex-1">
                    <Label htmlFor="monthlyMinute">Minuto *</Label>
                    <Input
                      id="monthlyMinute"
                      type="number"
                      min="0"
                      max="59"
                      value={dailyMinute}
                      onChange={(e) => setDailyMinute(e.target.value.padStart(2, '0'))}
                    />
                  </div>
                </div>
              </>
            )}

            {scheduleType === 'CRON' && (
              <div>
                <Label htmlFor="cronExpression">Expressão Cron *</Label>
                <Input
                  id="cronExpression"
                  value={cronExpression}
                  onChange={(e) => setCronExpression(e.target.value)}
                  placeholder="Ex: 0 2 * * *"
                />
                <p className="text-xs text-muted-foreground mt-1">
                  Formato: minuto hora dia mês dia-da-semana
                </p>
              </div>
            )}

            <div>
              <Label htmlFor="timezone">Fuso Horário *</Label>
              <Select value={timezone} onValueChange={setTimezone}>
                <SelectTrigger id="timezone">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TIMEZONES.map((tz) => (
                    <SelectItem key={tz.value} value={tz.value}>
                      {tz.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {scheduleType !== 'CRON' && (
              <Card className="bg-blue-50 border-blue-200">
                <CardContent className="p-3">
                  <p className="text-sm font-medium text-blue-900">Expressão Cron:</p>
                  <code className="text-xs text-blue-700 mt-1 block">{buildCronExpression()}</code>
                </CardContent>
              </Card>
            )}
          </CardContent>
        </Card>
      )}

      {/* Step 2: Datasets */}
      {step === 2 && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Selecionar Datasets</CardTitle>
              <Button variant="outline" size="sm" onClick={selectAllDatasets}>
                {filteredDatasets.length > 0 && filteredDatasets.every((d) => selectedDatasets.includes(d.dataset_id))
                  ? 'Desmarcar Todos'
                  : 'Selecionar Todos'}
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <div className="relative mb-3">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Buscar por nome, tabela ou tipo..."
                value={datasetSearch}
                onChange={(e) => setDatasetSearch(e.target.value)}
                className="pl-9 pr-9"
              />
              {datasetSearch && (
                <button
                  onClick={() => setDatasetSearch('')}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  <X className="h-4 w-4" />
                </button>
              )}
            </div>
            {datasets.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <Database className="h-10 w-10 mx-auto mb-2 opacity-50" />
                <p className="text-sm">Nenhum dataset disponível para este projeto/área</p>
              </div>
            ) : (
              <div className="space-y-2 max-h-96 overflow-y-auto">
                {filteredDatasets.map((dataset) => (
                  <div
                    key={dataset.dataset_id}
                    className="flex items-center gap-3 p-3 border rounded hover:bg-muted/50 cursor-pointer"
                    onClick={() => toggleDataset(dataset.dataset_id)}
                  >
                    <Checkbox
                      checked={selectedDatasets.includes(dataset.dataset_id)}
                      onCheckedChange={() => toggleDataset(dataset.dataset_id)}
                    />
                    <div className="flex-1 min-w-0">
                      <p className="font-medium text-sm truncate">{dataset.dataset_name}</p>
                      <p className="text-xs text-muted-foreground truncate">
                        {dataset.bronze_table}
                      </p>
                    </div>
                    <Badge variant="outline" className="text-xs">
                      {dataset.source_type}
                    </Badge>
                  </div>
                ))}
              </div>
            )}
            <div className="mt-4 p-3 bg-muted rounded flex items-center justify-between">
              <p className="text-sm font-medium">
                {selectedDatasets.length} dataset{selectedDatasets.length !== 1 && 's'} selecionado
                {selectedDatasets.length !== 1 && 's'}
              </p>
              {datasetSearch && (
                <p className="text-xs text-muted-foreground">
                  Exibindo {filteredDatasets.length} de {datasets.length}
                </p>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Step 3: Configuração */}
      {step === 3 && (
        <Card>
          <CardHeader>
            <CardTitle>Configurações Avançadas</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label htmlFor="maxConcurrentRuns">Execuções Paralelas Máximas</Label>
              <Input
                id="maxConcurrentRuns"
                type="number"
                min="1"
                max="10"
                value={maxConcurrentRuns}
                onChange={(e) => setMaxConcurrentRuns(e.target.value)}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Número máximo de instâncias deste job que podem rodar simultaneamente
              </p>
            </div>

            <div>
              <Label htmlFor="timeoutSeconds">Timeout (segundos)</Label>
              <Input
                id="timeoutSeconds"
                type="number"
                min="60"
                max="86400"
                value={timeoutSeconds}
                onChange={(e) => setTimeoutSeconds(e.target.value)}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Tempo máximo de execução antes do job ser cancelado
              </p>
            </div>

            <div className="flex items-center gap-2">
              <Checkbox
                id="retryOnTimeout"
                checked={retryOnTimeout}
                onCheckedChange={(checked) => setRetryOnTimeout(checked as boolean)}
              />
              <Label htmlFor="retryOnTimeout" className="cursor-pointer">
                Tentar novamente em caso de timeout
              </Label>
            </div>

            {!isEditing && (
              <div className="flex items-center gap-2">
                <Checkbox
                  id="enabled"
                  checked={enabled}
                  onCheckedChange={(checked) => setEnabled(checked as boolean)}
                />
                <Label htmlFor="enabled" className="cursor-pointer">
                  Ativar job imediatamente após criação
                </Label>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Step 4: Revisão */}
      {step === 4 && (
        <Card>
          <CardHeader>
            <CardTitle>Revisar Configurações</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-xs text-muted-foreground">Nome do Job</p>
                <p className="font-medium">{jobName}</p>
              </div>
              {description && (
                <div className="col-span-2">
                  <p className="text-xs text-muted-foreground">Descrição</p>
                  <p className="text-sm">{description}</p>
                </div>
              )}
              <div>
                <p className="text-xs text-muted-foreground">Projeto</p>
                <p className="text-sm">
                  {projects.find((p) => p.project_id === projectId)?.project_name}
                </p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Área</p>
                <p className="text-sm">{areas.find((a) => a.area_id === areaId)?.area_name}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Tipo de Agendamento</p>
                <p className="text-sm">
                  {SCHEDULE_TYPES.find((t) => t.value === scheduleType)?.label}
                </p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Expressão Cron</p>
                <code className="text-xs bg-muted px-2 py-1 rounded">{buildCronExpression()}</code>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Fuso Horário</p>
                <p className="text-sm">
                  {TIMEZONES.find((tz) => tz.value === timezone)?.label}
                </p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Datasets</p>
                <p className="text-sm font-medium">
                  {selectedDatasets.length} selecionado{selectedDatasets.length !== 1 && 's'}
                </p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Execuções Paralelas</p>
                <p className="text-sm">{maxConcurrentRuns}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Timeout</p>
                <p className="text-sm">{timeoutSeconds}s</p>
              </div>
            </div>

            <div className="pt-4 border-t">
              <div className="flex items-center gap-2">
                <Badge variant={enabled ? 'default' : 'secondary'}>
                  {enabled ? 'Ativo' : 'Inativo'}
                </Badge>
                {retryOnTimeout && <Badge variant="outline">Retry habilitado</Badge>}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Navigation */}
      <div className="flex items-center justify-between">
        <Button variant="outline" onClick={handleBack} disabled={step === 0 || submitting}>
          <ArrowLeft className="h-4 w-4 mr-1" /> Voltar
        </Button>

        {step < STEPS.length - 1 ? (
          <Button onClick={handleNext} disabled={!canNext()}>
            Próximo <ArrowRight className="h-4 w-4 ml-1" />
          </Button>
        ) : (
          <Button onClick={handleSubmit} disabled={submitting}>
            {submitting ? (
              <>
                <Loader2 className="h-4 w-4 mr-1 animate-spin" /> Salvando...
              </>
            ) : (
              <>
                <Save className="h-4 w-4 mr-1" /> {isEditing ? 'Salvar Alterações' : 'Criar Job'}
              </>
            )}
          </Button>
        )}
      </div>
    </div>
  );
};

export default CreateJob;
