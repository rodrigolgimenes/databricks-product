import { useState, useEffect, useCallback } from "react";
import { DataTable, DataTableColumn } from "@/components/ui/data-table";
import { FilterBar, FilterDefinition } from "@/components/ui/filter-bar";
import { StatusBadge } from "@/components/ui/status-badge";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
  ExternalLink, Briefcase, AlertTriangle,
  CheckCircle2, XCircle, Info, Database, Loader2,
} from "lucide-react";
import * as api from "@/lib/api";
import { getErrorSuggestion } from "@/lib/error-suggestions";

const formatDuration = (ms: any) => {
  const v = Number(ms);
  if (!v || isNaN(v)) return "—";
  const s = Math.round(v / 1000);
  const m = Math.floor(s / 60);
  const sec = s % 60;
  if (m >= 60) {
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
  }
  return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
};

const formatTs = (ts: any) =>
  ts ? new Date(ts).toLocaleString("pt-BR") : "—";

interface JobRun {
  execution_id?: string;
  job_id: string;
  job_name: string;
  databricks_run_id?: number;
  started_at?: string;
  finished_at?: string;
  status: string;
  duration_ms?: number;
  datasets_processed?: number;
  datasets_failed?: number;
  datasets_total?: number;
  error_message?: string;
  error_class?: string;
  triggered_by?: string;
  triggered_by_user?: string;
  run_page_url?: string;
}

interface JobSummary {
  job_id: string;
  job_name: string;
  last_status: string;
  total_runs: number;
  success_count: number;
  failed_count: number;
  running_count: number;
  schedule_type?: string;
  enabled?: boolean;
}

interface JobExecutionsTabProps {
  pollingInterval: number;
  isActive: boolean;
}

export function JobExecutionsTab({ pollingInterval, isActive }: JobExecutionsTabProps) {
  const [jobs, setJobs] = useState<any[]>([]);
  const [allRuns, setAllRuns] = useState<JobRun[]>([]);
  const [filteredRuns, setFilteredRuns] = useState<JobRun[]>([]);
  const [jobSummaries, setJobSummaries] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [sortKey, setSortKey] = useState("started_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const fetchData = useCallback(async () => {
    try {
      const jobsResult = await api.getJobs({ page_size: 100 });
      const jobsList = jobsResult.items || jobsResult.jobs || [];
      setJobs(jobsList);

      const combined: JobRun[] = [];
      const summaries: JobSummary[] = [];

      await Promise.all(
        jobsList.map(async (job: any) => {
          try {
            const runsResult = await api.getJobRuns(job.job_id, { limit: 20 });
            const runs = (runsResult.items || runsResult.runs || []).map((r: any) => ({
              ...r,
              job_name: job.job_name,
              job_id: job.job_id,
            }));
            combined.push(...runs);

            const lastRun = runs[0];
            summaries.push({
              job_id: job.job_id,
              job_name: job.job_name,
              last_status: lastRun?.status || "UNKNOWN",
              total_runs: runs.length,
              success_count: runs.filter((r: any) => ["SUCCESS", "SUCCEEDED"].includes(r.status)).length,
              failed_count: runs.filter((r: any) => r.status === "FAILED").length,
              running_count: runs.filter((r: any) => ["RUNNING", "PENDING"].includes(r.status)).length,
              schedule_type: job.schedule_type,
              enabled: job.enabled,
            });
          } catch (e) {
            console.error(`Error fetching runs for job ${job.job_id}:`, e);
            summaries.push({
              job_id: job.job_id,
              job_name: job.job_name,
              last_status: "ERROR",
              total_runs: 0,
              success_count: 0,
              failed_count: 0,
              running_count: 0,
              schedule_type: job.schedule_type,
              enabled: job.enabled,
            });
          }
        })
      );

      combined.sort((a, b) => {
        const ta = a.started_at ? new Date(a.started_at).getTime() : 0;
        const tb = b.started_at ? new Date(b.started_at).getTime() : 0;
        return tb - ta;
      });

      setAllRuns(combined);
      setJobSummaries(summaries);
    } catch (e) {
      console.error("JobExecutionsTab fetch error:", e);
    } finally {
      setLoading(false);
    }
  }, []);

  // Apply filters
  useEffect(() => {
    let filtered = [...allRuns];
    if (filters.status && filters.status !== "all") {
      filtered = filtered.filter((r) => r.status === filters.status);
    }
    if (filters.job && filters.job !== "all") {
      filtered = filtered.filter((r) => r.job_id === filters.job);
    }
    if (search) {
      const q = search.toLowerCase();
      filtered = filtered.filter(
        (r) =>
          (r.job_name || "").toLowerCase().includes(q) ||
          (r.error_message || "").toLowerCase().includes(q) ||
          (r.error_class || "").toLowerCase().includes(q)
      );
    }
    // Client-side sorting
    filtered.sort((a: any, b: any) => {
      let va: any, vb: any;
      if (sortKey === "started_at") {
        va = a.started_at ? new Date(a.started_at).getTime() : 0;
        vb = b.started_at ? new Date(b.started_at).getTime() : 0;
      } else if (sortKey === "duration") {
        va = Number(a.duration_ms || 0);
        vb = Number(b.duration_ms || 0);
      } else if (sortKey === "datasets") {
        va = Number(a.datasets_processed || 0) + Number(a.datasets_failed || 0);
        vb = Number(b.datasets_processed || 0) + Number(b.datasets_failed || 0);
      } else {
        va = String(a[sortKey] || "").toLowerCase();
        vb = String(b[sortKey] || "").toLowerCase();
      }
      if (va < vb) return sortDir === "asc" ? -1 : 1;
      if (va > vb) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    setFilteredRuns(filtered);
  }, [allRuns, filters, search, sortKey, sortDir]);

  useEffect(() => {
    if (!isActive) return;
    setLoading(true);
    fetchData();
  }, [fetchData, isActive]);

  useEffect(() => {
    if (!isActive) return;
    const interval = setInterval(fetchData, pollingInterval);
    return () => clearInterval(interval);
  }, [fetchData, pollingInterval, isActive]);

  const handleFilterChange = (key: string, value: string) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
    setPage(1);
  };

  const filterDefs: FilterDefinition[] = [
    {
      key: "job",
      label: "Job",
      options: jobs.map((j: any) => ({ value: j.job_id, label: j.job_name })),
    },
    {
      key: "status",
      label: "Status",
      options: [
        { value: "SUCCESS", label: "Success" },
        { value: "SUCCEEDED", label: "Succeeded" },
        { value: "FAILED", label: "Failed" },
        { value: "RUNNING", label: "Running" },
        { value: "PENDING", label: "Pending" },
        { value: "TIMEOUT", label: "Timeout" },
        { value: "CANCELLED", label: "Cancelled" },
        { value: "PARTIAL", label: "Parcial" },
      ],
    },
  ];

  const paginatedRuns = filteredRuns.slice((page - 1) * pageSize, page * pageSize);

  const handleSort = (key: string, dir: "asc" | "desc") => {
    setSortKey(key);
    setSortDir(dir);
    setPage(1);
  };

  const columns: DataTableColumn[] = [
    {
      key: "job_name",
      header: "Job",
      sortable: true,
      render: (row) => (
        <div className="min-w-0">
          <p className="font-medium text-sm truncate">{row.job_name}</p>
          {row.triggered_by && (
            <p className="text-[10px] text-muted-foreground">
              {row.triggered_by === "SCHEDULE"
                ? "⏰ Agendado"
                : row.triggered_by === "MANUAL"
                ? "👤 Manual"
                : row.triggered_by === "API"
                ? "🔗 API"
                : row.triggered_by}
              {row.triggered_by_user && ` · ${row.triggered_by_user}`}
            </p>
          )}
        </div>
      ),
    },
    {
      key: "status",
      header: "Status",
      sortable: true,
      render: (row) => {
        const normalized = row.status === "SUCCESS" ? "SUCCEEDED" : row.status;
        return <StatusBadge status={normalized} />;
      },
    },
    {
      key: "started_at",
      header: "Início",
      sortable: true,
      render: (row) => (
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="text-xs whitespace-nowrap cursor-help">
              {formatTs(row.started_at)}
            </span>
          </TooltipTrigger>
          <TooltipContent>
            {row.finished_at && (
              <p className="text-xs">Fim: {formatTs(row.finished_at)}</p>
            )}
          </TooltipContent>
        </Tooltip>
      ),
    },
    {
      key: "duration",
      header: "Duração",
      sortable: true,
      className: "text-right",
      render: (row) => {
        const isRunning = ["RUNNING", "PENDING"].includes(row.status);
        return (
          <span
            className={`text-xs font-mono ${
              isRunning
                ? "text-blue-600 animate-pulse"
                : "text-muted-foreground"
            }`}
          >
            {formatDuration(row.duration_ms)}
          </span>
        );
      },
    },
    {
      key: "datasets",
      header: "Datasets",
      sortable: true,
      render: (row) => {
        const total = Number(row.datasets_total || 0);
        const ok = Number(row.datasets_processed || 0);
        const fail = Number(row.datasets_failed || 0);
        if (!total && !ok && !fail)
          return <span className="text-xs text-muted-foreground">—</span>;
        const processed = ok + fail;
        const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
        const isRunning = ["RUNNING", "PENDING"].includes(row.status);
        return (
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-xs">
              {ok > 0 && (
                <span className="flex items-center gap-0.5 text-green-700">
                  <CheckCircle2 className="h-3 w-3" /> {ok}
                </span>
              )}
              {fail > 0 && (
                <span className="flex items-center gap-0.5 text-red-700">
                  <XCircle className="h-3 w-3" /> {fail}
                </span>
              )}
              {total > 0 && (
                <span className="text-muted-foreground">/ {total}</span>
              )}
              {isRunning && total > 0 && (
                <span className="flex items-center gap-0.5 text-blue-600 font-medium">
                  <Loader2 className="h-3 w-3 animate-spin" /> {pct}%
                </span>
              )}
            </div>
            {total > 0 && (
              <div className="w-full bg-gray-200 rounded-full h-1.5">
                <div
                  className={`h-1.5 rounded-full transition-all ${
                    fail > 0 ? "bg-red-500" : isRunning ? "bg-blue-500" : "bg-green-500"
                  }`}
                  style={{ width: `${Math.min(pct, 100)}%` }}
                />
              </div>
            )}
          </div>
        );
      },
    },
    {
      key: "error",
      header: "Erro",
      sortable: true,
      render: (row) => {
        if (!row.error_message && !row.error_class)
          return <span className="text-xs text-muted-foreground">—</span>;
        return (
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-1 text-red-600 cursor-help max-w-[200px]">
                <AlertTriangle className="h-3 w-3 flex-shrink-0" />
                <span className="text-xs truncate">
                  {row.error_class || row.error_message}
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent className="max-w-sm">
              {row.error_class && (
                <p className="text-xs font-medium">{row.error_class}</p>
              )}
              <p className="text-xs">{row.error_message}</p>
            </TooltipContent>
          </Tooltip>
        );
      },
    },
    {
      key: "actions",
      header: "",
      className: "text-right",
      render: (row) => (
        <div
          className="flex items-center gap-1 justify-end"
          onClick={(e) => e.stopPropagation()}
        >
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
      ),
    },
  ];

  const renderExpandedRow = (row: JobRun) => {
    const suggestion = getErrorSuggestion(row.error_message, row.error_class);

    return (
      <div className="space-y-3 p-2">
        {/* Execution IDs */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
          <div>
            <p className="text-muted-foreground">ID Execução</p>
            <p className="font-mono">{row.execution_id || "—"}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Run ID Databricks</p>
            <p className="font-mono">{row.databricks_run_id || "—"}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Início</p>
            <p>{formatTs(row.started_at)}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Fim</p>
            <p>{formatTs(row.finished_at)}</p>
          </div>
        </div>

        {/* Dataset breakdown */}
        {row.datasets_total != null && (
          <div className="flex items-center gap-4 p-3 bg-muted/30 rounded-lg text-xs">
            <div className="flex items-center gap-1.5">
              <Database className="h-4 w-4 text-muted-foreground" />
              <span className="font-medium">Datasets:</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="flex items-center gap-1 text-green-700">
                <CheckCircle2 className="h-3.5 w-3.5" />{" "}
                {row.datasets_processed ?? 0} processados
              </span>
              {(row.datasets_failed ?? 0) > 0 && (
                <span className="flex items-center gap-1 text-red-700">
                  <XCircle className="h-3.5 w-3.5" /> {row.datasets_failed} com
                  erro
                </span>
              )}
              <span className="text-muted-foreground">
                Total: {row.datasets_total}
              </span>
            </div>
          </div>
        )}

        {/* Error details */}
        {row.error_message && (
          <div className="p-3 bg-red-50 border border-red-200 rounded-lg space-y-2">
            <div className="flex items-center gap-2 text-red-700 text-xs font-medium">
              <AlertTriangle className="h-4 w-4" />
              {row.error_class || "Erro na Execução"}
            </div>
            <p className="text-xs text-red-600 whitespace-pre-wrap font-mono">
              {row.error_message}
            </p>
          </div>
        )}

        {/* Suggested action */}
        {suggestion &&
          (row.status === "FAILED" || row.status === "TIMEOUT") && (
            <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-start gap-2">
              <Info className="h-4 w-4 text-amber-600 mt-0.5 flex-shrink-0" />
              <div>
                <p className="text-xs font-medium text-amber-700">
                  Ação Sugerida
                </p>
                <p className="text-xs text-amber-600 mt-0.5">{suggestion}</p>
              </div>
            </div>
          )}

        {/* Databricks link */}
        {row.run_page_url && (
          <div className="flex justify-end">
            <a
              href={row.run_page_url}
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
  };

  return (
    <TooltipProvider>
      {/* Job Health Summary Cards */}
      {jobSummaries.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
          {jobSummaries.map((js) => (
            <Card
              key={js.job_id}
              className={`cursor-pointer transition-colors ${
                filters.job === js.job_id ? "ring-2 ring-primary" : ""
              } ${
                js.running_count > 0
                  ? "border-blue-200 bg-blue-50/30"
                  : js.failed_count > 0 && js.last_status === "FAILED"
                  ? "border-red-200 bg-red-50/30"
                  : ""
              }`}
              onClick={() =>
                handleFilterChange(
                  "job",
                  filters.job === js.job_id ? "all" : js.job_id
                )
              }
            >
              <CardContent className="p-3 flex items-center justify-between">
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <Briefcase className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                    <p className="font-medium text-sm truncate">
                      {js.job_name}
                    </p>
                  </div>
                  <p className="text-[10px] text-muted-foreground mt-0.5 ml-6">
                    {js.schedule_type || "Manual"} · {js.total_runs} execuções
                    {!js.enabled && " · Desabilitado"}
                  </p>
                </div>
                <div className="flex items-center gap-1 flex-shrink-0">
                  {js.success_count > 0 && (
                    <Badge
                      variant="secondary"
                      className="bg-green-100 text-green-700 text-[10px] px-1.5"
                    >
                      ✓ {js.success_count}
                    </Badge>
                  )}
                  {js.failed_count > 0 && (
                    <Badge
                      variant="secondary"
                      className="bg-red-100 text-red-700 text-[10px] px-1.5"
                    >
                      ✗ {js.failed_count}
                    </Badge>
                  )}
                  {js.running_count > 0 && (
                    <Badge
                      variant="secondary"
                      className="bg-blue-100 text-blue-700 text-[10px] px-1.5 animate-pulse"
                    >
                      ▶ {js.running_count}
                    </Badge>
                  )}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      <FilterBar
        filters={filterDefs}
        filterValues={filters}
        onFilterChange={handleFilterChange}
        searchPlaceholder="Buscar por job, erro..."
        searchValue={search}
        onSearchChange={(v) => {
          setSearch(v);
          setPage(1);
        }}
        onRefresh={fetchData}
        refreshing={loading}
      />
      <DataTable
        columns={columns}
        data={paginatedRuns}
        total={filteredRuns.length}
        page={page}
        pageSize={pageSize}
        onPageChange={setPage}
        onPageSizeChange={setPageSize}
        sortKey={sortKey}
        sortDir={sortDir}
        onSort={handleSort}
        loading={loading}
        emptyMessage="Nenhuma execução de job encontrada."
        rowKey={(row) =>
          row.execution_id || row.databricks_run_id || String(Math.random())
        }
        rowClassName={(row) =>
          row.status === "FAILED"
            ? "bg-red-50/30"
            : ["RUNNING", "PENDING"].includes(row.status)
            ? "bg-blue-50/30"
            : row.status === "TIMEOUT"
            ? "bg-amber-50/30"
            : ""
        }
        expandableRow={renderExpandedRow}
      />
    </TooltipProvider>
  );
}
