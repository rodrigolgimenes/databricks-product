const BASE = "/api/portal";

async function request<T = any>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  const contentType = String(res.headers.get("content-type") || "").toLowerCase();
  const raw = await res.text();
  let data: any = null;

  if (raw) {
    if (contentType.includes("application/json")) {
      try {
        data = JSON.parse(raw);
      } catch {
        throw new Error(`Resposta JSON inválida em ${BASE}${path}`);
      }
    } else {
      // Non-JSON body (commonly HTML fallback when endpoint is missing)
      const preview = raw.slice(0, 80).replace(/\s+/g, " ");
      throw new Error(`Endpoint ${BASE}${path} retornou conteúdo não-JSON (${contentType || "desconhecido"}): ${preview}`);
    }
  }
  if (!res.ok) {
    throw new Error(data?.message || data?.error || `HTTP ${res.status}`);
  }
  return (data ?? {}) as T;
}

// Health & Meta
export const getHealth = () => request("/health");
export const getMeta = () => request("/meta");

// Dashboard
export const getDashboardSummary = (limit = 50) =>
  request(`/dashboard/summary?limit=${limit}`);
export const getOrchestratorStatus = () => request("/orchestrator/status");

// Projects & Areas & Connections
export const getProjects = () => request("/projects");
export const getAreas = (projectId?: string) =>
  request(`/areas${projectId ? `?project_id=${projectId}` : ""}`);
export const getOracleConnections = (projectId?: string, areaId?: string) => {
  const params = new URLSearchParams();
  if (projectId) params.set("project_id", projectId);
  if (areaId) params.set("area_id", areaId);
  const qs = params.toString();
  return request(`/connections/oracle${qs ? `?${qs}` : ""}`);
};

// Datasets
export const getDatasets = (params: {
  page?: number;
  page_size?: number;
  search?: string;
  sort_by?: string;
  sort_dir?: "asc" | "desc";
  status?: string;
  source_type?: string;
  project_id?: string;
  area_id?: string;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.page_size) qs.set("page_size", String(params.page_size));
  if (params.search) qs.set("search", params.search);
  if (params.sort_by) qs.set("sort_by", params.sort_by);
  if (params.sort_dir) qs.set("sort_dir", params.sort_dir);
  if (params.status) qs.set("status", params.status);
  if (params.source_type) qs.set("source_type", params.source_type);
  if (params.project_id) qs.set("project_id", params.project_id);
  if (params.area_id) qs.set("area_id", params.area_id);
  const q = qs.toString();
  return request(`/datasets${q ? `?${q}` : ""}`);
};
export const getDataset = (id: string) => request(`/datasets/${id}`);
export const createDataset = (body: {
  project_id: string;
  area_id: string;
  dataset_name: string;
  source_type: string;
  connection_id: string;
  custom_bronze_table?: string;
  custom_silver_table?: string;
  naming_version?: number;
}) => request("/datasets", { method: "POST", body: JSON.stringify(body) });

// NOVO: Preview de nomenclatura antes de criar
export const previewDatasetNaming = (body: {
  area_id: string;
  dataset_name: string;
  naming_version?: number;  // Opcional: versão da convenção a usar
}) => request("/datasets/naming-preview", { method: "POST", body: JSON.stringify(body) });

export const publishDataset = (id: string) =>
  request(`/datasets/${id}/publish`, { method: "POST" });
export const enqueueDataset = (id: string) =>
  request(`/datasets/${id}/enqueue`, { method: "POST" });
export const bulkEnqueueDatasets = (datasetIds: string[], strategy: "sequential" | "parallel" = "parallel") =>
  request("/datasets/bulk-enqueue", { 
    method: "POST", 
    body: JSON.stringify({ dataset_ids: datasetIds, strategy }) 
  });

// Incremental strategy endpoints
export const confirmIncrementalStrategy = (
  datasetId: string,
  options: {
    watermark_col?: string;
    hash_exclude_cols?: string[];
    enable_reconciliation?: boolean;
  } = {}
) =>
  request(`/datasets/${datasetId}/confirm-strategy`, {
    method: "POST",
    body: JSON.stringify(options),
  });

export const rediscoverStrategy = (datasetId: string) =>
  request(`/datasets/${datasetId}/rediscover`, { method: "POST" });

// Admin: Naming Conventions
export interface NamingConvention {
  naming_version: number;
  bronze_pattern: string;
  silver_pattern: string;
  is_active: boolean;
  created_at: string;
  created_by: string;
  notes?: string;
}

export const getNamingConventions = () =>
  request<{ ok: boolean; items: NamingConvention[] }>("/admin/naming-conventions");

export const createNamingConvention = (body: {
  bronze_pattern: string;
  silver_pattern: string;
  notes?: string;
}) => request("/admin/naming-conventions", { method: "POST", body: JSON.stringify(body) });

export const activateNamingConvention = (version: number) =>
  request(`/admin/naming-conventions/${version}/activate`, { method: "POST" });

export const deactivateNamingConvention = (version: number) =>
  request(`/admin/naming-conventions/${version}/deactivate`, { method: "POST" });

export const updateNamingConvention = (version: number, body: {
  bronze_pattern?: string;
  silver_pattern?: string;
  notes?: string;
}) => request(`/admin/naming-conventions/${version}`, { method: "PATCH", body: JSON.stringify(body) });

export const deleteNamingConvention = (version: number) =>
  request(`/admin/naming-conventions/${version}`, { method: "DELETE" });

// Bulk rename datasets
export interface BulkRenamePreview {
  dataset_id: string;
  dataset_name: string;
  status: 'PREVIEW' | 'CONFLICT' | 'ERROR' | 'RENAMED';
  old_bronze?: string;
  new_bronze?: string;
  old_silver?: string;
  new_silver?: string;
  message?: string;
}

export const previewBulkRename = (body: {
  dataset_ids: string[];
  operation: 'REPLACE_SCHEMA_PREFIX' | 'REPLACE_CATALOG' | 'REPLACE_FULL';
  bronze_from?: string;
  bronze_to?: string;
  silver_from?: string;
  silver_to?: string;
  create_schemas?: boolean;
}) => request<{ ok: boolean; preview: true; results: BulkRenamePreview[]; schemas_to_create: string[]; created_schemas: string[] }>(
  "/datasets/bulk-rename",
  { method: "POST", body: JSON.stringify({ ...body, confirm: false }) }
);

export const executeBulkRename = (body: {
  dataset_ids: string[];
  operation: 'REPLACE_SCHEMA_PREFIX' | 'REPLACE_CATALOG' | 'REPLACE_FULL';
  bronze_from?: string;
  bronze_to?: string;
  silver_from?: string;
  silver_to?: string;
  create_schemas?: boolean;
}) => request<{ ok: boolean; renamed: number; results: BulkRenamePreview[]; created_schemas: string[] }>(
  "/datasets/bulk-rename",
  { method: "POST", body: JSON.stringify({ ...body, confirm: true }) }
);

export const deleteDataset = (id: string, confirmName: string, dropTables = false) =>
  request(`/datasets/${id}`, {
    method: "DELETE",
    body: JSON.stringify({ confirm_name: confirmName, drop_tables: dropTables }),
  });

// Bulk datasets
export const validateDatasetsBulk = (body: {
  project_id: string;
  area_id: string;
  source_type: string;
  connection_id: string;
  dataset_names: string[];
}) => request("/datasets/bulk/validate", { method: "POST", body: JSON.stringify(body) });
export const createDatasetsBulk = (body: {
  project_id: string;
  area_id: string;
  source_type: string;
  connection_id: string;
  dataset_names: string[];
  naming_version?: number;
}) => request("/datasets/bulk", { method: "POST", body: JSON.stringify(body) });

// Batch create (async with progress)
export const batchCreateDatasets = (body: {
  project_id: string;
  area_id: string;
  source_type: string;
  connection_id: string;
  dataset_names: string[];
  naming_version?: number;
}) => request("/datasets/batch-create", { method: "POST", body: JSON.stringify(body) });
export const getBatchStatus = (batchId: string) =>
  request(`/datasets/batch-status/${batchId}`);

// Dataset detail endpoints
export const getDatasetRuns = (id: string, limit = 50) =>
  request(`/datasets/${id}/runs?limit=${limit}`);
export const getDatasetSchema = (id: string) =>
  request(`/datasets/${id}/schema`);
export const getDatasetPreview = (id: string, limit = 10) =>
  request(`/datasets/${id}/preview?limit=${limit}`);
export const getDatasetStateChanges = (id: string, limit = 50) =>
  request(`/datasets/${id}/state-changes?limit=${limit}`);
export const getDatasetColumnsPreview = (id: string, limit = 5) =>
  request(`/datasets/${id}/columns-preview?limit=${limit}`);

export const validatePk = (datasetId: string, pkColumns: string[], scope: 'bronze' | 'source' = 'bronze') =>
  request<{
    ok: boolean;
    unique: boolean;
    pk_columns: string[];
    total_rows: number;
    distinct_rows: number;
    duplicate_count: number;
    sample_duplicates: Array<Record<string, any>>;
    scope: string;
    table: string;
  }>(`/datasets/${datasetId}/validate-pk`, {
    method: "POST",
    body: JSON.stringify({ pk_columns: pkColumns, scope }),
  });

// Run detail types (enterprise 3-layer architecture)
export interface DatasetContext {
  incremental_strategy?: string;
  incremental_metadata?: string | Record<string, any>;
  discovery_status?: string;
  discovery_suggestion?: string;
  enable_incremental?: boolean;
  strategy_decision_log?: string | any[];
}

export interface PreviousRun {
  run_id?: string;
  silver_row_count?: number;
  bronze_row_count?: number;
  duration_seconds?: number;
  finished_at?: string;
}

export interface RunDetailsResponse {
  batch_process?: any;
  run_queue?: any;
  table_details?: any[];
  dataset_context?: DatasetContext;
  previous_run?: PreviousRun;
}

// Runs
export const getRunDetails = (runId: string) => request<RunDetailsResponse>(`/runs/${runId}`);
export const getRunStatus = (runId: string) =>
  request(`/runs/${runId}/status`);
export const getRunSteps = (runId: string) => request(`/runs/${runId}/steps`);
export const getRunQueueItem = (queueId: string) =>
  request(`/run-queue/${queueId}`);

// Approvals
export const getPendingApprovals = (limit = 50) =>
  request(`/approvals/pending?limit=${limit}`);
export const approveSchema = (
  datasetId: string,
  schemaVersion: number,
  comments = ""
) =>
  request(`/datasets/${datasetId}/schema/${schemaVersion}/approve`, {
    method: "POST",
    body: JSON.stringify({ comments }),
  });
export const rejectSchema = (
  datasetId: string,
  schemaVersion: number,
  comments = ""
) =>
  request(`/datasets/${datasetId}/schema/${schemaVersion}/reject`, {
    method: "POST",
    body: JSON.stringify({ comments }),
  });

// Monitor endpoints
export const getQueueStats = () => request("/monitor/queue/stats");
export const getRecentBatchProcesses = (params: {
  page?: number;
  page_size?: number;
  search?: string;
  status?: string;
  period?: string;
  date_from?: string;
  date_to?: string;
  sort_key?: string;
  sort_dir?: string;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.page_size) qs.set("page_size", String(params.page_size));
  if (params.search) qs.set("search", params.search);
  if (params.status) qs.set("status", params.status);
  if (params.period) qs.set("period", params.period);
  if (params.date_from) qs.set("date_from", params.date_from);
  if (params.date_to) qs.set("date_to", params.date_to);
  if (params.sort_key) qs.set("sort_key", params.sort_key);
  if (params.sort_dir) qs.set("sort_dir", params.sort_dir);
  const q = qs.toString();
  return request(`/monitor/batch-processes/recent${q ? `?${q}` : ""}`);
};
export const getQueue = (params: {
  page?: number;
  page_size?: number;
  status?: string;
  search?: string;
  sort_key?: string;
  sort_dir?: string;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.page_size) qs.set("page_size", String(params.page_size));
  if (params.status) qs.set("status", params.status);
  if (params.search) qs.set("search", params.search);
  if (params.sort_key) qs.set("sort_key", params.sort_key);
  if (params.sort_dir) qs.set("sort_dir", params.sort_dir);
  const q = qs.toString();
  return request(`/monitor/queue${q ? `?${q}` : ""}`);
};
export const getFailedJobs = (params: {
  page?: number;
  page_size?: number;
  search?: string;
  sort_key?: string;
  sort_dir?: string;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.page_size) qs.set("page_size", String(params.page_size));
  if (params.search) qs.set("search", params.search);
  if (params.sort_key) qs.set("sort_key", params.sort_key);
  if (params.sort_dir) qs.set("sort_dir", params.sort_dir);
  const q = qs.toString();
  return request(`/monitor/queue/failed${q ? `?${q}` : ""}`);
};
export const getDatasetExecutions = (datasetId: string, limit = 50) =>
  request(`/monitor/datasets/${datasetId}/executions?limit=${limit}`);
export const getExecutionSteps = (runId: string) =>
  request(`/monitor/executions/${runId}/steps`);
export const getSreDashboard = (limit = 100) =>
  request(`/monitor/sre-dashboard?limit=${limit}`);

// Databricks Jobs API
export const getDatabricksConfig = () => request("/databricks/config");
export const getDatabricksJobs = (limit = 25, name?: string) =>
  request(`/databricks/jobs?limit=${limit}${name ? `&name=${encodeURIComponent(name)}` : ""}`);
export const getDatabricksJobRuns = (jobId: string | number, limit = 10) =>
  request(`/databricks/jobs/${jobId}/runs?limit=${limit}`);
export const triggerDatabricksJob = (jobId: string | number, notebookParams: Record<string, string> = {}) =>
  request(`/databricks/jobs/${jobId}/run-now`, { method: "POST", body: JSON.stringify({ notebook_params: notebookParams }) });
export const getDatabricksRun = (runId: string | number) =>
  request(`/databricks/runs/${runId}`);
export const cancelDatabricksRun = (runId: string | number) =>
  request(`/databricks/runs/${runId}/cancel`, { method: "POST" });

// Supabase API
export const testSupabaseConnection = () => request("/supabase/test-connection");
export const getSupabaseTables = (schema?: string) =>
  request(`/supabase/tables${schema ? `?schema=${schema}` : ""}`);
export const getSupabaseTableInfo = (tableName: string, schema?: string) =>
  request(`/supabase/tables/${tableName}/info${schema ? `?schema=${schema}` : ""}`);
export const getSupabaseSchemas = () => request("/supabase/schemas");

// Projects and Areas Management
export const createProject = (body: { project_name: string; description?: string }) =>
  request("/projects", { method: "POST", body: JSON.stringify(body) });

export const createArea = (body: { project_id: string; area_name: string; description?: string }) =>
  request("/areas", { method: "POST", body: JSON.stringify(body) });

export const updateProjectName = (projectId: string, newName: string) =>
  request(`/projects/${projectId}`, {
    method: "PUT",
    body: JSON.stringify({ project_name: newName }),
  });
  
export const updateAreaName = (areaId: string, newName: string) =>
  request(`/areas/${areaId}`, {
    method: "PUT",
    body: JSON.stringify({ area_name: newName }),
  });

// Jobs API
export const getJobs = (params: {
  page?: number;
  page_size?: number;
  enabled?: string;
  project_id?: string;
  area_id?: string;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.page_size) qs.set("page_size", String(params.page_size));
  if (params.enabled) qs.set("enabled", params.enabled);
  if (params.project_id) qs.set("project_id", params.project_id);
  if (params.area_id) qs.set("area_id", params.area_id);
  const q = qs.toString();
  return request(`/jobs${q ? `?${q}` : ""}`);
};
export const getJob = (jobId: string) => request(`/jobs/${jobId}`);
export const createJob = (body: {
  job_name: string;
  description?: string;
  schedule_type: string;
  cron_expression?: string;
  timezone?: string;
  project_id: string;
  area_id: string;
  dataset_ids?: string[];
  max_concurrent_runs?: number;
  timeout_seconds?: number;
  retry_on_timeout?: boolean;
}) => request("/jobs", { method: "POST", body: JSON.stringify(body) });
export const updateJob = (jobId: string, body: {
  job_name?: string;
  description?: string;
  schedule_type?: string;
  cron_expression?: string;
  timezone?: string;
  max_concurrent_runs?: number;
  timeout_seconds?: number;
  retry_on_timeout?: boolean;
  dataset_ids?: string[];
}) => request(`/jobs/${jobId}`, { method: "PATCH", body: JSON.stringify(body) });
export const toggleJob = (jobId: string) =>
  request(`/jobs/${jobId}/toggle`, { method: "POST" });
export const runJobNow = (jobId: string) =>
  request(`/jobs/${jobId}/run-now`, { method: "POST" });
export const getJobRuns = (jobId: string, params: { limit?: number; offset?: number } = {}) => {
  const qs = new URLSearchParams();
  if (params.limit) qs.set("limit", String(params.limit));
  if (params.offset) qs.set("offset", String(params.offset));
  const q = qs.toString();
  return request(`/jobs/${jobId}/runs${q ? `?${q}` : ""}`);
};
export const addJobDatasets = (jobId: string, datasetIds: string[]) =>
  request(`/jobs/${jobId}/datasets`, {
    method: "POST",
    body: JSON.stringify({ dataset_ids: datasetIds }),
  });
export const removeJobDataset = (jobId: string, datasetId: string) =>
  request(`/jobs/${jobId}/datasets/${datasetId}`, { method: "DELETE" });
export const getJobSyncStatus = () => request("/jobs/sync-status");
export const syncJob = (jobId: string) =>
  request(`/jobs/${jobId}/sync`, { method: "POST" });
export const deleteJob = (jobId: string) =>
  request(`/jobs/${jobId}`, { method: "DELETE" });

// Partial replay
export type ReplayMode = 'REMAINING_TODAY' | 'FAILED_ONLY' | 'ALL' | 'SELECTED';

export interface ReplayPreviewDataset {
  dataset_id: string;
  dataset_name: string;
  source_type: string;
  replay_status: 'SUCCEEDED' | 'FAILED' | 'PENDING' | 'NOT_ENQUEUED';
  succeeded_today: boolean;
  error_class: string | null;
  error_message: string | null;
}

export interface ReplayPreviewResponse {
  ok: boolean;
  execution: {
    execution_id: string;
    status: string;
    started_at: string;
    finished_at: string;
    datasets_total: number;
  };
  datasets: ReplayPreviewDataset[];
  summary: {
    total: number;
    succeeded: number;
    failed: number;
    pending: number;
    not_enqueued: number;
  };
}

export const getReplayPreview = (jobId: string, executionId: string) =>
  request<ReplayPreviewResponse>(`/jobs/${jobId}/replay-preview/${executionId}`);

export const executePartialReplay = (jobId: string, body: {
  execution_id: string;
  mode: ReplayMode;
  dataset_ids?: string[];
}) => request(`/jobs/${jobId}/replay`, { method: "POST", body: JSON.stringify(body) });

// Ops Control Plane
export const getOpsRealtimeBoard = (params: {
  env?: string;
  workspace_id?: string;
  limit?: number;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.env) qs.set("env", params.env);
  if (params.workspace_id) qs.set("workspace_id", params.workspace_id);
  if (params.limit) qs.set("limit", String(params.limit));
  const q = qs.toString();
  return request(`/ops/realtime${q ? `?${q}` : ""}`);
};

export const getOpsRunDetail = (runId: string) =>
  request(`/ops/runs/${runId}`);

export const getOpsIncidents = (params: {
  status?: string;
  severity?: string;
  limit?: number;
} = {}) => {
  const qs = new URLSearchParams();
  if (params.status) qs.set("status", params.status);
  if (params.severity) qs.set("severity", params.severity);
  if (params.limit) qs.set("limit", String(params.limit));
  const q = qs.toString();
  return request(`/ops/incidents${q ? `?${q}` : ""}`);
};

export const ackOpsIncident = (incidentId: string, body: { actor: string }) =>
  request(`/ops/incidents/${incidentId}/ack`, {
    method: "POST",
    body: JSON.stringify(body),
  });

export const assignOpsIncident = (
  incidentId: string,
  body: { actor: string; owner: string }
) =>
  request(`/ops/incidents/${incidentId}/assign`, {
    method: "POST",
    body: JSON.stringify(body),
  });

export const resolveOpsIncident = (
  incidentId: string,
  body: { actor: string; resolution_notes?: string }
) =>
  request(`/ops/incidents/${incidentId}/resolve`, {
    method: "POST",
    body: JSON.stringify(body),
  });

export const getJobGuardrailConfig = (portalJobId: string) =>
  request(`/ops/jobs/${portalJobId}/guardrails`);

export const upsertJobGuardrailConfig = (
  portalJobId: string,
  body: {
    expected_frequency: "hourly" | "daily" | "weekly";
    volume_min?: number;
    volume_max?: number;
    watermark_required: boolean;
    dq_required: boolean;
    silent_failure_check: boolean;
  }
) =>
  request(`/ops/jobs/${portalJobId}/guardrails`, {
    method: "PUT",
    body: JSON.stringify(body),
  });

export const getJobBaselineMetrics = (portalJobId: string) =>
  request(`/ops/jobs/${portalJobId}/baseline`);

export const evaluateReplayPolicy = (body: {
  portal_job_id: string;
  mode: "RETRY_RUN" | "REPLAY_SAFE" | "REPLAY_SANDBOX" | "BACKFILL_RANGE";
  range_start?: string;
  range_end?: string;
  force_full_refresh?: boolean;
}) =>
  request(`/ops/replay/evaluate`, {
    method: "POST",
    body: JSON.stringify(body),
  });

export const requestReplayExecution = (body: {
  portal_job_id: string;
  mode: "RETRY_RUN" | "REPLAY_SAFE" | "REPLAY_SANDBOX" | "BACKFILL_RANGE";
  risk_score: number;
  justification?: string;
}) =>
  request(`/ops/replay/requests`, {
    method: "POST",
    body: JSON.stringify(body),
  });
