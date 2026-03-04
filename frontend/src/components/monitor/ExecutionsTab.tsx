import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { DataTable, DataTableColumn } from "@/components/ui/data-table";
import { FilterBar, FilterDefinition } from "@/components/ui/filter-bar";
import { StatusBadge } from "@/components/ui/status-badge";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { ExternalLink, FileText, ArrowDownToLine, Database, ArrowUpDown, Loader2 } from "lucide-react";
import { RunDetailPanel } from "@/components/RunDetailPanel";
import { RunningPhaseBadge } from "@/components/monitor/RunningPhaseBadge";
import * as api from "@/lib/api";

const formatDuration = (seconds: any) => {
  const s = Number(seconds);
  if (!s || isNaN(s)) return "—";
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
};

const formatTs = (ts: any) =>
  ts ? new Date(ts).toLocaleString("pt-BR") : "—";

const fmtNum = (n: any) => {
  const v = Number(n);
  return isNaN(v) ? "—" : v.toLocaleString("pt-BR");
};

const statusFilters: FilterDefinition[] = [
  {
    key: "status",
    label: "Status",
    options: [
      { value: "SUCCEEDED", label: "Succeeded" },
      { value: "FAILED", label: "Failed" },
      { value: "RUNNING", label: "Running" },
    ],
  },
  {
    key: "period",
    label: "Período",
    options: [
      { value: "24h", label: "Últimas 24h" },
      { value: "7d", label: "Últimos 7 dias" },
      { value: "30d", label: "Últimos 30 dias" },
    ],
  },
];

interface ExecutionsTabProps {
  pollingInterval: number;
  isActive: boolean;
}

export function ExecutionsTab({ pollingInterval, isActive }: ExecutionsTabProps) {
  const navigate = useNavigate();
  const [data, setData] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [drawerRunId, setDrawerRunId] = useState<string | null>(null);
  const [drawerBp, setDrawerBp] = useState<any>(null);
  const [sortKey, setSortKey] = useState("started_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");

  const fetchData = useCallback(async () => {
    try {
      const result = await api.getRecentBatchProcesses({
        page,
        page_size: pageSize,
        search: search || undefined,
        status: filters.status && filters.status !== "all" ? filters.status : undefined,
        period: filters.period && filters.period !== "all" ? filters.period : undefined,
        date_from: dateFrom || undefined,
        date_to: dateTo || undefined,
        sort_key: sortKey,
        sort_dir: sortDir,
      });
      setData(result.items || []);
      setTotal(result.total || 0);
    } catch (e) {
      console.error("ExecutionsTab fetch error:", e);
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, search, filters, dateFrom, dateTo, sortKey, sortDir]);

  useEffect(() => {
    if (!isActive) return;
    setLoading(true);
    fetchData();
  }, [fetchData, isActive]);

  // Polling
  useEffect(() => {
    if (!isActive) return;
    const interval = setInterval(fetchData, pollingInterval);
    return () => clearInterval(interval);
  }, [fetchData, pollingInterval, isActive]);

  const handleFilterChange = (key: string, value: string) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
    setPage(1);
  };

  const columns: DataTableColumn[] = [
    {
      key: "dataset_name",
      header: "Dataset",
      sortable: true,
      render: (row) => (
        <div className="min-w-0">
          <Tooltip>
            <TooltipTrigger asChild>
              <p className="font-medium text-sm truncate cursor-help">{row.dataset_name || row.dataset_id?.slice(0, 16)}</p>
            </TooltipTrigger>
            <TooltipContent>
              <p className="text-xs">{row.dataset_name || row.dataset_id}</p>
            </TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger asChild>
              <p className="text-xs text-muted-foreground font-mono cursor-help">{row.run_id?.slice(0, 8)}...</p>
            </TooltipTrigger>
            <TooltipContent>
              <p className="text-xs font-mono">{row.run_id}</p>
            </TooltipContent>
          </Tooltip>
        </div>
      ),
    },
    {
      key: "status",
      header: "Status",
      sortable: true,
      render: (row) => (
        <div className="flex flex-col gap-1">
          <StatusBadge status={row.status} />
          {["RUNNING", "CLAIMED"].includes(String(row.status).toUpperCase()) && row.run_id && (
            <RunningPhaseBadge runId={row.run_id} />
          )}
        </div>
      ),
    },
    {
      key: "started_at",
      header: "Início",
      sortable: true,
      render: (row) => (
        <span className="text-xs whitespace-nowrap">
          {formatTs(row.started_at)}
        </span>
      ),
    },
    {
      key: "duration_seconds",
      header: "Duração",
      sortable: true,
      className: "text-right",
      render: (row) => (
        <span className={`text-xs font-mono ${
          ["RUNNING", "CLAIMED"].includes(String(row.status).toUpperCase())
            ? "text-blue-600 font-medium animate-pulse"
            : "text-muted-foreground"
        }`}>
          {formatDuration(row.duration_seconds)}
        </span>
      ),
    },
    {
      key: "load_type",
      header: "Tipo Carga",
      sortable: true,
      render: (row) => {
        const type = row.load_type || "—";
        const color = type === "FULL" ? "bg-blue-100 text-blue-800" : type === "INCREMENTAL" ? "bg-green-100 text-green-800" : "bg-gray-100 text-gray-800";
        return <span className={`text-xs px-2 py-0.5 rounded ${color} font-medium`}>{type}</span>;
      },
    },
    {
      key: "rows_read",
      header: "Lidas",
      className: "text-right",
      sortable: true,
      render: (row) => {
        if (row.load_type === "INCREMENTAL" && row.incremental_rows_read != null) {
          return (
            <div className="text-right">
              <span className="text-xs font-mono text-green-700 font-medium">{fmtNum(row.incremental_rows_read)}</span>
              <p className="text-[10px] text-muted-foreground">da origem</p>
            </div>
          );
        }
        return <span className="text-xs font-mono">{fmtNum(row.bronze_row_count)}</span>;
      },
    },
    {
      key: "bronze_row_count",
      header: "Total Bronze",
      className: "text-right",
      sortable: true,
      render: (row) => (
        <span className="text-xs font-mono text-muted-foreground">{fmtNum(row.bronze_row_count)}</span>
      ),
    },
    {
      key: "upsert_detail",
      header: "Upsert",
      className: "text-right",
      render: (row) => {
        const ins = row.bronze_inserted_count;
        const upd = row.bronze_updated_count;
        if (ins != null || upd != null) {
          return (
            <div className="text-right space-y-0.5">
              {upd != null && Number(upd) > 0 && (
                <div className="flex items-center justify-end gap-1">
                  <ArrowUpDown className="h-3 w-3 text-amber-600" />
                  <span className="text-xs font-mono text-amber-700">{fmtNum(upd)}</span>
                </div>
              )}
              {ins != null && Number(ins) > 0 && (
                <div className="flex items-center justify-end gap-1">
                  <ArrowDownToLine className="h-3 w-3 text-blue-600" />
                  <span className="text-xs font-mono text-blue-700">{fmtNum(ins)}</span>
                </div>
              )}
              {(!upd || Number(upd) === 0) && (!ins || Number(ins) === 0) && (
                <span className="text-xs text-muted-foreground">0</span>
              )}
            </div>
          );
        }
        if (row.load_type === "FULL" || row.bronze_operation === "OVERWRITE") {
          return <span className="text-[10px] text-muted-foreground">OVERWRITE</span>;
        }
        return <span className="text-xs text-muted-foreground">—</span>;
      },
    },
    {
      key: "silver_row_count",
      header: "Silver",
      className: "text-right",
      sortable: true,
      render: (row) => <span className="text-xs font-mono">{fmtNum(row.silver_row_count)}</span>,
    },
    {
      key: "actions",
      header: "",
      className: "text-right",
      render: (row) => (
        <div className="flex items-center gap-1 justify-end" onClick={(e) => e.stopPropagation()}>
          {row.run_id && (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 text-xs"
              onClick={() => {
                setDrawerRunId(row.run_id);
                setDrawerBp(row);
              }}
            >
              <FileText className="h-3 w-3 mr-1" /> Logs
            </Button>
          )}
          {row.dataset_id && (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 text-xs"
              onClick={() => navigate(`/datasets/${row.dataset_id}`)}
            >
              <ExternalLink className="h-3 w-3" />
            </Button>
          )}
        </div>
      ),
    },
  ];

  return (
    <TooltipProvider>
      <FilterBar
        filters={statusFilters}
        filterValues={filters}
        onFilterChange={handleFilterChange}
        searchPlaceholder="Buscar dataset..."
        searchValue={search}
        onSearchChange={(v) => { setSearch(v); setPage(1); }}
        onRefresh={fetchData}
        refreshing={loading}
      >
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-muted-foreground whitespace-nowrap">Data:</span>
          <input
            type="date"
            value={dateFrom}
            onChange={(e) => { setDateFrom(e.target.value); setPage(1); }}
            className="h-9 px-2 text-xs border rounded-md bg-background"
          />
          <span className="text-xs text-muted-foreground">até</span>
          <input
            type="date"
            value={dateTo}
            onChange={(e) => { setDateTo(e.target.value); setPage(1); }}
            className="h-9 px-2 text-xs border rounded-md bg-background"
          />
        </div>
      </FilterBar>
      <DataTable
        columns={columns}
        data={data}
        total={total}
        page={page}
        pageSize={pageSize}
        onPageChange={setPage}
        onPageSizeChange={setPageSize}
        sortKey={sortKey}
        sortDir={sortDir}
        onSort={(key, dir) => { setSortKey(key); setSortDir(dir); setPage(1); }}
        loading={loading}
        emptyMessage="Nenhuma execução encontrada."
        rowKey={(row) => row.run_id || String(Math.random())}
        rowClassName={(row) =>
          row.status === "FAILED" ? "bg-red-50/30" :
          ["RUNNING", "CLAIMED"].includes(String(row.status).toUpperCase()) ? "bg-blue-50/30" : ""
        }
        expandableRow={(row) =>
          row.run_id ? (
            <div>
              <RunDetailPanel runId={row.run_id} batchProcess={row} compact />
              {row.dataset_id && (
                <div className="mt-3 flex justify-end">
                  <Button variant="outline" size="sm" onClick={() => navigate(`/datasets/${row.dataset_id}`)}>
                    <ExternalLink className="h-3 w-3 mr-1" /> Ver Dataset
                  </Button>
                </div>
              )}
            </div>
          ) : null
        }
      />

      {/* Drawer lateral para Logs */}
      <Sheet open={!!drawerRunId} onOpenChange={() => setDrawerRunId(null)}>
        <SheetContent className="w-full sm:max-w-2xl overflow-y-auto">
          <SheetHeader>
            <SheetTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5" />
              Detalhes da Execução
            </SheetTitle>
          </SheetHeader>
          <div className="mt-4">
            {drawerRunId && (
              <RunDetailPanel runId={drawerRunId} batchProcess={drawerBp} />
            )}
          </div>
        </SheetContent>
      </Sheet>
    </TooltipProvider>
  );
}
