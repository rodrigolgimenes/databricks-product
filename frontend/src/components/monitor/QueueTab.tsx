import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { DataTable, DataTableColumn } from "@/components/ui/data-table";
import { FilterBar, FilterDefinition } from "@/components/ui/filter-bar";
import { StatusBadge } from "@/components/ui/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { ExternalLink, Database } from "lucide-react";
import * as api from "@/lib/api";

const formatTs = (ts: any) =>
  ts ? new Date(ts).toLocaleString("pt-BR") : "—";

const queueFilters: FilterDefinition[] = [
  {
    key: "status",
    label: "Status",
    options: [
      { value: "PENDING", label: "Pending" },
      { value: "RUNNING", label: "Running" },
      { value: "CLAIMED", label: "Claimed" },
      { value: "SUCCEEDED", label: "Succeeded" },
      { value: "FAILED", label: "Failed" },
    ],
  },
];

interface QueueTabProps {
  pollingInterval: number;
  isActive: boolean;
}

export function QueueTab({ pollingInterval, isActive }: QueueTabProps) {
  const navigate = useNavigate();
  const [data, setData] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [sortKey, setSortKey] = useState("");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const fetchData = useCallback(async () => {
    try {
      const result = await api.getQueue({
        page,
        page_size: pageSize,
        search: search || undefined,
        status: filters.status && filters.status !== "all" ? filters.status : undefined,
        sort_key: sortKey || undefined,
        sort_dir: sortDir,
      });
      setData(result.items || []);
      setTotal(result.total || 0);
    } catch (e) {
      console.error("QueueTab fetch error:", e);
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, search, filters, sortKey, sortDir]);

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

  const columns: DataTableColumn[] = [
    {
      key: "dataset_name",
      header: "Dataset",
      sortable: true,
      render: (row) => (
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <p className="font-medium text-sm truncate cursor-help">{row.dataset_name || row.dataset_id?.slice(0, 16)}</p>
              </TooltipTrigger>
              <TooltipContent>
                <p className="text-xs">{row.dataset_name || row.dataset_id}</p>
              </TooltipContent>
            </Tooltip>
            {row.source_type && (
              <Badge variant="outline" className="text-[10px] flex-shrink-0">{row.source_type}</Badge>
            )}
          </div>
          <p className="text-xs text-muted-foreground">
            {formatTs(row.requested_at)} · {row.trigger_type} · por {row.requested_by || "—"}
          </p>
        </div>
      ),
    },
    {
      key: "status",
      header: "Status",
      sortable: true,
      render: (row) => <StatusBadge status={row.status} />,
    },
    {
      key: "priority",
      header: "Prioridade",
      sortable: true,
      className: "text-center",
      render: (row) => <span className="text-xs font-mono">{row.priority ?? "—"}</span>,
    },
    {
      key: "attempt",
      header: "Tentativa",
      sortable: true,
      className: "text-center",
      render: (row) => <span className="text-xs font-mono">{row.attempt}/{row.max_retries}</span>,
    },
    {
      key: "actions",
      header: "",
      className: "text-right",
      render: (row) => (
        <div className="flex items-center gap-1 justify-end" onClick={(e) => e.stopPropagation()}>
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
        filters={queueFilters}
        filterValues={filters}
        onFilterChange={handleFilterChange}
        searchPlaceholder="Buscar dataset..."
        searchValue={search}
        onSearchChange={(v) => { setSearch(v); setPage(1); }}
        onRefresh={fetchData}
        refreshing={loading}
      />
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
        emptyIcon={<Database className="h-10 w-10" />}
        emptyMessage="Fila vazia."
        rowKey={(row) => row.queue_id || String(Math.random())}
        rowClassName={(row) =>
          row.status === "FAILED" ? "bg-red-50/30" :
          ["RUNNING", "CLAIMED"].includes(String(row.status).toUpperCase()) ? "bg-blue-50/30" : ""
        }
        expandableRow={(row) => (
          <div className="space-y-3 text-xs">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              <div>
                <span className="text-muted-foreground">Queue ID:</span>{" "}
                <span className="font-mono">{row.queue_id}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Dataset ID:</span>{" "}
                <span className="font-mono">{row.dataset_id}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Run ID:</span>{" "}
                <span className="font-mono">{row.run_id || "—"}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Claim Owner:</span>{" "}
                <span className="font-mono">{row.claim_owner || "—"}</span>
              </div>
              {row.connection_id && (
                <div>
                  <span className="text-muted-foreground">Connection:</span>{" "}
                  <span className="font-mono">{row.connection_id}</span>
                </div>
              )}
              {row.bronze_table && (
                <div>
                  <span className="text-muted-foreground">Bronze:</span>{" "}
                  <span className="font-mono">{row.bronze_table}</span>
                </div>
              )}
              {row.silver_table && (
                <div>
                  <span className="text-muted-foreground">Silver:</span>{" "}
                  <span className="font-mono">{row.silver_table}</span>
                </div>
              )}
              {row.execution_state && (
                <div>
                  <span className="text-muted-foreground">Estado Dataset:</span>{" "}
                  <Badge variant="outline" className="text-xs">{row.execution_state}</Badge>
                </div>
              )}
              <div>
                <span className="text-muted-foreground">Claimed At:</span>{" "}
                <span>{formatTs(row.claimed_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Início:</span>{" "}
                <span>{formatTs(row.started_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Fim:</span>{" "}
                <span>{formatTs(row.finished_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Próxima tentativa:</span>{" "}
                <span>{formatTs(row.next_retry_at)}</span>
              </div>
            </div>
            {row.last_error_message && (
              <div>
                <p className="font-medium text-red-700 mb-1">Erro ({row.last_error_class}):</p>
                <pre className="text-red-800 bg-red-100 p-3 rounded overflow-auto max-h-48 whitespace-pre-wrap font-mono">
                  {row.last_error_message}
                </pre>
              </div>
            )}
          </div>
        )}
      />
    </TooltipProvider>
  );
}
