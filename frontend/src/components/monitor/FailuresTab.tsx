import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { DataTable, DataTableColumn } from "@/components/ui/data-table";
import { FilterBar } from "@/components/ui/filter-bar";
import { StatusBadge } from "@/components/ui/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { ExternalLink, RotateCw, AlertCircle } from "lucide-react";
import * as api from "@/lib/api";

const formatTs = (ts: any) =>
  ts ? new Date(ts).toLocaleString("pt-BR") : "—";

interface FailuresTabProps {
  pollingInterval: number;
  isActive: boolean;
}

export function FailuresTab({ pollingInterval, isActive }: FailuresTabProps) {
  const navigate = useNavigate();
  const [data, setData] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [retrying, setRetrying] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState("");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const fetchData = useCallback(async () => {
    try {
      const result = await api.getFailedJobs({
        page,
        page_size: pageSize,
        search: search || undefined,
        sort_key: sortKey || undefined,
        sort_dir: sortDir,
      });
      setData(result.items || []);
      setTotal(result.total || 0);
    } catch (e) {
      console.error("FailuresTab fetch error:", e);
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, search, sortKey, sortDir]);

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

  const handleRetry = async (datasetId: string) => {
    setRetrying(datasetId);
    try {
      await api.enqueueDataset(datasetId);
      fetchData();
    } catch (e) {
      console.error("Retry error:", e);
    } finally {
      setRetrying(null);
    }
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
        </div>
      ),
    },
    {
      key: "last_error_class",
      header: "Classe do Erro",
      sortable: true,
      render: (row) =>
        row.last_error_class ? (
          <Badge variant="destructive" className="text-xs">{row.last_error_class}</Badge>
        ) : (
          <span className="text-xs text-muted-foreground">—</span>
        ),
    },
    {
      key: "last_error_message",
      header: "Erro",
      render: (row) => (
        <Tooltip>
          <TooltipTrigger asChild>
            <p className="text-xs text-red-700 truncate max-w-[300px] cursor-help">
              {(row.last_error_message || "").slice(0, 120)}
            </p>
          </TooltipTrigger>
          <TooltipContent className="max-w-md">
            <p className="text-xs whitespace-pre-wrap">{row.last_error_message}</p>
          </TooltipContent>
        </Tooltip>
      ),
    },
    {
      key: "requested_at",
      header: "Data",
      sortable: true,
      render: (row) => (
        <span className="text-xs whitespace-nowrap">
          {formatTs(row.requested_at)}
        </span>
      ),
    },
    {
      key: "attempt",
      header: "Tentativa",
      sortable: true,
      className: "text-center",
      render: (row) => (
        <span className="text-xs font-mono">{row.attempt}/{row.max_retries}</span>
      ),
    },
    {
      key: "actions",
      header: "",
      className: "text-right",
      render: (row) => (
        <div className="flex items-center gap-1 justify-end" onClick={(e) => e.stopPropagation()}>
          {row.dataset_id && (
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-xs"
              disabled={retrying === row.dataset_id}
              onClick={() => handleRetry(row.dataset_id)}
            >
              <RotateCw className={`h-3 w-3 mr-1 ${retrying === row.dataset_id ? "animate-spin" : ""}`} />
              Retry
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
        emptyIcon={<AlertCircle className="h-10 w-10 text-green-500" />}
        emptyMessage="Nenhuma falha encontrada. 🎉"
        rowKey={(row) => row.queue_id || String(Math.random())}
        rowClassName={() => "bg-red-50/20"}
        expandableRow={(row) => (
          <div className="space-y-3">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
              <div>
                <span className="text-muted-foreground">Queue ID:</span>{" "}
                <span className="font-mono">{row.queue_id?.slice(0, 12)}...</span>
              </div>
              <div>
                <span className="text-muted-foreground">Dataset ID:</span>{" "}
                <span className="font-mono">{row.dataset_id?.slice(0, 12)}...</span>
              </div>
              <div>
                <span className="text-muted-foreground">Solicitado:</span>{" "}
                <span>{formatTs(row.requested_at)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Próxima tentativa:</span>{" "}
                <span>{formatTs(row.next_retry_at)}</span>
              </div>
            </div>
            {row.last_error_message && (
              <div>
                <p className="text-xs font-medium text-red-700 mb-1">Mensagem de Erro Completa:</p>
                <pre className="text-xs text-red-800 bg-red-100 p-3 rounded overflow-auto max-h-48 whitespace-pre-wrap font-mono">
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
