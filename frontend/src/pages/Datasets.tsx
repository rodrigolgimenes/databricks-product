import { useState, useEffect, useCallback, useRef } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Database,
  RefreshCw,
  Search,
  Plus,
  ArrowRight,
  Play,
  Loader2,
  LayoutGrid,
  List,
  ArrowUp,
  ArrowDown,
  ArrowUpDown,
  MoreHorizontal,
  Eye,
  Copy,
  ChevronLeft,
  ChevronRight,
  Download,
  X,
  Filter,
  Edit,
} from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useNavigate } from "react-router-dom";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import * as api from "@/lib/api";

// ── Constants ──

const stateColor: Record<string, string> = {
  ACTIVE: "bg-green-500 text-white",
  PAUSED: "bg-yellow-500 text-white",
  DRAFT: "bg-blue-500 text-white",
  BLOCKED_SCHEMA_CHANGE: "bg-red-500 text-white",
  DEPRECATED: "bg-gray-500 text-white",
};

const sourceTypeColor: Record<string, string> = {
  ORACLE: "bg-orange-100 text-orange-800",
  SUPABASE: "bg-green-100 text-green-800",
};

const STATUS_OPTIONS = ["ACTIVE", "PAUSED", "DRAFT", "BLOCKED_SCHEMA_CHANGE", "DEPRECATED"];
const SOURCE_OPTIONS = ["ORACLE", "SUPABASE"];
const PAGE_SIZES = [20, 50, 100];

type SortDir = "asc" | "desc" | null;
type ViewMode = "list" | "cards";

// ── Helpers ──

function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(t);
  }, [value, delay]);
  return debounced;
}

function persist(key: string, value: string) {
  try { localStorage.setItem(`datasets_${key}`, value); } catch { /* noop */ }
}
function restore(key: string, fallback: string): string {
  try { return localStorage.getItem(`datasets_${key}`) || fallback; } catch { return fallback; }
}

// ── Component ──

const Datasets = () => {
  const navigate = useNavigate();

  // View mode
  const [viewMode, setViewMode] = useState<ViewMode>(() => restore("viewMode", "list") as ViewMode);

  // Data
  const [datasets, setDatasets] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [loading, setLoading] = useState(true);

  // Search & filters
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, 350);
  const [filterStatus, setFilterStatus] = useState("");
  const [filterSource, setFilterSource] = useState("");
  const [filterProject, setFilterProject] = useState("");
  const [filterArea, setFilterArea] = useState("");

  // Sort
  const [sortBy, setSortBy] = useState<string>(() => restore("sortBy", ""));
  const [sortDir, setSortDir] = useState<SortDir>(() => {
    const v = restore("sortDir", "");
    return v === "asc" || v === "desc" ? v : null;
  });

  // Pagination
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(() => parseInt(restore("pageSize", "50"), 10) || 50);

  // Selection
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  // Bulk execute modal
  const [showBulkModal, setShowBulkModal] = useState(false);
  const [bulkStrategy, setBulkStrategy] = useState<"sequential" | "parallel">("parallel");
  const [bulkExecuting, setBulkExecuting] = useState(false);

  // Bulk delete modal
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [deleting, setDeleting] = useState(false);

  // Bulk rename modal
  const [showRenameModal, setShowRenameModal] = useState(false);
  const [renameOperation, setRenameOperation] = useState<"REPLACE_SCHEMA_PREFIX" | "REPLACE_CATALOG" | "REPLACE_FULL">("REPLACE_SCHEMA_PREFIX");
  const [renameBronzeFrom, setRenameBronzeFrom] = useState("");
  const [renameBronzeTo, setRenameBronzeTo] = useState("");
  const [renameSilverFrom, setRenameSilverFrom] = useState("");
  const [renameSilverTo, setRenameSilverTo] = useState("");
  const [renameCreateSchemas, setRenameCreateSchemas] = useState(true);
  const [renamePreview, setRenamePreview] = useState<any>(null);
  const [renaming, setRenaming] = useState(false);
  const [loadingPreview, setLoadingPreview] = useState(false);

  // Projects & areas for filters
  const [projects, setProjects] = useState<any[]>([]);
  const [areas, setAreas] = useState<any[]>([]);

  // Ref for tracking if component is mounted
  const mountedRef = useRef(true);
  useEffect(() => { return () => { mountedRef.current = false; }; }, []);

  // Load projects & areas once
  useEffect(() => {
    api.getProjects().then(d => setProjects(d.items || [])).catch(console.error);
    api.getAreas().then(d => setAreas(d.items || [])).catch(console.error);
  }, []);

  // Fetch datasets
  const fetchDatasets = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, any> = { page, page_size: pageSize };
      if (debouncedSearch) params.search = debouncedSearch;
      if (sortBy && sortDir) { params.sort_by = sortBy; params.sort_dir = sortDir; }
      if (filterStatus) params.status = filterStatus;
      if (filterSource) params.source_type = filterSource;
      if (filterProject) params.project_id = filterProject;
      if (filterArea) params.area_id = filterArea;

      const data = await api.getDatasets(params);
      if (!mountedRef.current) return;
      setDatasets(data.items || []);
      setTotal(data.total || 0);
      setTotalPages(data.total_pages || 1);
    } catch (e) {
      console.error(e);
    } finally {
      if (mountedRef.current) setLoading(false);
    }
  }, [page, pageSize, debouncedSearch, sortBy, sortDir, filterStatus, filterSource, filterProject, filterArea]);

  useEffect(() => { fetchDatasets(); }, [fetchDatasets]);

  // Reset page to 1 when filters/search change
  useEffect(() => { setPage(1); }, [debouncedSearch, filterStatus, filterSource, filterProject, filterArea]);

  // Persist preferences
  useEffect(() => { persist("viewMode", viewMode); }, [viewMode]);
  useEffect(() => { persist("pageSize", String(pageSize)); }, [pageSize]);
  useEffect(() => { persist("sortBy", sortBy); persist("sortDir", sortDir || ""); }, [sortBy, sortDir]);

  // ── Selection ──

  const toggleSelection = (id: string) => {
    const n = new Set(selectedIds);
    n.has(id) ? n.delete(id) : n.add(id);
    setSelectedIds(n);
  };

  const allOnPageSelected = datasets.length > 0 && datasets.every(d => selectedIds.has(d.dataset_id));
  const someOnPageSelected = datasets.some(d => selectedIds.has(d.dataset_id));

  const toggleSelectAll = () => {
    if (allOnPageSelected) {
      const n = new Set(selectedIds);
      datasets.forEach(d => n.delete(d.dataset_id));
      setSelectedIds(n);
    } else {
      const n = new Set(selectedIds);
      datasets.forEach(d => n.add(d.dataset_id));
      setSelectedIds(n);
    }
  };

  // ── Sort ──

  const handleSort = (col: string) => {
    if (sortBy === col) {
      if (sortDir === "asc") { setSortDir("desc"); }
      else if (sortDir === "desc") { setSortBy(""); setSortDir(null); }
      else { setSortDir("asc"); }
    } else {
      setSortBy(col);
      setSortDir("asc");
    }
    setPage(1);
  };

  const SortIcon = ({ col }: { col: string }) => {
    if (sortBy !== col) return <ArrowUpDown className="h-3 w-3 ml-1 opacity-40" />;
    if (sortDir === "asc") return <ArrowUp className="h-3 w-3 ml-1 text-primary" />;
    return <ArrowDown className="h-3 w-3 ml-1 text-primary" />;
  };

  // ── Bulk execute ──

  const handleBulkExecute = async () => {
    setBulkExecuting(true);
    try {
      const res = await api.bulkEnqueueDatasets(Array.from(selectedIds), bulkStrategy);
      const s = res.summary || {};
      alert(`${s.enqueued || 0} dataset(s) enfileirado(s), ${s.skipped || 0} ignorado(s).`);
      setSelectedIds(new Set());
      setShowBulkModal(false);
      fetchDatasets();
    } catch (e: any) {
      alert(`Erro ao enfileirar: ${e.message}`);
    } finally {
      setBulkExecuting(false);
    }
  };

  // ── Bulk rename ──

  const loadRenamePreview = async () => {
    setLoadingPreview(true);
    try {
      const result = await api.previewBulkRename({
        dataset_ids: Array.from(selectedIds),
        operation: renameOperation,
        bronze_from: renameBronzeFrom || undefined,
        bronze_to: renameBronzeTo || undefined,
        silver_from: renameSilverFrom || undefined,
        silver_to: renameSilverTo || undefined,
        create_schemas: renameCreateSchemas,
      });
      setRenamePreview(result);
    } catch (e: any) {
      alert(`Erro ao gerar preview: ${e.message}`);
      setRenamePreview(null);
    } finally {
      setLoadingPreview(false);
    }
  };

  const handleBulkRename = async () => {
    setRenaming(true);
    try {
      const result = await api.executeBulkRename({
        dataset_ids: Array.from(selectedIds),
        operation: renameOperation,
        bronze_from: renameBronzeFrom || undefined,
        bronze_to: renameBronzeTo || undefined,
        silver_from: renameSilverFrom || undefined,
        silver_to: renameSilverTo || undefined,
        create_schemas: renameCreateSchemas,
      });
      
      const renamed = result.renamed || 0;
      const conflicts = result.results?.filter((r: any) => r.status === 'CONFLICT').length || 0;
      const errors = result.results?.filter((r: any) => r.status === 'ERROR').length || 0;
      
      alert(
        `Renomeação concluída!\n` +
        `✓ ${renamed} dataset(s) renomeado(s)\n` +
        `⚠️ ${conflicts} conflito(s)\n` +
        `✗ ${errors} erro(s)${errors > 0 ? '\n\nVerifique o console para detalhes.' : ''}`
      );

      if (errors > 0 || conflicts > 0) {
        console.log('[RENAME] Resultados:', result.results?.filter((r: any) => r.status !== 'RENAMED'));
      }

      setSelectedIds(new Set());
      setShowRenameModal(false);
      setRenamePreview(null);
      fetchDatasets();
    } catch (e: any) {
      alert(`Erro ao renomear: ${e.message}`);
    } finally {
      setRenaming(false);
    }
  };

  // Resetar preview quando operacao ou parametros mudam
  useEffect(() => {
    setRenamePreview(null);
  }, [renameOperation, renameBronzeFrom, renameBronzeTo, renameSilverFrom, renameSilverTo]);

  // ── Bulk delete ──

  const handleBulkDelete = async () => {
    setDeleting(true);
    try {
      const selectedDatasets = datasets.filter(d => selectedIds.has(d.dataset_id));
      const results = [];
      let deleted = 0;
      let failed = 0;

      for (const ds of selectedDatasets) {
        try {
          await api.deleteDataset(ds.dataset_id, ds.dataset_name, false); // NUNCA dropar tabelas
          deleted++;
          results.push({ dataset_id: ds.dataset_id, status: 'DELETED' });
        } catch (err: any) {
          failed++;
          results.push({ dataset_id: ds.dataset_id, status: 'ERROR', error: err.message });
        }
      }

      alert(
        `Exclusão concluída!\n` +
        `✓ ${deleted} dataset(s) excluído(s)\n` +
        `✗ ${failed} falha(s)${failed > 0 ? '\n\nVerifique o console para detalhes.' : ''}`
      );

      if (failed > 0) {
        console.log('[DELETE] Resultados:', results.filter(r => r.status === 'ERROR'));
      }

      setSelectedIds(new Set());
      setShowDeleteModal(false);
      setDeleteConfirmText("");
      fetchDatasets();
    } catch (e: any) {
      alert(`Erro ao excluir: ${e.message}`);
    } finally {
      setDeleting(false);
    }
  };

  // ── CSV export ──

  const handleExportCSV = () => {
    const selectedDatasets = datasets.filter(d => selectedIds.has(d.dataset_id));
    if (!selectedDatasets.length) return;
    const headers = ["dataset_id", "dataset_name", "execution_state", "source_type", "project_id", "area_id", "bronze_table", "silver_table"];
    const rows = selectedDatasets.map(d => headers.map(h => `"${String(d[h] || "").replace(/"/g, '""')}"`).join(","));
    const csv = [headers.join(","), ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `datasets_export_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // ── Copy ──
  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text).catch(console.error);
  };

  // ── Filter active count ──
  const activeFilters = [filterStatus, filterSource, filterProject, filterArea].filter(Boolean).length;

  const clearFilters = () => {
    setFilterStatus("");
    setFilterSource("");
    setFilterProject("");
    setFilterArea("");
  };

  // ── Resolve names ──
  const projectName = (id: string) => projects.find(p => p.project_id === id)?.project_name || id?.slice(0, 8);
  const areaName = (id: string) => areas.find(a => a.area_id === id)?.area_name || id?.slice(0, 8);

  // ── Strategy badge ──
  const renderStrategyBadge = (ds: any) => {
    const discoveryStatus = ds.discovery_status;
    const strategy = ds.incremental_strategy || "SNAPSHOT";
    const enableIncremental = ds.enable_incremental;

    // Pending confirmation - show yellow badge
    if (discoveryStatus === "PENDING_CONFIRMATION") {
      return (
        <Badge
          variant="outline"
          className="text-xs bg-yellow-50 text-yellow-700 border-yellow-300"
        >
          🟡 {ds.discovery_suggestion || "PENDENTE"}
        </Badge>
      );
    }

    // Incremental active - show green badge
    if (enableIncremental && strategy !== "SNAPSHOT") {
      return (
        <Badge variant="outline" className="text-xs bg-green-50 text-green-700 border-green-300">
          🟢 {strategy}
        </Badge>
      );
    }

    // REQUIRES_CDC - show red badge
    if (strategy === "REQUIRES_CDC") {
      return (
        <Badge variant="outline" className="text-xs bg-red-50 text-red-700 border-red-300">
          🔴 CDC REQUERIDO
        </Badge>
      );
    }

    // Default SNAPSHOT - show blue badge
    return (
      <Badge variant="outline" className="text-xs bg-blue-50 text-blue-700 border-blue-300">
        🔵 FULL REFRESH
      </Badge>
    );
  };

  // ── Pagination info ──
  const startItem = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const endItem = Math.min(page * pageSize, total);

  return (
    <TooltipProvider>
    <div className="space-y-4">
      {/* ── Header ── */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold">Datasets</h1>
          <p className="text-muted-foreground text-sm mt-0.5">
            {total} dataset{total !== 1 && "s"}
          </p>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {/* Bulk action bar */}
          {selectedIds.size > 0 && (
            <div className="flex items-center gap-2 mr-2 px-3 py-1.5 bg-primary/10 rounded-lg border border-primary/20">
              <span className="text-sm font-medium">{selectedIds.size} selecionado{selectedIds.size > 1 && "s"}</span>
              <Button size="sm" variant="default" className="h-7 bg-green-600 hover:bg-green-700" onClick={() => setShowBulkModal(true)}>
                <Play className="h-3 w-3 mr-1" /> Executar
              </Button>
              <Button size="sm" variant="outline" className="h-7" onClick={handleExportCSV}>
                <Download className="h-3 w-3 mr-1" /> CSV
              </Button>
              <Button size="sm" variant="outline" className="h-7" onClick={() => setShowRenameModal(true)}>
                <Edit className="h-3 w-3 mr-1" /> Renomear
              </Button>
              <Button 
                size="sm" 
                variant="destructive" 
                className="h-7" 
                onClick={() => setShowDeleteModal(true)}
              >
                <X className="h-3 w-3 mr-1" /> Excluir
              </Button>
              <Button size="sm" variant="ghost" className="h-7" onClick={() => setSelectedIds(new Set())}>
                <X className="h-3 w-3" />
              </Button>
            </div>
          )}

          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Buscar datasets..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-10 w-64 h-9"
            />
          </div>

          {/* View toggle */}
          <div className="flex items-center gap-0.5 p-0.5 bg-muted rounded-md">
            <button
              className={`p-1.5 rounded ${viewMode === "list" ? "bg-background shadow" : "text-muted-foreground hover:text-foreground"}`}
              onClick={() => setViewMode("list")}
              title="Lista"
            >
              <List className="h-4 w-4" />
            </button>
            <button
              className={`p-1.5 rounded ${viewMode === "cards" ? "bg-background shadow" : "text-muted-foreground hover:text-foreground"}`}
              onClick={() => setViewMode("cards")}
              title="Cards"
            >
              <LayoutGrid className="h-4 w-4" />
            </button>
          </div>

          <Button variant="outline" size="sm" className="h-9" onClick={fetchDatasets}>
            <RefreshCw className="h-4 w-4" />
          </Button>
          <Button size="sm" className="h-9" onClick={() => navigate("/create")}>
            <Plus className="h-4 w-4 mr-1" /> Novo
          </Button>
        </div>
      </div>

      {/* ── Filters row ── */}
      <div className="flex items-center gap-2 flex-wrap">
        <Filter className="h-4 w-4 text-muted-foreground" />
        <Select value={filterStatus} onValueChange={v => setFilterStatus(v === "__all__" ? "" : v)}>
          <SelectTrigger className="w-[150px] h-8 text-xs">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="__all__">Todos Status</SelectItem>
            {STATUS_OPTIONS.map(s => (
              <SelectItem key={s} value={s}>{s}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={filterSource} onValueChange={v => setFilterSource(v === "__all__" ? "" : v)}>
          <SelectTrigger className="w-[130px] h-8 text-xs">
            <SelectValue placeholder="Source" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="__all__">Todos Sources</SelectItem>
            {SOURCE_OPTIONS.map(s => (
              <SelectItem key={s} value={s}>{s}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={filterProject} onValueChange={v => setFilterProject(v === "__all__" ? "" : v)}>
          <SelectTrigger className="w-[160px] h-8 text-xs">
            <SelectValue placeholder="Projeto" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="__all__">Todos Projetos</SelectItem>
            {projects.map(p => (
              <SelectItem key={p.project_id} value={p.project_id}>{p.project_name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={filterArea} onValueChange={v => setFilterArea(v === "__all__" ? "" : v)}>
          <SelectTrigger className="w-[160px] h-8 text-xs">
            <SelectValue placeholder="Área" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="__all__">Todas Áreas</SelectItem>
            {areas.map(a => (
              <SelectItem key={a.area_id} value={a.area_id}>{a.area_name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        {activeFilters > 0 && (
          <Button variant="ghost" size="sm" className="h-8 text-xs" onClick={clearFilters}>
            <X className="h-3 w-3 mr-1" /> Limpar filtros ({activeFilters})
          </Button>
        )}
      </div>

      {/* ── Content ── */}
      {loading ? (
        <div className="flex items-center justify-center min-h-[40vh]">
          <LoadingSpinner text="Carregando datasets..." />
        </div>
      ) : datasets.length === 0 ? (
        <div className="text-center py-16 text-muted-foreground">
          <Database className="h-12 w-12 mx-auto mb-4 opacity-30" />
          <p className="text-lg font-medium">Nenhum dataset encontrado</p>
          <p className="text-sm mt-1">
            {debouncedSearch || activeFilters > 0
              ? "Tente alterar seus filtros ou busca"
              : "Crie um novo dataset para começar"}
          </p>
        </div>
      ) : viewMode === "list" ? (
        /* ── TABLE VIEW ── */
        <div className="border rounded-lg">
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/30">
                <TableHead className="w-10">
                  <Checkbox
                    checked={allOnPageSelected}
                    onCheckedChange={toggleSelectAll}
                    aria-label="Selecionar todos"
                    {...(someOnPageSelected && !allOnPageSelected ? { "data-state": "indeterminate" } : {})}
                  />
                </TableHead>
                <TableHead>
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("dataset_name")}>
                    Dataset <SortIcon col="dataset_name" />
                  </button>
                </TableHead>
                <TableHead className="w-32">
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("execution_state")}>
                    Status <SortIcon col="execution_state" />
                  </button>
                </TableHead>
                <TableHead className="w-24">
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("source_type")}>
                    Source <SortIcon col="source_type" />
                  </button>
                </TableHead>
                <TableHead>
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("project_id")}>
                    Projeto <SortIcon col="project_id" />
                  </button>
                </TableHead>
                <TableHead>
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("area_id")}>
                    Área <SortIcon col="area_id" />
                  </button>
                </TableHead>
                <TableHead className="hidden xl:table-cell">
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("bronze_table")}>
                    Bronze <SortIcon col="bronze_table" />
                  </button>
                </TableHead>
                <TableHead className="hidden xl:table-cell">
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("silver_table")}>
                    Silver <SortIcon col="silver_table" />
                  </button>
                </TableHead>
                <TableHead className="w-16 hidden lg:table-cell">
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("current_schema_ver")}>
                    Schema <SortIcon col="current_schema_ver" />
                  </button>
                </TableHead>
                <TableHead className="w-32 hidden xl:table-cell">
                  Estratégia
                </TableHead>
                <TableHead className="w-28">
                  <button className="flex items-center font-medium hover:text-foreground" onClick={() => handleSort("created_at")}>
                    Criado em <SortIcon col="created_at" />
                  </button>
                </TableHead>
                <TableHead className="w-12" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {datasets.map((ds) => (
                <TableRow
                  key={ds.dataset_id}
                  className="cursor-pointer"
                  data-state={selectedIds.has(ds.dataset_id) ? "selected" : undefined}
                >
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <Checkbox
                      checked={selectedIds.has(ds.dataset_id)}
                      onCheckedChange={() => toggleSelection(ds.dataset_id)}
                    />
                  </TableCell>
                  <TableCell onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    <div className="flex items-center gap-2">
                      <div>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <p className="font-medium text-sm truncate max-w-[260px] cursor-help">{ds.dataset_name}</p>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>{ds.dataset_name}</p>
                          </TooltipContent>
                        </Tooltip>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <p className="text-xs text-muted-foreground cursor-help">{ds.dataset_id?.slice(0, 8)}...</p>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="font-mono">{ds.dataset_id}</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <button
                        className="opacity-0 group-hover:opacity-100 hover:text-primary transition-opacity"
                        onClick={(e) => { e.stopPropagation(); copyToClipboard(ds.dataset_name); }}
                        title="Copiar nome"
                      >
                        <Copy className="h-3 w-3" />
                      </button>
                    </div>
                  </TableCell>
                  <TableCell onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    <Badge className={`text-xs ${stateColor[ds.execution_state] || "bg-gray-100"}`}>
                      {ds.execution_state}
                    </Badge>
                  </TableCell>
                  <TableCell onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    <Badge variant="outline" className={`text-xs ${sourceTypeColor[ds.source_type] || ""}`}>
                      {ds.source_type}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-xs" onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    {projectName(ds.project_id)}
                  </TableCell>
                  <TableCell className="text-xs" onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    {areaName(ds.area_id)}
                  </TableCell>
                  <TableCell className="hidden xl:table-cell text-xs font-mono truncate max-w-[200px]" onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="cursor-help">{ds.bronze_table || "—"}</span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="font-mono">{ds.bronze_table || "N/A"}</p>
                      </TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell className="hidden xl:table-cell text-xs font-mono truncate max-w-[200px]" onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="cursor-help">{ds.silver_table || "—"}</span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="font-mono">{ds.silver_table || "N/A"}</p>
                      </TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell className="hidden lg:table-cell text-xs text-center" onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    {ds.current_schema_ver ?? "—"}
                  </TableCell>
                  <TableCell className="hidden xl:table-cell" onClick={(e) => e.stopPropagation()}>
                    {renderStrategyBadge(ds)}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground" onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="cursor-help">
                          {ds.created_at ? new Date(ds.created_at).toLocaleDateString("pt-BR") : ""}
                        </span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>{ds.created_at ? new Date(ds.created_at).toLocaleString("pt-BR") : "N/A"}</p>
                      </TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="sm" className="h-7 w-7 p-0">
                          <MoreHorizontal className="h-4 w-4" />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem onClick={() => navigate(`/datasets/${ds.dataset_id}`)}>
                          <Eye className="h-4 w-4 mr-2" /> Ver detalhes
                        </DropdownMenuItem>
                        <DropdownMenuItem onClick={() => copyToClipboard(ds.dataset_id)}>
                          <Copy className="h-4 w-4 mr-2" /> Copiar ID
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          onClick={async () => {
                            try {
                              await api.enqueueDataset(ds.dataset_id);
                              fetchDatasets();
                            } catch (err: any) {
                              alert(err.message);
                            }
                          }}
                          disabled={["PAUSED", "DEPRECATED", "BLOCKED_SCHEMA_CHANGE"].includes(ds.execution_state)}
                        >
                          <Play className="h-4 w-4 mr-2" /> Executar
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      ) : (
        /* ── CARDS VIEW ── */
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {datasets.map((ds) => (
            <Card
              key={ds.dataset_id}
              className="group cursor-pointer hover:shadow-lg hover:border-primary/20 transition-all duration-200"
              onClick={() => navigate(`/datasets/${ds.dataset_id}`)}
            >
              <CardContent className="p-5">
                <div className="flex items-start justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <Checkbox
                      checked={selectedIds.has(ds.dataset_id)}
                      onCheckedChange={() => toggleSelection(ds.dataset_id)}
                      onClick={(e) => e.stopPropagation()}
                    />
                    <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-primary to-blue-500 flex items-center justify-center">
                      <Database className="h-4 w-4 text-primary-foreground" />
                    </div>
                    <div>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <p className="font-semibold text-sm truncate max-w-[200px] cursor-help">{ds.dataset_name}</p>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>{ds.dataset_name}</p>
                        </TooltipContent>
                      </Tooltip>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <p className="text-xs text-muted-foreground cursor-help">{ds.dataset_id?.slice(0, 8)}...</p>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p className="font-mono">{ds.dataset_id}</p>
                        </TooltipContent>
                      </Tooltip>
                    </div>
                  </div>
                  <Badge className={stateColor[ds.execution_state] || "bg-gray-100"}>{ds.execution_state}</Badge>
                </div>
                <div className="space-y-2 text-xs">
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Source</span>
                    <Badge variant="outline" className={sourceTypeColor[ds.source_type] || ""}>{ds.source_type}</Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Projeto</span>
                    <span className="truncate max-w-[160px]">{projectName(ds.project_id)}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Área</span>
                    <span className="truncate max-w-[160px]">{areaName(ds.area_id)}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Bronze</span>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="font-mono truncate max-w-[200px] cursor-help">{ds.bronze_table || "—"}</span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="font-mono">{ds.bronze_table || "N/A"}</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Silver</span>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="font-mono truncate max-w-[200px] cursor-help">{ds.silver_table || "—"}</span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="font-mono">{ds.silver_table || "N/A"}</p>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Schema v.</span>
                    <span>{ds.current_schema_ver ?? "—"}</span>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t flex items-center justify-between">
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className="text-xs text-muted-foreground cursor-help">
                        {ds.created_at ? new Date(ds.created_at).toLocaleDateString("pt-BR") : ""}
                      </span>
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>{ds.created_at ? new Date(ds.created_at).toLocaleString("pt-BR") : "N/A"}</p>
                    </TooltipContent>
                  </Tooltip>
                  <ArrowRight className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors" />
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* ── Pagination ── */}
      {!loading && total > 0 && (
        <div className="flex items-center justify-between pt-2">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <span>Mostrando {startItem}–{endItem} de {total}</span>
            <Select value={String(pageSize)} onValueChange={v => { setPageSize(Number(v)); setPage(1); }}>
              <SelectTrigger className="w-[80px] h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {PAGE_SIZES.map(s => (
                  <SelectItem key={s} value={String(s)}>{s}/pág</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-center gap-1">
            <Button variant="outline" size="sm" className="h-8" disabled={page <= 1} onClick={() => setPage(page - 1)}>
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="text-sm px-3">
              Página {page} de {totalPages}
            </span>
            <Button variant="outline" size="sm" className="h-8" disabled={page >= totalPages} onClick={() => setPage(page + 1)}>
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}

      {/* ── Bulk Execute Dialog ── */}
      <Dialog open={showBulkModal} onOpenChange={setShowBulkModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Executar {selectedIds.size} dataset(s)</DialogTitle>
            <DialogDescription>
              Os datasets selecionados serão enfileirados para execução.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <Label>Estratégia de execução</Label>
            <RadioGroup value={bulkStrategy} onValueChange={(v) => setBulkStrategy(v as "sequential" | "parallel")}>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="parallel" id="parallel" />
                <Label htmlFor="parallel" className="font-normal">
                  Paralelo — todos os datasets são enfileirados com mesma prioridade
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="sequential" id="sequential" />
                <Label htmlFor="sequential" className="font-normal">
                  Sequencial — prioridades decrescentes para execução em ordem
                </Label>
              </div>
            </RadioGroup>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowBulkModal(false)}>Cancelar</Button>
            <Button onClick={handleBulkExecute} disabled={bulkExecuting} className="bg-green-600 hover:bg-green-700">
              {bulkExecuting ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}
              Executar {selectedIds.size} dataset(s)
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>


      {/* ── Bulk Delete Dialog ── */}
      <Dialog open={showDeleteModal} onOpenChange={setShowDeleteModal}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-destructive">Excluir {selectedIds.size} dataset(s)?</DialogTitle>
            <DialogDescription className="space-y-2">
              <p>Esta ação não pode ser desfeita. Os datasets serão removidos da ferramenta de ingestão e orquestração.</p>
              <div className="bg-yellow-50 border border-yellow-200 rounded-md p-3 text-sm text-yellow-800">
                <strong>⚠️ Importante:</strong>
                <ul className="list-disc list-inside mt-1 space-y-0.5">
                  <li>As tabelas Bronze e Silver <strong>NÃO</strong> serão excluídas automaticamente</li>
                  <li>Apenas os registros de configuração serão removidos</li>
                  <li>Histórico de execuções será mantido</li>
                </ul>
              </div>
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="space-y-2">
              <Label htmlFor="confirm-text">Para confirmar, digite <strong>EXCLUIR</strong> abaixo:</Label>
              <Input
                id="confirm-text"
                placeholder="Digite EXCLUIR para confirmar"
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
                className={deleteConfirmText === "EXCLUIR" ? "border-green-500" : ""}
              />
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setShowDeleteModal(false);
                setDeleteConfirmText("");
              }}
              disabled={deleting}
            >
              Cancelar
            </Button>
            <Button
              variant="destructive"
              onClick={handleBulkDelete}
              disabled={deleteConfirmText !== "EXCLUIR" || deleting}
            >
              {deleting ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Excluindo...
                </>
              ) : (
                <>
                  <X className="h-4 w-4 mr-2" />
                  Excluir {selectedIds.size} dataset(s)
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ── Bulk Rename Dialog ── */}
      <Dialog open={showRenameModal} onOpenChange={setShowRenameModal}>
        <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Renomear {selectedIds.size} dataset(s)</DialogTitle>
            <DialogDescription>
              Configure a operação de renomeação em massa das tabelas Bronze e Silver.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            {/* Operação */}
            <div className="space-y-2">
              <Label>Operação</Label>
              <Select value={renameOperation} onValueChange={(v: any) => setRenameOperation(v)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="REPLACE_SCHEMA_PREFIX">Substituir Prefixo do Schema</SelectItem>
                  <SelectItem value="REPLACE_CATALOG">Substituir Catálogo</SelectItem>
                  <SelectItem value="REPLACE_FULL">Substituição Completa</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {renameOperation === "REPLACE_SCHEMA_PREFIX" && "Ex: bronze_old.table → bronze_new.table"}
                {renameOperation === "REPLACE_CATALOG" && "Ex: catalog_old.schema.table → catalog_new.schema.table"}
                {renameOperation === "REPLACE_FULL" && "Substitui o nome completo catalog.schema.table"}
              </p>
            </div>

            {/* Campos de transformação */}
            <div className="grid grid-cols-2 gap-4">
              {renameOperation !== "REPLACE_FULL" && (
                <>
                  <div className="space-y-2">
                    <Label htmlFor="bronze-from">Bronze: De</Label>
                    <Input
                      id="bronze-from"
                      placeholder={renameOperation === "REPLACE_SCHEMA_PREFIX" ? "bronze_old" : "catalog_old"}
                      value={renameBronzeFrom}
                      onChange={(e) => setRenameBronzeFrom(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="bronze-to">Bronze: Para</Label>
                    <Input
                      id="bronze-to"
                      placeholder={renameOperation === "REPLACE_SCHEMA_PREFIX" ? "bronze_new" : "catalog_new"}
                      value={renameBronzeTo}
                      onChange={(e) => setRenameBronzeTo(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="silver-from">Silver: De</Label>
                    <Input
                      id="silver-from"
                      placeholder={renameOperation === "REPLACE_SCHEMA_PREFIX" ? "silver_old" : "catalog_old"}
                      value={renameSilverFrom}
                      onChange={(e) => setRenameSilverFrom(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="silver-to">Silver: Para</Label>
                    <Input
                      id="silver-to"
                      placeholder={renameOperation === "REPLACE_SCHEMA_PREFIX" ? "silver_new" : "catalog_new"}
                      value={renameSilverTo}
                      onChange={(e) => setRenameSilverTo(e.target.value)}
                    />
                  </div>
                </>
              )}
              {renameOperation === "REPLACE_FULL" && (
                <>
                  <div className="space-y-2">
                    <Label htmlFor="bronze-to-full">Bronze: Novo Nome Completo</Label>
                    <Input
                      id="bronze-to-full"
                      placeholder="catalog.schema.table"
                      value={renameBronzeTo}
                      onChange={(e) => setRenameBronzeTo(e.target.value)}
                      className="font-mono text-sm"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="silver-to-full">Silver: Novo Nome Completo</Label>
                    <Input
                      id="silver-to-full"
                      placeholder="catalog.schema.table"
                      value={renameSilverTo}
                      onChange={(e) => setRenameSilverTo(e.target.value)}
                      className="font-mono text-sm"
                    />
                  </div>
                </>
              )}
            </div>

            {/* Criar schemas */}
            <div className="flex items-center space-x-2">
              <Checkbox
                id="create-schemas"
                checked={renameCreateSchemas}
                onCheckedChange={(checked) => setRenameCreateSchemas(Boolean(checked))}
              />
              <Label htmlFor="create-schemas" className="font-normal cursor-pointer">
                Criar schemas automaticamente se não existirem
              </Label>
            </div>

            {/* Preview */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label>Preview das Mudanças</Label>
                <Button size="sm" variant="outline" onClick={loadRenamePreview} disabled={loadingPreview}>
                  {loadingPreview ? (
                    <><Loader2 className="h-3 w-3 mr-2 animate-spin" /> Carregando...</>
                  ) : (
                    <><RefreshCw className="h-3 w-3 mr-2" /> Gerar Preview</>
                  )}
                </Button>
              </div>

              {renamePreview && (
                <div className="border rounded-lg p-4 bg-muted/30 max-h-80 overflow-y-auto">
                  {renamePreview.schemas_to_create?.length > 0 && (
                    <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded">
                      <p className="text-sm font-medium text-blue-900">Schemas a criar:</p>
                      <ul className="text-xs text-blue-700 mt-1 space-y-0.5">
                        {renamePreview.schemas_to_create.map((s: string) => (
                          <li key={s}>• {s}</li>
                        ))}
                      </ul>
                    </div>
                  )}

                  <div className="space-y-2">
                    {renamePreview.results?.map((result: any, idx: number) => (
                      <div
                        key={result.dataset_id}
                        className={`p-3 rounded border ${
                          result.status === 'PREVIEW'
                            ? 'bg-green-50 border-green-200'
                            : result.status === 'CONFLICT'
                            ? 'bg-yellow-50 border-yellow-300'
                            : 'bg-red-50 border-red-300'
                        }`}
                      >
                        <div className="text-xs font-medium mb-1">
                          {result.dataset_name}
                          {result.status !== 'PREVIEW' && (
                            <span className="ml-2 text-red-600">• {result.message}</span>
                          )}
                        </div>
                        {result.status === 'PREVIEW' && (
                          <>
                            <div className="text-xs space-y-1">
                              <div className="flex items-center gap-2">
                                <span className="text-muted-foreground">Bronze:</span>
                                <code className="text-red-600 line-through">{result.old_bronze}</code>
                                <ArrowRight className="h-3 w-3" />
                                <code className="text-green-600">{result.new_bronze}</code>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="text-muted-foreground">Silver:</span>
                                <code className="text-red-600 line-through">{result.old_silver}</code>
                                <ArrowRight className="h-3 w-3" />
                                <code className="text-green-600">{result.new_silver}</code>
                              </div>
                            </div>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setShowRenameModal(false);
                setRenamePreview(null);
              }}
              disabled={renaming}
            >
              Cancelar
            </Button>
            <Button
              onClick={handleBulkRename}
              disabled={!renamePreview || renaming}
            >
              {renaming ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Renomeando...
                </>
              ) : (
                <>
                  <Edit className="h-4 w-4 mr-2" />
                  Renomear {selectedIds.size} dataset(s)
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
    </TooltipProvider>
  );
};

export default Datasets;
