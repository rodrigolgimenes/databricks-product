import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  ArrowLeft, Play, Database, CheckCircle, AlertCircle, Clock, Layers, Eye, History,
  ChevronDown, ChevronRight, RefreshCw, Trash2, AlertTriangle, Settings,
} from "lucide-react";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import { RunDetailPanel } from "@/components/RunDetailPanel";
import { IncrementalConfigDialog } from "@/components/IncrementalConfigDialog";
import * as api from "@/lib/api";

const stateColor: Record<string, string> = {
  ACTIVE: "bg-green-500 text-white",
  PAUSED: "bg-yellow-500 text-white",
  DRAFT: "bg-blue-500 text-white",
  BLOCKED_SCHEMA_CHANGE: "bg-red-500 text-white",
  DEPRECATED: "bg-gray-500 text-white",
};

const DatasetDetail = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [dataset, setDataset] = useState<any>(null);
  const [runs, setRuns] = useState<any>(null);
  const [schema, setSchema] = useState<any>(null);
  const [preview, setPreview] = useState<any>(null);
  const [stateChanges, setStateChanges] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [enqueuing, setEnqueuing] = useState(false);
  const [enqueueMsg, setEnqueueMsg] = useState("");
  const [expandedRun, setExpandedRun] = useState<string | null>(null);
  // Delete
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [dropTables, setDropTables] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState("");
  // Incremental config
  const [configOpen, setConfigOpen] = useState(false);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    api.getDataset(id).then((d) => {
      setDataset(d.item);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [id]);

  const loadRuns = () => {
    if (!id) return;
    api.getDatasetRuns(id).then(setRuns).catch(console.error);
  };

  const loadSchema = () => {
    if (!id) return;
    api.getDatasetSchema(id).then(setSchema).catch(console.error);
  };

  const loadPreview = () => {
    if (!id) return;
    api.getDatasetPreview(id).then(setPreview).catch((e) =>
      setPreview({ error: e.message })
    );
  };

  const loadStateChanges = () => {
    if (!id) return;
    api.getDatasetStateChanges(id).then((d) => setStateChanges(d.items || [])).catch(console.error);
  };

  const handleConfigUpdated = () => {
    // Reload dataset to get updated config
    if (!id) return;
    api.getDataset(id).then((d) => setDataset(d.item)).catch(console.error);
  };

  const handleEnqueue = async () => {
    if (!id) return;
    setEnqueuing(true);
    setEnqueueMsg("");
    try {
      const res = await api.enqueueDataset(id);
      setEnqueueMsg(`Enfileirado! queue_id: ${res.queue_id?.slice(0, 8)}...`);
    } catch (e: any) {
      setEnqueueMsg(`Erro: ${e.message}`);
    } finally {
      setEnqueuing(false);
    }
  };

  const handleDelete = async () => {
    if (!id || !dataset) return;
    setDeleting(true);
    setDeleteError("");
    try {
      await api.deleteDataset(id, deleteConfirmText, dropTables);
      setDeleteOpen(false);
      navigate("/datasets");
    } catch (e: any) {
      setDeleteError(e.message || "Erro ao excluir dataset.");
    } finally {
      setDeleting(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <LoadingSpinner size="lg" text="Carregando dataset..." />
      </div>
    );
  }

  if (!dataset) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" onClick={() => navigate("/datasets")}>
          <ArrowLeft className="h-4 w-4 mr-2" /> Voltar
        </Button>
        <p className="text-muted-foreground">Dataset não encontrado.</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate("/datasets")}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold">{dataset.dataset_name}</h1>
              <Badge className={stateColor[dataset.execution_state] || "bg-gray-100"}>
                {dataset.execution_state}
              </Badge>
            </div>
            <p className="text-sm text-muted-foreground mt-1 font-mono">{dataset.dataset_id}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {enqueueMsg && (
            <span className="text-sm text-muted-foreground mr-2">{enqueueMsg}</span>
          )}
          <Button 
            variant="outline" 
            onClick={() => setConfigOpen(true)}
          >
            <Settings className="h-4 w-4 mr-2" />
            Configurar Carga
          </Button>
          <Button onClick={handleEnqueue} disabled={enqueuing}>
            <Play className="h-4 w-4 mr-2" />
            {enqueuing ? "Enfileirando..." : "Executar"}
          </Button>
          <Button
            variant="outline"
            size="icon"
            className="text-destructive hover:bg-destructive/10"
            onClick={() => { setDeleteOpen(true); setDeleteConfirmText(""); setDeleteError(""); setDropTables(false); }}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Incremental Configuration Dialog */}
      <IncrementalConfigDialog
        open={configOpen}
        onOpenChange={setConfigOpen}
        datasetId={dataset.dataset_id}
        datasetName={dataset.dataset_name}
        currentConfig={{
          enable_incremental: dataset.enable_incremental,
          incremental_strategy: dataset.incremental_strategy,
          bronze_mode: dataset.bronze_mode,
          override_watermark_value: dataset.override_watermark_value,
          incremental_metadata: dataset.incremental_metadata,
        }}
        onConfigUpdated={handleConfigUpdated}
      />

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-destructive">
              <AlertTriangle className="h-5 w-5" /> Excluir Dataset
            </DialogTitle>
            <DialogDescription>
              Esta ação é <strong>irreversível</strong>. Todos os dados relacionados serão excluídos
              permanentemente (execuções, schemas, fila, histórico).
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <div className="p-3 bg-destructive/10 border border-destructive/20 rounded-lg text-sm">
              <p className="font-medium">Dataset: <span className="font-mono">{dataset.dataset_name}</span></p>
              <p className="text-xs text-muted-foreground mt-1">ID: {dataset.dataset_id}</p>
              {dataset.bronze_table && <p className="text-xs text-muted-foreground">Bronze: {dataset.bronze_table}</p>}
              {dataset.silver_table && <p className="text-xs text-muted-foreground">Silver: {dataset.silver_table}</p>}
            </div>
            <div className="space-y-2">
              <Label htmlFor="confirm-name">
                Digite <span className="font-mono font-bold">{dataset.dataset_name}</span> para confirmar:
              </Label>
              <Input
                id="confirm-name"
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
                placeholder={dataset.dataset_name}
                autoComplete="off"
              />
            </div>
            {(dataset.bronze_table || dataset.silver_table) && (
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={dropTables}
                  onChange={(e) => setDropTables(e.target.checked)}
                  className="rounded border-destructive"
                />
                <span>
                  Excluir também as tabelas Delta (Bronze/Silver)
                </span>
              </label>
            )}
            {deleteError && (
              <p className="text-sm text-destructive">{deleteError}</p>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteOpen(false)} disabled={deleting}>
              Cancelar
            </Button>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleting || deleteConfirmText !== dataset.dataset_name}
            >
              <Trash2 className="h-4 w-4 mr-2" />
              {deleting ? "Excluindo..." : "Excluir permanentemente"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Info Card */}
      <Card>
        <CardContent className="p-6">
          <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
            <div>
              <p className="text-sm text-muted-foreground">Project ID</p>
              <p className="font-medium font-mono text-sm">{dataset.project_id || "—"}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Area ID</p>
              <p className="font-medium font-mono text-sm">{dataset.area_id || "—"}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Source Type</p>
              <Badge variant="outline">{dataset.source_type}</Badge>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Connection ID</p>
              <p className="font-medium font-mono text-sm">{dataset.connection_id || "—"}</p>
            </div>
            <div className="col-span-2 md:col-span-3">
              <p className="text-sm text-muted-foreground mb-1">Bronze Table</p>
              <p className="font-medium font-mono text-xs break-all bg-slate-50 p-2 rounded border">
                {dataset.bronze_table || "—"}
              </p>
            </div>
            <div className="col-span-2 md:col-span-3">
              <p className="text-sm text-muted-foreground mb-1">Silver Table</p>
              <p className="font-medium font-mono text-xs break-all bg-slate-50 p-2 rounded border">
                {dataset.silver_table || "—"}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Schema Version</p>
              <p className="font-medium">{dataset.current_schema_ver ?? "—"}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Criado em</p>
              <p className="font-medium">
                {dataset.created_at ? new Date(dataset.created_at).toLocaleString("pt-BR") : "—"}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Criado por</p>
              <p className="font-medium">{dataset.created_by || "—"}</p>
            </div>
          </div>
          
          {/* Incremental Load Configuration */}
          {dataset.enable_incremental && (
            <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-lg">
              <div className="flex items-center gap-2 mb-3">
                <div className="h-2 w-2 bg-green-500 rounded-full"></div>
                <h3 className="text-sm font-semibold text-green-900">Carga Incremental Habilitada</h3>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {(() => {
                  let watermarkColumn = null;
                  let lookbackDays = null;
                  try {
                    const metadata = dataset.incremental_metadata ? JSON.parse(dataset.incremental_metadata) : {};
                    watermarkColumn = metadata.watermark_column;
                    lookbackDays = metadata.lookback_days;
                  } catch {}
                  
                  return (
                    <>
                      <div>
                        <p className="text-xs text-green-700">Coluna Data Delta</p>
                        {watermarkColumn ? (
                          <p className="font-mono font-medium text-sm text-green-900">{watermarkColumn}</p>
                        ) : (
                          <p className="text-xs text-amber-700 italic">⚠️ Não configurada</p>
                        )}
                      </div>
                      <div>
                        <p className="text-xs text-green-700">Modo Bronze</p>
                        <p className="font-medium text-sm text-green-900">{dataset.bronze_mode || "CURRENT"}</p>
                      </div>
                      <div>
                        <p className="text-xs text-green-700">Estratégia</p>
                        <p className="font-medium text-sm text-green-900">{dataset.incremental_strategy || "—"}</p>
                      </div>
                      {lookbackDays && (
                        <div>
                          <p className="text-xs text-green-700">Lookback Days</p>
                          <p className="font-medium text-sm text-green-900">{lookbackDays} dias</p>
                        </div>
                      )}
                    </>
                  );
                })()}
              </div>
              {(() => {
                let watermarkColumn = null;
                try {
                  const metadata = dataset.incremental_metadata ? JSON.parse(dataset.incremental_metadata) : {};
                  watermarkColumn = metadata.watermark_column;
                } catch {}
                
                if (!watermarkColumn) {
                  return (
                    <div className="mt-3 p-2 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
                      <strong>⚠️ Atenção:</strong> Coluna de data delta não configurada. 
                      <button 
                        onClick={() => setConfigOpen(true)}
                        className="ml-1 underline font-medium hover:text-amber-900"
                      >
                        Clique aqui para configurar
                      </button>
                    </div>
                  );
                }
                return null;
              })()}
            </div>
          )}
          
          {!dataset.enable_incremental && (
            <div className="mt-4 p-3 bg-slate-50 border border-slate-200 rounded-lg text-sm text-slate-700">
              <strong>Carga Incremental:</strong> Desabilitada (sempre FULL)
            </div>
          )}
        </CardContent>
      </Card>

      {/* Tabs */}
      <Tabs defaultValue="runs" onValueChange={(v) => {
        if (v === "runs" && !runs) loadRuns();
        if (v === "schema" && !schema) loadSchema();
        if (v === "preview" && !preview) loadPreview();
        if (v === "history" && stateChanges.length === 0) loadStateChanges();
      }}>
        <TabsList>
          <TabsTrigger value="runs" onClick={() => !runs && loadRuns()}>
            <Clock className="h-4 w-4 mr-2" /> Execuções
          </TabsTrigger>
          <TabsTrigger value="schema" onClick={() => !schema && loadSchema()}>
            <Layers className="h-4 w-4 mr-2" /> Schema
          </TabsTrigger>
          <TabsTrigger value="preview" onClick={() => !preview && loadPreview()}>
            <Eye className="h-4 w-4 mr-2" /> Preview
          </TabsTrigger>
          <TabsTrigger value="history" onClick={() => stateChanges.length === 0 && loadStateChanges()}>
            <History className="h-4 w-4 mr-2" /> Histórico
          </TabsTrigger>
        </TabsList>

        {/* Runs */}
        <TabsContent value="runs">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle>Execuções</CardTitle>
              <Button variant="ghost" size="sm" onClick={() => { setRuns(null); loadRuns(); }}>
                <RefreshCw className="h-4 w-4 mr-1" /> Atualizar
              </Button>
            </CardHeader>
            <CardContent>
              {!runs ? (
                <LoadingSpinner text="Carregando..." />
              ) : (
                <div className="space-y-3">
                  {(runs.batch_process || []).length === 0 && (
                    <p className="text-muted-foreground text-center py-6">Nenhuma execução encontrada.</p>
                  )}
                  {(runs.batch_process || []).map((bp: any, i: number) => {
                    const isExpanded = expandedRun === bp.run_id;
                    const durationSec = bp.started_at && bp.finished_at
                      ? Math.round((new Date(bp.finished_at).getTime() - new Date(bp.started_at).getTime()) / 1000)
                      : null;
                    const fmtDuration = (s: number | null) => {
                      if (!s) return "—";
                      if (s < 60) return `${s}s`;
                      const m = Math.floor(s / 60);
                      const sec = s % 60;
                      return m < 60 ? `${m}m ${sec}s` : `${Math.floor(m / 60)}h ${m % 60}m`;
                    };

                    return (
                      <div key={i} className={`border rounded-lg overflow-hidden transition-all ${
                        isExpanded ? "ring-2 ring-primary/20" : ""
                      } ${bp.status === "FAILED" ? "border-red-200" : ""}`}>
                        <button
                          className={`w-full flex items-center justify-between p-4 hover:bg-muted/30 transition-colors text-left ${
                            bp.status === "FAILED" ? "bg-red-50/30" : ""
                          }`}
                          onClick={() => setExpandedRun(isExpanded ? null : bp.run_id)}
                        >
                          <div className="flex items-center gap-3">
                            {bp.status === "SUCCEEDED" ? (
                              <CheckCircle className="h-5 w-5 text-green-500" />
                            ) : bp.status === "FAILED" ? (
                              <AlertCircle className="h-5 w-5 text-red-500" />
                            ) : (
                              <Clock className="h-5 w-5 text-blue-500 animate-spin" />
                            )}
                            <div>
                              <p className="font-mono text-sm">{bp.run_id?.slice(0, 12)}...</p>
                              <p className="text-xs text-muted-foreground">
                                {bp.started_at ? new Date(bp.started_at).toLocaleString("pt-BR") : ""}
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-4 text-sm">
                            <Badge variant={bp.status === "SUCCEEDED" ? "default" : bp.status === "FAILED" ? "destructive" : "secondary"}>
                              {bp.status}
                            </Badge>
                            <span className="text-xs text-muted-foreground w-16 text-right">
                              {fmtDuration(durationSec)}
                            </span>
                            <div className="text-right text-xs text-muted-foreground">
                              <p>Bronze: {bp.bronze_row_count != null ? Number(bp.bronze_row_count).toLocaleString("pt-BR") : "—"}</p>
                              <p>Silver: {bp.silver_row_count != null ? Number(bp.silver_row_count).toLocaleString("pt-BR") : "—"}</p>
                            </div>
                            {isExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                          </div>
                        </button>
                        {isExpanded && bp.run_id && (
                          <div className="px-4 pb-4 border-t">
                            <RunDetailPanel runId={bp.run_id} batchProcess={bp} compact />
                          </div>
                        )}
                      </div>
                    );
                  })}
                  {runs.last_error && (
                    <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
                      <p className="font-medium text-red-800 text-sm">Último erro:</p>
                      <p className="text-sm text-red-700 mt-1">{runs.last_error.human}</p>
                      <p className="text-xs text-red-500 mt-1 font-mono">
                        {runs.last_error.technical?.error_class}: {runs.last_error.technical?.error_message}
                      </p>
                      {runs.last_error.debug?.stacktrace && (
                        <details className="mt-2">
                          <summary className="text-xs text-red-600 cursor-pointer">Stacktrace</summary>
                          <pre className="text-xs text-red-700 mt-1 p-2 bg-red-100 rounded overflow-auto max-h-48 whitespace-pre-wrap font-mono">
                            {runs.last_error.debug.stacktrace}
                          </pre>
                        </details>
                      )}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Schema */}
        <TabsContent value="schema">
          <Card>
            <CardHeader>
              <CardTitle>Versões de Schema</CardTitle>
            </CardHeader>
            <CardContent>
              {!schema ? (
                <LoadingSpinner text="Carregando..." />
              ) : (
                <div className="space-y-4">
                  {(schema.versions || []).length === 0 && (
                    <p className="text-muted-foreground text-center py-6">Nenhum schema registrado.</p>
                  )}
                  {(schema.versions || []).map((v: any, i: number) => (
                    <div key={i} className="p-4 border rounded-lg">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <span className="font-bold text-lg">v{v.schema_version}</span>
                          <Badge variant={v.status === "ACTIVE" ? "default" : v.status === "PENDING" ? "secondary" : "outline"}>
                            {v.status}
                          </Badge>
                        </div>
                        <span className="text-sm text-muted-foreground">
                          {v.created_at ? new Date(v.created_at).toLocaleString("pt-BR") : ""}
                        </span>
                      </div>
                      <p className="text-xs text-muted-foreground mt-2 font-mono">
                        fingerprint: {v.schema_fingerprint || "—"}
                      </p>
                    </div>
                  ))}
                  {(schema.diff || []).length > 0 && (
                    <div className="mt-4 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
                      <p className="font-medium text-yellow-800 mb-2">Diferenças (Active → Pending):</p>
                      {schema.diff.map((d: any, i: number) => (
                        <p key={i} className="text-sm text-yellow-700">
                          {d.type}: <span className="font-mono">{d.column}</span>
                          {d.type === "TYPE_CHANGE" && ` (${d.active?.type} → ${d.pending?.type})`}
                        </p>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Preview */}
        <TabsContent value="preview">
          <Card>
            <CardHeader>
              <CardTitle>Preview dos Dados (Silver)</CardTitle>
            </CardHeader>
            <CardContent>
              {!preview ? (
                <LoadingSpinner text="Carregando preview..." />
              ) : preview.error ? (
                <p className="text-muted-foreground">{preview.error}</p>
              ) : (
                <>
                  <div className="text-xs text-muted-foreground mb-2">
                    {(preview.rows || []).length} linha{(preview.rows || []).length !== 1 && 's'} · {(preview.columns || []).length} coluna{(preview.columns || []).length !== 1 && 's'}
                  </div>
                  <div className="border rounded-lg overflow-hidden">
                    <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                      <table className="text-sm border-collapse" style={{ minWidth: `${Math.max(800, (preview.columns || []).length * 160)}px` }}>
                        <thead className="sticky top-0 z-10">
                          <tr className="bg-muted/80 backdrop-blur-sm border-b">
                            <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground border-r bg-muted/80 w-12">#</th>
                            {(preview.columns || []).map((col: any, i: number) => (
                              <th key={i} className="text-left px-3 py-2 font-medium text-xs text-muted-foreground whitespace-nowrap border-r last:border-r-0 min-w-[120px]">
                                <span className="text-[10px] text-muted-foreground/60 mr-1">Aᵇc</span> {col.name}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {(preview.rows || []).map((row: any[], ri: number) => (
                            <tr key={ri} className="border-b last:border-b-0 hover:bg-blue-50/50 transition-colors">
                              <td className="text-right px-3 py-1.5 text-xs text-muted-foreground border-r bg-muted/30 tabular-nums">{ri + 1}</td>
                              {row.map((cell, ci) => (
                                <td key={ci} className="px-3 py-1.5 whitespace-nowrap font-mono text-xs border-r last:border-r-0 max-w-[300px] truncate" title={cell === null ? 'null' : String(cell)}>
                                  {cell === null ? <span className="text-muted-foreground/50 italic">null</span> : String(cell)}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                  {(preview.rows || []).length === 0 && (
                    <p className="text-center py-6 text-muted-foreground">Sem dados para exibir.</p>
                  )}
                </>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* State Changes History */}
        <TabsContent value="history">
          <Card>
            <CardHeader>
              <CardTitle>Histórico de Mudanças de Estado</CardTitle>
            </CardHeader>
            <CardContent>
              {stateChanges.length === 0 ? (
                <p className="text-muted-foreground text-center py-6">Nenhuma mudança de estado registrada.</p>
              ) : (
                <div className="space-y-3">
                  {stateChanges.map((sc, i) => (
                    <div key={i} className="flex items-center gap-4 p-3 border rounded-lg">
                      <div className="flex-1">
                        <div className="flex items-center gap-2">
                          <Badge variant="outline">{sc.old_state}</Badge>
                          <span className="text-muted-foreground">→</span>
                          <Badge>{sc.new_state}</Badge>
                        </div>
                        <p className="text-sm text-muted-foreground mt-1">{sc.reason}</p>
                      </div>
                      <div className="text-right text-xs text-muted-foreground">
                        <p>{sc.changed_by}</p>
                        <p>{sc.changed_at ? new Date(sc.changed_at).toLocaleString("pt-BR") : ""}</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default DatasetDetail;
