import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import {
  ArrowLeft, ArrowRight, CheckCircle, Database, Loader2,
  List, FileSpreadsheet, XCircle, Copy, AlertTriangle, Pencil, Check, X, Settings,
} from "lucide-react";
import * as api from "@/lib/api";
import BatchProgressOverlay from "@/components/BatchProgressOverlay";
import { SupabaseTableSelector } from "@/components/SupabaseTableSelector";
import { NamingConventionManager } from "@/components/NamingConventionManager";

const STEPS = ["Projeto", "Fonte", "Dataset", "Revisão"];

type BulkItem = {
  index: number;
  dataset_name: string;
  status: string;
  message: string;
  dataset_id?: string;
};

type BulkSummary = {
  total: number;
  valid?: number;
  created?: number;
  error: number;
  duplicate: number;
  exists: number;
  failed?: number;
};

const statusConfig: Record<string, { label: string; color: string; icon: typeof CheckCircle }> = {
  VALID: { label: "Válido", color: "text-green-600 bg-green-50", icon: CheckCircle },
  CREATED: { label: "Criado", color: "text-green-600 bg-green-50", icon: CheckCircle },
  ERROR: { label: "Erro", color: "text-red-600 bg-red-50", icon: XCircle },
  DUPLICATE: { label: "Duplicado", color: "text-yellow-600 bg-yellow-50", icon: Copy },
  EXISTS: { label: "Já existe", color: "text-orange-600 bg-orange-50", icon: AlertTriangle },
};

const CreateDataset = () => {
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  // Data for selects
  const [projects, setProjects] = useState<any[]>([]);
  const [areas, setAreas] = useState<any[]>([]);
  const [connections, setConnections] = useState<any[]>([]);
  const [namingConventions, setNamingConventions] = useState<any[]>([]);
  const [selectedConvention, setSelectedConvention] = useState<number | null>(null);

  // Form state
  const [projectId, setProjectId] = useState("");
  const [areaId, setAreaId] = useState("");
  const [sourceType, setSourceType] = useState("ORACLE");
  const [connectionId, setConnectionId] = useState("");
  
  // Custom names (for display override)
  const [customProjectName, setCustomProjectName] = useState<string | null>(null);
  const [customAreaName, setCustomAreaName] = useState<string | null>(null);
  
  // Inline editing states
  const [editingProject, setEditingProject] = useState(false);
  const [editingArea, setEditingArea] = useState(false);
  const [editProjectValue, setEditProjectValue] = useState("");
  const [editAreaValue, setEditAreaValue] = useState("");
  const [saving, setSaving] = useState(false);

  // Dataset step — single vs bulk
  const [mode, setMode] = useState<"single" | "bulk">("single");
  const [datasetName, setDatasetName] = useState("");
  const [bulkText, setBulkText] = useState("");

  // Supabase tables selection
  const [supabaseTables, setSupabaseTables] = useState<string[]>([]);

  // Bulk validation
  const [bulkItems, setBulkItems] = useState<BulkItem[]>([]);
  const [bulkSummary, setBulkSummary] = useState<BulkSummary | null>(null);
  const [validating, setValidating] = useState(false);
  const [validated, setValidated] = useState(false);
  
  // NOVO: Nomenclatura customizada
  const [useCustomNaming, setUseCustomNaming] = useState(false);
  const [namingPreview, setNamingPreview] = useState<any>(null);
  const [namingPreviewLoading, setNamingPreviewLoading] = useState(false);
  const [namingPreviewError, setNamingPreviewError] = useState("");
  const [customCatalog, setCustomCatalog] = useState("");
  const [customBronzeSchema, setCustomBronzeSchema] = useState("");
  const [customBronzeTable, setCustomBronzeTable] = useState("");
  const [customSilverSchema, setCustomSilverSchema] = useState("");
  const [customSilverTable, setCustomSilverTable] = useState("");

  // Bulk creation result
  const [bulkResult, setBulkResult] = useState<{ results: BulkItem[]; summary: BulkSummary } | null>(null);

  // Batch progress
  const [activeBatchId, setActiveBatchId] = useState<string | null>(null);
  
  // Naming convention manager modal
  const [showConventionManager, setShowConventionManager] = useState(false);

  const loadNamingConventions = () => {
    api.getNamingConventions()
      .then((d) => {
        setNamingConventions(d.items || []);
        // Pré-selecionar a convenção ativa
        const active = d.items?.find((nc: any) => nc.is_active);
        if (active) setSelectedConvention(active.naming_version);
      })
      .catch(console.error);
  };
  
  useEffect(() => {
    api.getProjects().then((d) => setProjects(d.items || [])).catch(console.error);
    loadNamingConventions();
  }, []);

  useEffect(() => {
    if (projectId) {
      api.getAreas(projectId).then((d) => setAreas(d.items || [])).catch(console.error);
    }
  }, [projectId]);

  useEffect(() => {
    if (projectId && areaId) {
      api.getOracleConnections(projectId, areaId).then((d) => setConnections(d.items || [])).catch(console.error);
    }
  }, [projectId, areaId]);
  
  // Buscar preview de nomenclatura quando entrar no step 3 (single mode) ou quando trocar convenção
  useEffect(() => {
    if (step === 3 && mode === "single" && areaId && datasetName) {
      const payload: any = { area_id: areaId, dataset_name: datasetName };
      if (selectedConvention !== null) {
        payload.naming_version = selectedConvention;
      }
      
      setNamingPreviewLoading(true);
      setNamingPreviewError("");
      api.previewDatasetNaming(payload)
        .then((res) => {
          setNamingPreview(res.preview);
          // Inicializar campos customizados com valores do preview
          if (res.preview.bronze_parts) {
            setCustomCatalog(res.preview.bronze_parts.catalog);
            setCustomBronzeSchema(res.preview.bronze_parts.schema);
            setCustomBronzeTable(res.preview.bronze_parts.table);
          }
          if (res.preview.silver_parts) {
            setCustomSilverSchema(res.preview.silver_parts.schema);
            setCustomSilverTable(res.preview.silver_parts.table);
          }
        })
        .catch((e) => {
          setNamingPreview(null);
          setNamingPreviewError(e.message || "Erro ao carregar preview de nomenclatura");
        })
        .finally(() => setNamingPreviewLoading(false));
    }
  }, [step, mode, areaId, datasetName, selectedConvention]);

  const bulkLines = bulkText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  const canNext = () => {
    if (step === 0) return projectId && areaId;
    if (step === 1) {
      // Supabase doesn't need connection_id
      if (sourceType === "SUPABASE") return true;
      return sourceType && connectionId;
    }
    if (step === 2) {
      // For Supabase, check if tables are selected
      if (sourceType === "SUPABASE") return supabaseTables.length > 0;
      if (mode === "single") return datasetName.trim().length > 0;
      return validated && (bulkSummary?.valid ?? 0) > 0;
    }
    return true;
  };

  const handleValidate = async () => {
    setValidating(true);
    setError("");
    try {
      const res = await api.validateDatasetsBulk({
        project_id: projectId,
        area_id: areaId,
        source_type: sourceType,
        connection_id: connectionId,
        dataset_names: bulkLines,
      });
      setBulkItems(res.items || []);
      setBulkSummary(res.summary || null);
      setValidated(true);
    } catch (e: any) {
      setError(e.message || "Erro ao validar");
    } finally {
      setValidating(false);
    }
  };

  const handleSubmitSingle = async () => {
    setSubmitting(true);
    setError("");
    try {
      const payload: any = {
        project_id: projectId,
        area_id: areaId,
        dataset_name: datasetName.trim(),
        source_type: sourceType,
        connection_id: connectionId,
      };
      
      // Se customização está ativa, incluir nomes customizados
      if (useCustomNaming && customCatalog && customBronzeSchema && customBronzeTable && customSilverSchema && customSilverTable) {
        payload.custom_bronze_table = `${customCatalog}.${customBronzeSchema}.${customBronzeTable}`;
        payload.custom_silver_table = `${customCatalog}.${customSilverSchema}.${customSilverTable}`;
      }
      // Enviar naming_version selecionada
      if (selectedConvention !== null) {
        payload.naming_version = selectedConvention;
      }
      
      const res = await api.createDataset(payload);
      navigate(`/datasets/${res.item.dataset_id}`);
    } catch (e: any) {
      setError(e.message || "Erro ao criar dataset");
    } finally {
      setSubmitting(false);
    }
  };

  const handleSubmitBulk = async () => {
    setSubmitting(true);
    setError("");
    try {
      const validNames = bulkItems.filter((it) => it.status === "VALID").map((it) => it.dataset_name);
      const res = await api.batchCreateDatasets({
        project_id: projectId,
        area_id: areaId,
        source_type: sourceType,
        connection_id: connectionId,
        dataset_names: validNames,
        naming_version: selectedConvention ?? undefined,
      });
      setActiveBatchId(res.batch_id);
    } catch (e: any) {
      setError(e.message || "Erro ao criar datasets");
    } finally {
      setSubmitting(false);
    }
  };

  const handleBatchComplete = (results: BulkItem[], summary: BulkSummary) => {
    setActiveBatchId(null);
    setBulkResult({ results, summary });
  };

  const handleSubmit = () => {
    if (sourceType === "SUPABASE") {
      handleSubmitSupabase();
    } else if (mode === "single") {
      handleSubmitSingle();
    } else {
      handleSubmitBulk();
    }
  };

  const handleSubmitSupabase = async () => {
    setSubmitting(true);
    setError("");
    try {
      // Transform Supabase table names to follow naming convention
      // Replace dots with underscores (public.table_name -> public_table_name)
      const transformedNames = supabaseTables.map(tableName => 
        tableName.replace(/\./g, '_')
      );

      // Create datasets for all selected Supabase tables
      const res = await api.batchCreateDatasets({
        project_id: projectId,
        area_id: areaId,
        source_type: sourceType,
        connection_id: "supabase-default",
        dataset_names: transformedNames,
        naming_version: selectedConvention ?? undefined,
      });
      setActiveBatchId(res.batch_id);
    } catch (e: any) {
      setError(e.message || "Erro ao criar datasets do Supabase");
    } finally {
      setSubmitting(false);
    }
  };

  // When bulk text changes, reset validation
  const handleBulkTextChange = (val: string) => {
    setBulkText(val);
    setValidated(false);
    setBulkItems([]);
    setBulkSummary(null);
  };

  const projectName = customProjectName || projects.find((p) => p.project_id === projectId)?.project_name || projectId;
  const areaName = customAreaName || areas.find((a) => a.area_id === areaId)?.area_name || areaId;

  // Batch progress overlay
  if (activeBatchId) {
    return <BatchProgressOverlay batchId={activeBatchId} onComplete={handleBatchComplete} />;
  }

  // If bulk creation is done, show result screen
  if (bulkResult) {
    const s = bulkResult.summary;
    return (
      <div className="space-y-6 max-w-4xl mx-auto">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate("/datasets")}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-2xl font-bold">Criação em Massa — Resultado</h1>
            <p className="text-muted-foreground mt-1">
              {s.created ?? 0} criados · {s.exists ?? 0} já existiam · {s.duplicate ?? 0} duplicados · {(s.error ?? 0) + (s.failed ?? 0)} erros
            </p>
          </div>
        </div>

        {/* Summary cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="flex items-center p-4">
              <div className="p-2 bg-green-100 rounded-lg mr-3">
                <CheckCircle className="h-5 w-5 text-green-600" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Criados</p>
                <p className="text-2xl font-bold text-green-700">{s.created ?? 0}</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex items-center p-4">
              <div className="p-2 bg-orange-100 rounded-lg mr-3">
                <AlertTriangle className="h-5 w-5 text-orange-600" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Já existiam</p>
                <p className="text-2xl font-bold text-orange-700">{s.exists ?? 0}</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex items-center p-4">
              <div className="p-2 bg-yellow-100 rounded-lg mr-3">
                <Copy className="h-5 w-5 text-yellow-600" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Duplicados</p>
                <p className="text-2xl font-bold text-yellow-700">{s.duplicate ?? 0}</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex items-center p-4">
              <div className="p-2 bg-red-100 rounded-lg mr-3">
                <XCircle className="h-5 w-5 text-red-600" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Erros</p>
                <p className="text-2xl font-bold text-red-700">{(s.error ?? 0) + (s.failed ?? 0)}</p>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Results table */}
        <Card>
          <CardContent className="p-0">
            <div className="max-h-[50vh] overflow-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-background border-b">
                  <tr>
                    <th className="text-left p-3 font-medium">#</th>
                    <th className="text-left p-3 font-medium">Dataset</th>
                    <th className="text-left p-3 font-medium">Status</th>
                    <th className="text-left p-3 font-medium">Detalhes</th>
                  </tr>
                </thead>
                <tbody>
                  {bulkResult.results.map((r, i) => {
                    const cfg = statusConfig[r.status] || statusConfig.ERROR;
                    const Icon = cfg.icon;
                    return (
                      <tr key={i} className="border-b hover:bg-muted/30">
                        <td className="p-3 text-muted-foreground">{i + 1}</td>
                        <td className="p-3 font-mono text-xs">{r.dataset_name}</td>
                        <td className="p-3">
                          <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
                            <Icon className="h-3 w-3" /> {cfg.label}
                          </span>
                        </td>
                        <td className="p-3 text-xs text-muted-foreground">
                          {r.status === "CREATED" && r.dataset_id ? (
                            <button
                              className="text-blue-600 hover:underline"
                              onClick={() => navigate(`/datasets/${r.dataset_id}`)}
                            >
                              Ver dataset →
                            </button>
                          ) : (
                            r.message || ""
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        <div className="flex justify-between">
          <Button variant="outline" onClick={() => navigate("/datasets")}>
            <ArrowLeft className="h-4 w-4 mr-2" /> Ir para Datasets
          </Button>
          <Button onClick={() => { setBulkResult(null); setBulkText(""); setValidated(false); setBulkItems([]); setBulkSummary(null); setStep(2); }}>
            Criar mais datasets
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-3xl mx-auto">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="icon" onClick={() => navigate("/datasets")}>
          <ArrowLeft className="h-5 w-5" />
        </Button>
        <div>
          <h1 className="text-2xl font-bold">Criar Novo Dataset</h1>
          <p className="text-muted-foreground mt-1">Siga o assistente para configurar seu dataset</p>
        </div>
      </div>

      {/* Wizard Steps */}
      <div className="flex items-center justify-between">
        {STEPS.map((s, i) => (
          <div key={i} className="flex items-center">
            <div
              className={`flex items-center justify-center h-10 w-10 rounded-full font-bold text-sm transition-all ${
                i < step
                  ? "bg-green-500 text-white"
                  : i === step
                  ? "bg-gradient-to-r from-primary to-blue-500 text-white shadow-lg"
                  : "bg-muted text-muted-foreground"
              }`}
            >
              {i < step ? <CheckCircle className="h-5 w-5" /> : i + 1}
            </div>
            <span
              className={`ml-2 text-sm font-medium ${
                i === step ? "text-primary" : "text-muted-foreground"
              }`}
            >
              {s}
            </span>
            {i < STEPS.length - 1 && (
              <div className={`mx-4 h-0.5 w-12 ${i < step ? "bg-green-500" : "bg-muted"}`} />
            )}
          </div>
        ))}
      </div>

      {/* Step Content */}
      <Card>
        <CardHeader>
          <CardTitle>{STEPS[step]}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Step 0: Project + Area */}
          {step === 0 && (
            <>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label>Projeto</Label>
                  {projectId && !editingProject && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs"
                      onClick={() => {
                        const currentName = projects.find(p => p.project_id === projectId)?.project_name || '';
                        setEditProjectValue(currentName);
                        setEditingProject(true);
                      }}
                    >
                      <Pencil className="h-3 w-3 mr-1" />
                      Editar nome
                    </Button>
                  )}
                </div>
                {editingProject ? (
                  <div className="flex gap-2">
                    <Input
                      value={editProjectValue}
                      onChange={(e) => setEditProjectValue(e.target.value)}
                      placeholder="Novo nome do projeto"
                      autoFocus
                      onKeyDown={async (e) => {
                        if (e.key === 'Enter' && editProjectValue.trim()) {
                          setSaving(true);
                          try {
                            await api.updateProjectName(projectId, editProjectValue.trim());
                            setProjects(prev => prev.map(p => 
                              p.project_id === projectId ? { ...p, project_name: editProjectValue.trim() } : p
                            ));
                            setEditingProject(false);
                          } catch (err: any) {
                            setError(err.message || 'Erro ao atualizar projeto');
                          } finally {
                            setSaving(false);
                          }
                        }
                        if (e.key === 'Escape') setEditingProject(false);
                      }}
                    />
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      disabled={!editProjectValue.trim() || saving}
                      onClick={async () => {
                        setSaving(true);
                        try {
                          await api.updateProjectName(projectId, editProjectValue.trim());
                          setProjects(prev => prev.map(p => 
                            p.project_id === projectId ? { ...p, project_name: editProjectValue.trim() } : p
                          ));
                          setEditingProject(false);
                        } catch (err: any) {
                          setError(err.message || 'Erro ao atualizar projeto');
                        } finally {
                          setSaving(false);
                        }
                      }}
                    >
                      {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4 text-green-600" />}
                    </Button>
                    <Button type="button" size="icon" variant="ghost" onClick={() => setEditingProject(false)}>
                      <X className="h-4 w-4 text-red-600" />
                    </Button>
                  </div>
                ) : (
                  <Select value={projectId} onValueChange={(v) => { setProjectId(v); setAreaId(""); }}>
                    <SelectTrigger>
                      <SelectValue placeholder="Selecione o projeto" />
                    </SelectTrigger>
                    <SelectContent>
                      {projects.map((p) => (
                        <SelectItem key={p.project_id} value={p.project_id}>
                          {p.project_name} {!p.is_active && "(inativo)"}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label>Área</Label>
                  {areaId && !editingArea && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs"
                      onClick={() => {
                        const currentName = areas.find(a => a.area_id === areaId)?.area_name || '';
                        setEditAreaValue(currentName);
                        setEditingArea(true);
                      }}
                    >
                      <Pencil className="h-3 w-3 mr-1" />
                      Editar nome
                    </Button>
                  )}
                </div>
                {editingArea ? (
                  <div className="flex gap-2">
                    <Input
                      value={editAreaValue}
                      onChange={(e) => setEditAreaValue(e.target.value)}
                      placeholder="Novo nome da área"
                      autoFocus
                      onKeyDown={async (e) => {
                        if (e.key === 'Enter' && editAreaValue.trim()) {
                          setSaving(true);
                          try {
                            await api.updateAreaName(areaId, editAreaValue.trim());
                            setAreas(prev => prev.map(a => 
                              a.area_id === areaId ? { ...a, area_name: editAreaValue.trim() } : a
                            ));
                            setEditingArea(false);
                          } catch (err: any) {
                            setError(err.message || 'Erro ao atualizar área');
                          } finally {
                            setSaving(false);
                          }
                        }
                        if (e.key === 'Escape') setEditingArea(false);
                      }}
                    />
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      disabled={!editAreaValue.trim() || saving}
                      onClick={async () => {
                        setSaving(true);
                        try {
                          await api.updateAreaName(areaId, editAreaValue.trim());
                          setAreas(prev => prev.map(a => 
                            a.area_id === areaId ? { ...a, area_name: editAreaValue.trim() } : a
                          ));
                          setEditingArea(false);
                        } catch (err: any) {
                          setError(err.message || 'Erro ao atualizar área');
                        } finally {
                          setSaving(false);
                        }
                      }}
                    >
                      {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4 text-green-600" />}
                    </Button>
                    <Button type="button" size="icon" variant="ghost" onClick={() => setEditingArea(false)}>
                      <X className="h-4 w-4 text-red-600" />
                    </Button>
                  </div>
                ) : (
                  <Select value={areaId} onValueChange={setAreaId} disabled={!projectId}>
                    <SelectTrigger>
                      <SelectValue placeholder={projectId ? "Selecione a área" : "Selecione um projeto primeiro"} />
                    </SelectTrigger>
                    <SelectContent>
                      {areas.map((a) => (
                        <SelectItem key={a.area_id} value={a.area_id}>
                          {a.area_name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              </div>
            </>
          )}

          {/* Step 1: Source Type + Connection */}
          {step === 1 && (
            <>
              <div className="space-y-2">
                <Label>Tipo de Fonte</Label>
                <Select value={sourceType} onValueChange={setSourceType}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ORACLE">Oracle</SelectItem>
                    <SelectItem value="SUPABASE">Supabase (PostgreSQL)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {sourceType !== "SUPABASE" && (
                <div className="space-y-2">
                  <Label>Conexão</Label>
                  <Select value={connectionId} onValueChange={setConnectionId}>
                    <SelectTrigger>
                      <SelectValue placeholder="Selecione a conexão" />
                    </SelectTrigger>
                    <SelectContent>
                      {connections.map((c) => (
                        <SelectItem key={c.connection_id} value={c.connection_id}>
                          {c.connection_id?.slice(0, 8)}... — {c.jdbc_url || "N/A"}
                        </SelectItem>
                      ))}
                      {connections.length === 0 && (
                        <div className="p-2 text-sm text-muted-foreground text-center">
                          Nenhuma conexão aprovada encontrada
                        </div>
                      )}
                    </SelectContent>
                  </Select>
                </div>
              )}
              {sourceType === "SUPABASE" && (
                <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-center gap-2">
                    <CheckCircle className="h-5 w-5 text-green-600" />
                    <div>
                      <p className="text-sm font-medium text-green-900">Conexão Supabase Configurada</p>
                      <p className="text-xs text-green-700 mt-1">
                        As credenciais do Supabase estão configuradas no backend.
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Step 2: Dataset — single or bulk */}
          {step === 2 && (
            <div className="space-y-4">
              {/* Supabase Table Selector */}
              {sourceType === "SUPABASE" ? (
                <SupabaseTableSelector
                  onTablesSelected={setSupabaseTables}
                  selectedTables={supabaseTables}
                />
              ) : (
                <>
                  {/* Mode toggle */}
                  <div className="flex items-center gap-2 p-1 bg-muted rounded-lg w-fit">
                    <button
                      className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-all ${
                        mode === "single" ? "bg-background shadow text-foreground" : "text-muted-foreground hover:text-foreground"
                      }`}
                      onClick={() => setMode("single")}
                    >
                      <List className="h-4 w-4" /> Unitário
                    </button>
                    <button
                      className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-all ${
                        mode === "bulk" ? "bg-background shadow text-foreground" : "text-muted-foreground hover:text-foreground"
                      }`}
                      onClick={() => setMode("bulk")}
                    >
                      <FileSpreadsheet className="h-4 w-4" /> Em massa
                    </button>
                  </div>

                  {mode === "single" ? (
                <div className="space-y-2">
                  <Label>Nome do Dataset</Label>
                  <Input
                    value={datasetName}
                    onChange={(e) => setDatasetName(e.target.value)}
                    placeholder={sourceType === "ORACLE" ? "Ex: SCHEMA.TABELA@DBLINK" : "Ex: MINHA_TABELA"}
                  />
                  <p className="text-xs text-muted-foreground">
                    {sourceType === "ORACLE"
                      ? "Para Oracle: SCHEMA.TABELA@DBLINK ou apenas TABELA"
                      : "Apenas letras, números e underscore"}
                  </p>
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label>Colar lista de datasets (uma tabela por linha)</Label>
                    <Textarea
                      value={bulkText}
                      onChange={(e) => handleBulkTextChange(e.target.value)}
                      placeholder={
                        "Cole aqui a lista de tabelas, uma por linha:\n\n" +
                        "CMASTER.BASE_DAT_CUS_PRO@CMASTERPRD\n" +
                        "CIVIL_10465_RHP.R010SIT@CMASTERPRD\n" +
                        "CIVIL_10465_RHP.R018CCU@CMASTERPRD"
                      }
                      className="min-h-[160px] font-mono text-xs"
                    />
                    <p className="text-xs text-muted-foreground">
                      {bulkLines.length} {bulkLines.length === 1 ? "linha detectada" : "linhas detectadas"}
                      {bulkLines.length > 0 && " — clique em Validar para verificar"}
                    </p>
                  </div>

                  <div className="flex items-center gap-3">
                    <Button
                      onClick={handleValidate}
                      disabled={validating || bulkLines.length === 0}
                      variant="secondary"
                    >
                      {validating ? (
                        <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Validando...</>
                      ) : (
                        <><CheckCircle className="h-4 w-4 mr-2" /> Validar</>
                      )}
                    </Button>
                    {bulkLines.length > 0 && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => handleBulkTextChange("")}
                      >
                        Limpar lista
                      </Button>
                    )}
                  </div>

                  {/* Validation results */}
                  {validated && bulkSummary && (
                    <div className="space-y-3">
                      <div className="flex items-center gap-4 flex-wrap text-sm">
                        <Badge variant="outline" className="gap-1">
                          Total: {bulkSummary.total}
                        </Badge>
                        <Badge className="bg-green-100 text-green-800 gap-1">
                          <CheckCircle className="h-3 w-3" /> Válidos: {bulkSummary.valid}
                        </Badge>
                        {bulkSummary.error > 0 && (
                          <Badge variant="destructive" className="gap-1">
                            <XCircle className="h-3 w-3" /> Erros: {bulkSummary.error}
                          </Badge>
                        )}
                        {bulkSummary.duplicate > 0 && (
                          <Badge className="bg-yellow-100 text-yellow-800 gap-1">
                            <Copy className="h-3 w-3" /> Duplicados: {bulkSummary.duplicate}
                          </Badge>
                        )}
                        {bulkSummary.exists > 0 && (
                          <Badge className="bg-orange-100 text-orange-800 gap-1">
                            <AlertTriangle className="h-3 w-3" /> Já existem: {bulkSummary.exists}
                          </Badge>
                        )}
                      </div>

                      {/* Items table (scrollable) */}
                      <div className="border rounded-lg max-h-[300px] overflow-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-background border-b">
                            <tr>
                              <th className="text-left p-2 font-medium w-10">#</th>
                              <th className="text-left p-2 font-medium">Dataset</th>
                              <th className="text-left p-2 font-medium w-28">Status</th>
                              <th className="text-left p-2 font-medium">Mensagem</th>
                            </tr>
                          </thead>
                          <tbody>
                            {bulkItems.map((item, i) => {
                              const cfg = statusConfig[item.status] || statusConfig.ERROR;
                              const Icon = cfg.icon;
                              return (
                                <tr key={i} className="border-b hover:bg-muted/30">
                                  <td className="p-2 text-muted-foreground text-xs">{i + 1}</td>
                                  <td className="p-2 font-mono text-xs">{item.dataset_name}</td>
                                  <td className="p-2">
                                    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
                                      <Icon className="h-3 w-3" /> {cfg.label}
                                    </span>
                                  </td>
                                  <td className="p-2 text-xs text-muted-foreground">{item.message}</td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </div>
              )}
                </>
              )}
            </div>
          )}

          {/* Step 3: Review */}
          {step === 3 && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <p className="text-muted-foreground">Projeto</p>
                  <p className="font-medium">{projectName}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Área</p>
                  <p className="font-medium">{areaName}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Tipo de Fonte</p>
                  <Badge variant="outline">{sourceType}</Badge>
                </div>
                {sourceType !== "SUPABASE" && (
                  <div>
                    <p className="text-muted-foreground">Conexão</p>
                    <p className="font-mono text-xs">{connectionId?.slice(0, 12)}...</p>
                  </div>
                )}
              </div>

              {sourceType === "SUPABASE" ? (
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <Database className="h-5 w-5 text-primary" />
                    <p className="font-medium">
                      Tabelas Supabase: {supabaseTables.length} tabelas selecionadas
                    </p>
                  </div>
                  <div className="border rounded-lg max-h-[200px] overflow-auto">
                    <table className="w-full text-xs">
                      <tbody>
                        {supabaseTables.map((table, i) => (
                          <tr key={i} className="border-b">
                            <td className="p-2 text-muted-foreground w-8">{i + 1}</td>
                            <td className="p-2 font-mono">{table}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : mode === "single" ? (
                <div>
                  <p className="text-muted-foreground text-sm">Nome do Dataset</p>
                  <p className="font-bold text-lg">{datasetName}</p>
                </div>
              ) : (
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <FileSpreadsheet className="h-5 w-5 text-primary" />
                    <p className="font-medium">
                      Criação em massa: {bulkSummary?.valid ?? 0} datasets serão criados
                    </p>
                  </div>
                  <div className="flex items-center gap-3 flex-wrap text-sm">
                    <Badge className="bg-green-100 text-green-800 gap-1">
                      <CheckCircle className="h-3 w-3" /> {bulkSummary?.valid ?? 0} válidos
                    </Badge>
                    {(bulkSummary?.error ?? 0) > 0 && (
                      <Badge variant="destructive" className="gap-1">
                        {bulkSummary?.error} erros (ignorados)
                      </Badge>
                    )}
                    {(bulkSummary?.duplicate ?? 0) > 0 && (
                      <Badge className="bg-yellow-100 text-yellow-800 gap-1">
                        {bulkSummary?.duplicate} duplicados (ignorados)
                      </Badge>
                    )}
                    {(bulkSummary?.exists ?? 0) > 0 && (
                      <Badge className="bg-orange-100 text-orange-800 gap-1">
                        {bulkSummary?.exists} já existem (ignorados)
                      </Badge>
                    )}
                  </div>
                  <div className="border rounded-lg max-h-[200px] overflow-auto">
                    <table className="w-full text-xs">
                      <tbody>
                        {bulkItems.filter(it => it.status === "VALID").map((item, i) => (
                          <tr key={i} className="border-b">
                            <td className="p-2 text-muted-foreground w-8">{i + 1}</td>
                            <td className="p-2 font-mono">{item.dataset_name}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Nomenclatura — visível para todas as fontes de dados */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base flex items-center gap-2">
                    <Database className="h-5 w-5" />
                    Nomenclatura das Tabelas
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {mode === "single" && sourceType !== "SUPABASE" && (
                    <div className="flex items-center space-x-2">
                      <Checkbox 
                        id="custom-naming" 
                        checked={useCustomNaming}
                        onCheckedChange={(checked) => setUseCustomNaming(checked === true)}
                      />
                      <label
                        htmlFor="custom-naming"
                        className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer"
                      >
                        Customizar nomenclatura
                      </label>
                    </div>
                  )}

                  {!(mode === "single" && sourceType !== "SUPABASE" && useCustomNaming) ? (
                    <div className="space-y-4">
                      {/* Seletor de convenção */}
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <Label className="text-sm">Convenção de Nomenclatura</Label>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 text-xs"
                            onClick={() => setShowConventionManager(true)}
                            type="button"
                          >
                            <Settings className="h-3 w-3 mr-1" />
                            Gerenciar
                          </Button>
                        </div>
                        <Select 
                          value={selectedConvention?.toString() || ""} 
                          onValueChange={(v) => setSelectedConvention(parseInt(v))}
                        >
                          <SelectTrigger className="w-full">
                            <SelectValue placeholder="Selecione uma convenção" />
                          </SelectTrigger>
                          <SelectContent>
                            {namingConventions.map((nc) => (
                              <SelectItem key={nc.naming_version} value={nc.naming_version.toString()}>
                                <div className="flex items-center gap-2">
                                  <span>v{nc.naming_version}</span>
                                  {nc.is_active && (
                                    <Badge className="bg-green-600 text-white h-5 text-[10px] px-1.5">
                                      ATIVA
                                    </Badge>
                                  )}
                                  {nc.notes && <span className="text-xs text-muted-foreground">- {nc.notes}</span>}
                                </div>
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <p className="text-xs text-muted-foreground">
                          Escolha qual convenção usar para gerar os nomes das tabelas
                        </p>
                      </div>

                      {/* Preview — apenas para modo unitário */}
                      {mode === "single" && sourceType !== "SUPABASE" && (
                        namingPreviewLoading ? (
                          <div className="flex items-center gap-2 p-4 bg-muted/50 rounded-lg">
                            <Loader2 className="h-4 w-4 animate-spin" />
                            <span className="text-sm text-muted-foreground">Carregando preview...</span>
                          </div>
                        ) : namingPreviewError ? (
                          <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-lg text-sm text-yellow-800">
                            <p className="flex items-center gap-2">
                              <AlertTriangle className="h-4 w-4" />
                              {namingPreviewError}
                            </p>
                          </div>
                        ) : namingPreview ? (
                          <div className="space-y-3 p-4 bg-muted/50 rounded-lg text-sm">
                            <div>
                              <p className="text-xs text-muted-foreground font-medium mb-1">• Bronze</p>
                              <p className="font-mono text-xs">{namingPreview.bronze_table}</p>
                            </div>
                            <div>
                              <p className="text-xs text-muted-foreground font-medium mb-1">• Silver</p>
                              <p className="font-mono text-xs">{namingPreview.silver_table}</p>
                            </div>
                          </div>
                        ) : null
                      )}

                      {/* Info da convenção selecionada */}
                      {selectedConvention && (
                        <p className="text-xs text-muted-foreground italic">
                          ℹ️ Usando convenção v{selectedConvention}
                          {namingConventions.find((nc) => nc.naming_version === selectedConvention)?.notes && 
                            ` - ${namingConventions.find((nc) => nc.naming_version === selectedConvention)?.notes}`
                          }
                        </p>
                      )}
                    </div>
                  ) : (
                    // Customização (apenas para modo unitário)
                    <div className="space-y-4">
                      <div className="space-y-2">
                        <Label>Catálogo</Label>
                        <Input
                          value={customCatalog}
                          onChange={(e) => setCustomCatalog(e.target.value)}
                          placeholder="cm_dbx_dev"
                        />
                      </div>
                      
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-3">
                          <p className="text-sm font-medium">Bronze</p>
                          <div className="space-y-2">
                            <Label className="text-xs">Schema</Label>
                            <Input
                              value={customBronzeSchema}
                              onChange={(e) => setCustomBronzeSchema(e.target.value)}
                              placeholder="bronze_mega"
                              className="font-mono text-xs"
                            />
                          </div>
                          <div className="space-y-2">
                            <Label className="text-xs">Tabela</Label>
                            <Input
                              value={customBronzeTable}
                              onChange={(e) => setCustomBronzeTable(e.target.value)}
                              placeholder="CMASTER_GLO_AGENTES"
                              className="font-mono text-xs"
                            />
                          </div>
                        </div>
                        
                        <div className="space-y-3">
                          <p className="text-sm font-medium">Silver</p>
                          <div className="space-y-2">
                            <Label className="text-xs">Schema</Label>
                            <Input
                              value={customSilverSchema}
                              onChange={(e) => setCustomSilverSchema(e.target.value)}
                              placeholder="silver_mega"
                              className="font-mono text-xs"
                            />
                          </div>
                          <div className="space-y-2">
                            <Label className="text-xs">Tabela</Label>
                            <Input
                              value={customSilverTable}
                              onChange={(e) => setCustomSilverTable(e.target.value)}
                              placeholder="CMASTER_GLO_AGENTES"
                              className="font-mono text-xs"
                            />
                          </div>
                        </div>
                      </div>
                      
                      <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg text-xs space-y-1">
                        <p className="font-medium text-blue-900">Preview dos nomes finais:</p>
                        <p className="font-mono text-blue-800">
                          • Bronze: {customCatalog}.{customBronzeSchema}.{customBronzeTable}
                        </p>
                        <p className="font-mono text-blue-800">
                          • Silver: {customCatalog}.{customSilverSchema}.{customSilverTable}
                        </p>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              {error && (
                <p className="text-sm text-destructive bg-destructive/10 p-3 rounded-lg">{error}</p>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Navigation */}
      <div className="flex items-center justify-between">
        <Button variant="outline" onClick={() => step > 0 ? setStep(step - 1) : navigate("/datasets")}>
          <ArrowLeft className="h-4 w-4 mr-2" />
          {step === 0 ? "Cancelar" : "Voltar"}
        </Button>

        {step < STEPS.length - 1 ? (
          <Button onClick={() => setStep(step + 1)} disabled={!canNext()}>
            Próximo <ArrowRight className="h-4 w-4 ml-2" />
          </Button>
        ) : (
          <Button onClick={handleSubmit} disabled={submitting}>
            {submitting ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" /> Criando...
              </>
            ) : sourceType === "SUPABASE" ? (
              <>
                <Database className="h-4 w-4 mr-2" /> Criar {supabaseTables.length} Datasets
              </>
            ) : mode === "single" ? (
              <>
                <Database className="h-4 w-4 mr-2" /> Criar Dataset
              </>
            ) : (
              <>
                <Database className="h-4 w-4 mr-2" /> Criar {bulkSummary?.valid ?? 0} Datasets
              </>
            )}
          </Button>
        )}
      </div>
      
      {/* Naming Convention Manager Modal */}
      <NamingConventionManager
        open={showConventionManager}
        onOpenChange={setShowConventionManager}
        selectedVersion={selectedConvention}
        onSelect={(version) => setSelectedConvention(version)}
        onConventionChanged={loadNamingConventions}
      />
    </div>
  );
};

export default CreateDataset;
