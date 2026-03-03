import { useState, useEffect } from "react";
import {
  getNamingConventions,
  createNamingConvention,
  activateNamingConvention,
  deactivateNamingConvention,
  updateNamingConvention,
  deleteNamingConvention,
  getProjects,
  getAreas,
  createProject,
  createArea,
  updateProjectName,
  updateAreaName,
  type NamingConvention,
} from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Textarea } from "../components/ui/textarea";
import { Label } from "../components/ui/label";
import { Badge } from "../components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "../components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "../components/ui/dialog";
import { Settings as SettingsIcon, Plus, Edit, Check, AlertCircle, Trash2, FolderOpen, Loader2, ChevronRight, Pencil, X, Power } from "lucide-react";

export default function Settings() {
  const [conventions, setConventions] = useState<NamingConvention[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [editMode, setEditMode] = useState<{ version: number } | null>(null);
  const [deleteConfirmVersion, setDeleteConfirmVersion] = useState<number | null>(null);
  const [deleting, setDeleting] = useState(false);

  // Form state
  const [bronzePattern, setBronzePattern] = useState("");
  const [silverPattern, setSilverPattern] = useState("");
  const [notes, setNotes] = useState("");
  const [submitting, setSubmitting] = useState(false);

  // Projects & Areas state
  const [projects, setProjects] = useState<any[]>([]);
  const [areas, setAreas] = useState<any[]>([]);
  const [loadingProjects, setLoadingProjects] = useState(false);
  const [selectedProject, setSelectedProject] = useState<string | null>(null);
  const [loadingAreas, setLoadingAreas] = useState(false);
  // Create project dialog
  const [showCreateProjectDialog, setShowCreateProjectDialog] = useState(false);
  const [newProjectName, setNewProjectName] = useState("");
  const [newProjectDesc, setNewProjectDesc] = useState("");
  const [creatingProject, setCreatingProject] = useState(false);
  // Create area dialog
  const [showCreateAreaDialog, setShowCreateAreaDialog] = useState(false);
  const [newAreaName, setNewAreaName] = useState("");
  const [newAreaDesc, setNewAreaDesc] = useState("");
  const [creatingArea, setCreatingArea] = useState(false);
  // Inline editing
  const [editingProjectId, setEditingProjectId] = useState<string | null>(null);
  const [editProjectValue, setEditProjectValue] = useState("");
  const [editingAreaId, setEditingAreaId] = useState<string | null>(null);
  const [editAreaValue, setEditAreaValue] = useState("");
  const [savingEdit, setSavingEdit] = useState(false);

  useEffect(() => {
    loadConventions();
    loadProjects();
  }, []);

  useEffect(() => {
    if (selectedProject) {
      loadAreas(selectedProject);
    } else {
      setAreas([]);
    }
  }, [selectedProject]);

  const loadConventions = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getNamingConventions();
      setConventions(data.items);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    if (!bronzePattern.trim() || !silverPattern.trim()) {
      alert("Bronze e Silver patterns são obrigatórios");
      return;
    }

    try {
      setSubmitting(true);
      await createNamingConvention({
        bronze_pattern: bronzePattern.trim(),
        silver_pattern: silverPattern.trim(),
        notes: notes.trim(),
      });
      setShowCreateDialog(false);
      resetForm();
      await loadConventions();
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleUpdate = async () => {
    if (!editMode) return;

    try {
      setSubmitting(true);
      await updateNamingConvention(editMode.version, {
        bronze_pattern: bronzePattern.trim() || undefined,
        silver_pattern: silverPattern.trim() || undefined,
        notes: notes.trim() || undefined,
      });
      setEditMode(null);
      resetForm();
      await loadConventions();
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleActivate = async (version: number) => {
    if (!confirm(`Ativar naming convention v${version}? Isso desativará a convenção atual.`)) {
      return;
    }

    try {
      await activateNamingConvention(version);
      await loadConventions();
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    }
  };

  const handleDeactivate = async (version: number) => {
    if (!confirm(`Desativar naming convention v${version}?`)) {
      return;
    }

    try {
      await deactivateNamingConvention(version);
      await loadConventions();
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    }
  };

  const confirmDelete = async () => {
    if (deleteConfirmVersion === null) return;
    try {
      setDeleting(true);
      await deleteNamingConvention(deleteConfirmVersion);
      setDeleteConfirmVersion(null);
      await loadConventions();
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setDeleting(false);
    }
  };

  const resetForm = () => {
    setBronzePattern("");
    setSilverPattern("");
    setNotes("");
  };

  // ===== Projects & Areas =====
  const loadProjects = async () => {
    try {
      setLoadingProjects(true);
      const data = await getProjects();
      setProjects(data.items || []);
    } catch (e: any) {
      console.error("Erro ao carregar projetos:", e.message);
    } finally {
      setLoadingProjects(false);
    }
  };

  const loadAreas = async (projectId: string) => {
    try {
      setLoadingAreas(true);
      const data = await getAreas(projectId);
      setAreas(data.items || []);
    } catch (e: any) {
      console.error("Erro ao carregar áreas:", e.message);
    } finally {
      setLoadingAreas(false);
    }
  };

  const handleCreateProject = async () => {
    if (!newProjectName.trim()) return;
    try {
      setCreatingProject(true);
      const res = await createProject({ project_name: newProjectName.trim(), description: newProjectDesc.trim() || undefined });
      setProjects((prev) => [...prev, res.item]);
      setShowCreateProjectDialog(false);
      setNewProjectName("");
      setNewProjectDesc("");
      setSelectedProject(res.item.project_id);
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setCreatingProject(false);
    }
  };

  const handleCreateArea = async () => {
    if (!newAreaName.trim() || !selectedProject) return;
    try {
      setCreatingArea(true);
      const res = await createArea({ project_id: selectedProject, area_name: newAreaName.trim(), description: newAreaDesc.trim() || undefined });
      setAreas((prev) => [...prev, res.item]);
      setShowCreateAreaDialog(false);
      setNewAreaName("");
      setNewAreaDesc("");
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setCreatingArea(false);
    }
  };

  const handleSaveProjectName = async (projectId: string) => {
    if (!editProjectValue.trim()) return;
    try {
      setSavingEdit(true);
      await updateProjectName(projectId, editProjectValue.trim());
      setProjects((prev) => prev.map((p) => p.project_id === projectId ? { ...p, project_name: editProjectValue.trim() } : p));
      setEditingProjectId(null);
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setSavingEdit(false);
    }
  };

  const handleSaveAreaName = async (areaId: string) => {
    if (!editAreaValue.trim()) return;
    try {
      setSavingEdit(true);
      await updateAreaName(areaId, editAreaValue.trim());
      setAreas((prev) => prev.map((a) => a.area_id === areaId ? { ...a, area_name: editAreaValue.trim() } : a));
      setEditingAreaId(null);
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setSavingEdit(false);
    }
  };

  const openEditDialog = (conv: NamingConvention) => {
    setBronzePattern(conv.bronze_pattern);
    setSilverPattern(conv.silver_pattern);
    setNotes(conv.notes || "");
    setEditMode({ version: conv.naming_version });
  };

  // Preview do pattern com exemplo
  const getPreviewExample = (pattern: string, area = "mega", dataset = "SCHEMA_TABLE") => {
    return pattern.replace("{area}", area).replace("{dataset}", dataset);
  };

  return (
    <div className="p-6">
      <div className="mb-6 flex items-center gap-3">
        <SettingsIcon className="h-8 w-8 text-primary" />
        <div>
          <h1 className="text-3xl font-bold">Configurações</h1>
          <p className="text-muted-foreground">
            Gerencie parâmetros do sistema: projetos, áreas e convenções de nomenclatura
          </p>
        </div>
      </div>

      <Tabs defaultValue="projects" className="space-y-4">
        <TabsList>
          <TabsTrigger value="projects">
            <FolderOpen className="h-4 w-4 mr-2" />
            Projetos e Áreas
          </TabsTrigger>
          <TabsTrigger value="naming">
            <Edit className="h-4 w-4 mr-2" />
            Naming Conventions
          </TabsTrigger>
        </TabsList>

        {/* ===== TAB: Projetos e Áreas ===== */}
        <TabsContent value="projects">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Projetos */}
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg">Projetos</CardTitle>
                  <Button size="sm" onClick={() => setShowCreateProjectDialog(true)}>
                    <Plus className="mr-1 h-4 w-4" /> Novo Projeto
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {loadingProjects ? (
                  <div className="flex items-center justify-center py-8 text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin mr-2" /> Carregando...
                  </div>
                ) : projects.length === 0 ? (
                  <p className="text-center py-8 text-muted-foreground">Nenhum projeto cadastrado.</p>
                ) : (
                  <div className="space-y-1">
                    {projects.map((p) => (
                      <div
                        key={p.project_id}
                        className={`flex items-center justify-between p-3 rounded-lg cursor-pointer transition-colors ${
                          selectedProject === p.project_id ? "bg-primary/10 border border-primary/30" : "hover:bg-muted"
                        }`}
                        onClick={() => setSelectedProject(p.project_id)}
                      >
                        {editingProjectId === p.project_id ? (
                          <div className="flex gap-2 flex-1" onClick={(e) => e.stopPropagation()}>
                            <Input
                              value={editProjectValue}
                              onChange={(e) => setEditProjectValue(e.target.value)}
                              className="h-8 text-sm"
                              autoFocus
                              onKeyDown={(e) => {
                                if (e.key === "Enter") handleSaveProjectName(p.project_id);
                                if (e.key === "Escape") setEditingProjectId(null);
                              }}
                            />
                            <Button size="icon" variant="ghost" className="h-8 w-8" disabled={savingEdit} onClick={() => handleSaveProjectName(p.project_id)}>
                              {savingEdit ? <Loader2 className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3 text-green-600" />}
                            </Button>
                            <Button size="icon" variant="ghost" className="h-8 w-8" onClick={() => setEditingProjectId(null)}>
                              <X className="h-3 w-3 text-red-600" />
                            </Button>
                          </div>
                        ) : (
                          <>
                            <div className="flex items-center gap-2">
                              <ChevronRight className={`h-4 w-4 transition-transform ${selectedProject === p.project_id ? "rotate-90 text-primary" : "text-muted-foreground"}`} />
                              <div>
                                <p className="text-sm font-medium">{p.project_name}</p>
                                {p.description && <p className="text-xs text-muted-foreground">{p.description}</p>}
                              </div>
                              {!p.is_active && <Badge variant="outline" className="text-xs">inativo</Badge>}
                            </div>
                            <Button
                              size="icon"
                              variant="ghost"
                              className="h-7 w-7 opacity-0 group-hover:opacity-100"
                              onClick={(e) => {
                                e.stopPropagation();
                                setEditProjectValue(p.project_name);
                                setEditingProjectId(p.project_id);
                              }}
                            >
                              <Pencil className="h-3 w-3" />
                            </Button>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Áreas do projeto selecionado */}
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg">
                    {selectedProject
                      ? `Áreas — ${projects.find((p) => p.project_id === selectedProject)?.project_name || ""}`
                      : "Áreas"}
                  </CardTitle>
                  {selectedProject && (
                    <Button size="sm" onClick={() => setShowCreateAreaDialog(true)}>
                      <Plus className="mr-1 h-4 w-4" /> Nova Área
                    </Button>
                  )}
                </div>
              </CardHeader>
              <CardContent>
                {!selectedProject ? (
                  <p className="text-center py-8 text-muted-foreground">Selecione um projeto para ver suas áreas.</p>
                ) : loadingAreas ? (
                  <div className="flex items-center justify-center py-8 text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin mr-2" /> Carregando...
                  </div>
                ) : areas.length === 0 ? (
                  <p className="text-center py-8 text-muted-foreground">Nenhuma área cadastrada neste projeto.</p>
                ) : (
                  <div className="space-y-1">
                    {areas.map((a) => (
                      <div key={a.area_id} className="flex items-center justify-between p-3 rounded-lg hover:bg-muted">
                        {editingAreaId === a.area_id ? (
                          <div className="flex gap-2 flex-1">
                            <Input
                              value={editAreaValue}
                              onChange={(e) => setEditAreaValue(e.target.value)}
                              className="h-8 text-sm"
                              autoFocus
                              onKeyDown={(e) => {
                                if (e.key === "Enter") handleSaveAreaName(a.area_id);
                                if (e.key === "Escape") setEditingAreaId(null);
                              }}
                            />
                            <Button size="icon" variant="ghost" className="h-8 w-8" disabled={savingEdit} onClick={() => handleSaveAreaName(a.area_id)}>
                              {savingEdit ? <Loader2 className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3 text-green-600" />}
                            </Button>
                            <Button size="icon" variant="ghost" className="h-8 w-8" onClick={() => setEditingAreaId(null)}>
                              <X className="h-3 w-3 text-red-600" />
                            </Button>
                          </div>
                        ) : (
                          <>
                            <div>
                              <p className="text-sm font-medium">{a.area_name}</p>
                              {a.description && <p className="text-xs text-muted-foreground">{a.description}</p>}
                            </div>
                            <Button
                              size="icon"
                              variant="ghost"
                              className="h-7 w-7"
                              onClick={() => {
                                setEditAreaValue(a.area_name);
                                setEditingAreaId(a.area_id);
                              }}
                            >
                              <Pencil className="h-3 w-3" />
                            </Button>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* ===== TAB: Naming Conventions ===== */}
        <TabsContent value="naming">
          <div className="flex items-center justify-between mb-4">
            <p className="text-sm text-muted-foreground">Gerencie convenções de nomenclatura para tabelas Bronze e Silver</p>
            <Button onClick={() => setShowCreateDialog(true)}>
              <Plus className="mr-2 h-4 w-4" />
              Nova Convenção
            </Button>
          </div>

          {loading && (
            <div className="text-center py-8 text-muted-foreground">
              Carregando convenções...
            </div>
          )}

          {error && (
            <div className="bg-destructive/10 border border-destructive rounded-lg p-4 mb-4 flex items-center gap-2">
              <AlertCircle className="h-5 w-5 text-destructive" />
              <span className="text-destructive">{error}</span>
            </div>
          )}

          {!loading && !error && conventions.length === 0 && (
            <Card>
              <CardContent className="py-12 text-center text-muted-foreground">
                Nenhuma convenção de nomenclatura cadastrada.
              </CardContent>
            </Card>
          )}

          <div className="grid gap-4">
            {conventions.map((conv) => (
              <Card key={conv.naming_version} className={conv.is_active ? "border-primary border-2" : ""}>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="flex items-center gap-2">
                        <CardTitle>Versão {conv.naming_version}</CardTitle>
                        {conv.is_active && (
                          <Badge variant="default" className="bg-green-600">
                            <Check className="mr-1 h-3 w-3" />
                            PADRÃO
                          </Badge>
                        )}
                      </div>
                      {conv.notes && (
                        <p className="text-sm text-muted-foreground mt-1">{conv.notes}</p>
                      )}
                    </div>
                    <div className="flex gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => openEditDialog(conv)}
                      >
                        <Edit className="mr-1 h-3 w-3" />
                        Editar
                      </Button>
                      {conv.is_active ? (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleDeactivate(conv.naming_version)}
                          className="text-orange-600 hover:text-orange-700 hover:bg-orange-50"
                        >
                          <Power className="mr-1 h-3 w-3" />
                          Desativar
                        </Button>
                      ) : (
                        <Button
                          variant="default"
                          size="sm"
                          onClick={() => handleActivate(conv.naming_version)}
                        >
                          <Check className="mr-1 h-3 w-3" />
                          Definir como Padrão
                        </Button>
                      )}
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setDeleteConfirmVersion(conv.naming_version)}
                        className="text-red-600 hover:text-red-700 hover:bg-red-50"
                      >
                        <Trash2 className="mr-1 h-3 w-3" />
                        Excluir
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <Label className="text-xs text-muted-foreground">Bronze Pattern</Label>
                      <code className="block mt-1 px-3 py-2 bg-muted rounded text-sm">
                        {conv.bronze_pattern}
                      </code>
                      <p className="text-xs text-muted-foreground mt-1">
                        Exemplo: {getPreviewExample(conv.bronze_pattern)}
                      </p>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">Silver Pattern</Label>
                      <code className="block mt-1 px-3 py-2 bg-muted rounded text-sm">
                        {conv.silver_pattern}
                      </code>
                      <p className="text-xs text-muted-foreground mt-1">
                        Exemplo: {getPreviewExample(conv.silver_pattern)}
                      </p>
                    </div>
                    <div className="text-xs text-muted-foreground pt-2 border-t">
                      Criado por {conv.created_by} em{" "}
                      {new Date(conv.created_at).toLocaleString("pt-BR")}
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>
      </Tabs>

      {/* Create/Edit Naming Convention Dialog */}
      <Dialog
        open={showCreateDialog || editMode !== null}
        onOpenChange={(open) => {
          if (!open) {
            setShowCreateDialog(false);
            setEditMode(null);
            resetForm();
          }
        }}
      >
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>
              {editMode ? `Editar Convenção v${editMode.version}` : "Nova Convenção de Nomenclatura"}
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-4">
            <div>
              <Label htmlFor="bronze-pattern">
                Bronze Pattern <span className="text-destructive">*</span>
              </Label>
              <Input
                id="bronze-pattern"
                value={bronzePattern}
                onChange={(e) => setBronzePattern(e.target.value)}
                placeholder="bronze_{area}.{dataset}"
                className="font-mono"
              />
              <p className="text-xs text-muted-foreground mt-1">
                Use {"{area}"} e {"{dataset}"} como placeholders
              </p>
              {bronzePattern && (
                <p className="text-xs mt-1">
                  <span className="text-muted-foreground">Preview: </span>
                  <code className="text-primary">{getPreviewExample(bronzePattern)}</code>
                </p>
              )}
            </div>

            <div>
              <Label htmlFor="silver-pattern">
                Silver Pattern <span className="text-destructive">*</span>
              </Label>
              <Input
                id="silver-pattern"
                value={silverPattern}
                onChange={(e) => setSilverPattern(e.target.value)}
                placeholder="silver_{area}.{dataset}"
                className="font-mono"
              />
              <p className="text-xs text-muted-foreground mt-1">
                Use {"{area}"} e {"{dataset}"} como placeholders
              </p>
              {silverPattern && (
                <p className="text-xs mt-1">
                  <span className="text-muted-foreground">Preview: </span>
                  <code className="text-primary">{getPreviewExample(silverPattern)}</code>
                </p>
              )}
            </div>

            <div>
              <Label htmlFor="notes">Notas (opcional)</Label>
              <Textarea
                id="notes"
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                placeholder="Descrição ou comentários sobre esta convenção"
                rows={3}
              />
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setShowCreateDialog(false);
                setEditMode(null);
                resetForm();
              }}
              disabled={submitting}
            >
              Cancelar
            </Button>
            <Button
              onClick={editMode ? handleUpdate : handleCreate}
              disabled={submitting || !bronzePattern.trim() || !silverPattern.trim()}
            >
              {submitting ? "Salvando..." : editMode ? "Atualizar" : "Criar"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      {/* Delete Confirmation Dialog */}
      <Dialog
        open={deleteConfirmVersion !== null}
        onOpenChange={(open) => { if (!open) setDeleteConfirmVersion(null); }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Confirmar Exclusão</DialogTitle>
          </DialogHeader>
          <div className="py-4">
            <p className="text-sm text-muted-foreground">
              Tem certeza que deseja excluir a convenção v{deleteConfirmVersion}?
              Esta ação não pode ser desfeita.
            </p>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setDeleteConfirmVersion(null)}
              disabled={deleting}
            >
              Cancelar
            </Button>
            <Button
              onClick={confirmDelete}
              disabled={deleting}
              className="bg-red-600 hover:bg-red-700"
            >
              {deleting ? "Excluindo..." : "Excluir"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Create Project Dialog */}
      <Dialog open={showCreateProjectDialog} onOpenChange={(open) => { if (!open) { setShowCreateProjectDialog(false); setNewProjectName(""); setNewProjectDesc(""); } }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Novo Projeto</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="settings-project-name">Nome do Projeto <span className="text-destructive">*</span></Label>
              <Input
                id="settings-project-name"
                value={newProjectName}
                onChange={(e) => setNewProjectName(e.target.value)}
                placeholder="Ex: CRM, ERP, Data Lake"
                autoFocus
                onKeyDown={(e) => { if (e.key === "Enter" && newProjectName.trim()) handleCreateProject(); }}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="settings-project-desc">Descrição (opcional)</Label>
              <Textarea
                id="settings-project-desc"
                value={newProjectDesc}
                onChange={(e) => setNewProjectDesc(e.target.value)}
                placeholder="Breve descrição do projeto"
                rows={2}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => { setShowCreateProjectDialog(false); setNewProjectName(""); setNewProjectDesc(""); }} disabled={creatingProject}>
              Cancelar
            </Button>
            <Button onClick={handleCreateProject} disabled={creatingProject || !newProjectName.trim()}>
              {creatingProject ? "Criando..." : "Criar Projeto"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Create Area Dialog */}
      <Dialog open={showCreateAreaDialog} onOpenChange={(open) => { if (!open) { setShowCreateAreaDialog(false); setNewAreaName(""); setNewAreaDesc(""); } }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Nova Área</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label className="text-sm text-muted-foreground">Projeto</Label>
              <p className="text-sm font-medium">{projects.find((p) => p.project_id === selectedProject)?.project_name || selectedProject}</p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="settings-area-name">Nome da Área <span className="text-destructive">*</span></Label>
              <Input
                id="settings-area-name"
                value={newAreaName}
                onChange={(e) => setNewAreaName(e.target.value)}
                placeholder="Ex: mega, financeiro, rh"
                autoFocus
                onKeyDown={(e) => { if (e.key === "Enter" && newAreaName.trim()) handleCreateArea(); }}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="settings-area-desc">Descrição (opcional)</Label>
              <Textarea
                id="settings-area-desc"
                value={newAreaDesc}
                onChange={(e) => setNewAreaDesc(e.target.value)}
                placeholder="Breve descrição da área"
                rows={2}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => { setShowCreateAreaDialog(false); setNewAreaName(""); setNewAreaDesc(""); }} disabled={creatingArea}>
              Cancelar
            </Button>
            <Button onClick={handleCreateArea} disabled={creatingArea || !newAreaName.trim()}>
              {creatingArea ? "Criando..." : "Criar Área"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
