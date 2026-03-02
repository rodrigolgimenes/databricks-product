import { useState, useEffect } from "react";
import {
  getNamingConventions,
  createNamingConvention,
  activateNamingConvention,
  updateNamingConvention,
  deleteNamingConvention,
  type NamingConvention,
} from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Textarea } from "../components/ui/textarea";
import { Label } from "../components/ui/label";
import { Badge } from "../components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "../components/ui/dialog";
import { Settings as SettingsIcon, Plus, Edit, Check, AlertCircle, Trash2 } from "lucide-react";

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

  useEffect(() => {
    loadConventions();
  }, []);

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
      <div className="mb-6 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <SettingsIcon className="h-8 w-8 text-primary" />
          <div>
            <h1 className="text-3xl font-bold">Configurações</h1>
            <p className="text-muted-foreground">
              Gerencie convenções de nomenclatura para tabelas Bronze e Silver
            </p>
          </div>
        </div>
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
                    <Badge variant="outline" className="h-8 px-3 text-xs text-muted-foreground border-dashed">
                      Convenção padrão
                    </Badge>
                  ) : (
                    <>
                      <Button
                        variant="default"
                        size="sm"
                        onClick={() => handleActivate(conv.naming_version)}
                      >
                        <Check className="mr-1 h-3 w-3" />
                        Definir como Padrão
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setDeleteConfirmVersion(conv.naming_version)}
                        className="text-red-600 hover:text-red-700 hover:bg-red-50"
                      >
                        <Trash2 className="mr-1 h-3 w-3" />
                        Excluir
                      </Button>
                    </>
                  )}
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

      {/* Create/Edit Dialog */}
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
    </div>
  );
}
