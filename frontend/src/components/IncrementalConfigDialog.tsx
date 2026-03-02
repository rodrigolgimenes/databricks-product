import { useState, useEffect } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Loader2, AlertCircle, Info, Sparkles, CheckCircle2, Key, ShieldCheck, ShieldAlert, ShieldX } from "lucide-react";
import { toast } from "sonner";
import * as api from "@/lib/api";

interface IncrementalConfigDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  datasetId: string;
  datasetName: string;
  currentConfig?: {
    enable_incremental?: boolean;
    incremental_strategy?: string;
    bronze_mode?: string;
    override_watermark_value?: string | null;
    incremental_metadata?: string | null;
  };
  onConfigUpdated?: () => void;
}

export function IncrementalConfigDialog({
  open,
  onOpenChange,
  datasetId,
  datasetName,
  currentConfig,
  onConfigUpdated,
}: IncrementalConfigDialogProps) {
  const [loading, setLoading] = useState(false);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [enableIncremental, setEnableIncremental] = useState(currentConfig?.enable_incremental || false);
  const [bronzeMode, setBronzeMode] = useState<string>(currentConfig?.bronze_mode || "CURRENT");
  const [incrementalStrategy, setIncrementalStrategy] = useState<string>(currentConfig?.incremental_strategy || "");
  const [overrideWatermark, setOverrideWatermark] = useState<string>(currentConfig?.override_watermark_value || "");
  const [lookbackDays, setLookbackDays] = useState<string>("3");
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  const [columnsPreview, setColumnsPreview] = useState<any>(null);
  const [pkColumns, setPkColumns] = useState<string[]>([]);
  const [pkSource, setPkSource] = useState<string>("");
  const [pkConfidence, setPkConfidence] = useState<number>(0);
  const [validatingPk, setValidatingPk] = useState(false);
  const [pkValidation, setPkValidation] = useState<{
    unique: boolean;
    total_rows: number;
    distinct_rows: number;
    duplicate_count: number;
    sample_duplicates: Array<Record<string, any>>;
  } | null>(null);

  useEffect(() => {
    if (currentConfig) {
      setEnableIncremental(currentConfig.enable_incremental || false);
      setBronzeMode(currentConfig.bronze_mode || "CURRENT");
      setIncrementalStrategy(currentConfig.incremental_strategy || "");
      setOverrideWatermark(currentConfig.override_watermark_value || "");
      
      // Parse lookback days, watermark_column, pk, pk_source, pk_confidence from metadata
      try {
        const metadata = currentConfig.incremental_metadata ? JSON.parse(currentConfig.incremental_metadata) : {};
        setLookbackDays(String(metadata.lookback_days || 3));
        setWatermarkColumn(metadata.watermark_column || "");
        setPkColumns(metadata.pk || []);
        setPkSource(metadata.pk_source || "");
        setPkConfidence(metadata.pk_confidence || 0);
      } catch {
        setLookbackDays("3");
        setWatermarkColumn("");
        setPkColumns([]);
        setPkSource("");
        setPkConfidence(0);
      }
    }
  }, [currentConfig, open]);

  // Load columns preview when dialog opens
  useEffect(() => {
    if (open && datasetId) {
      setLoadingPreview(true);
      api.getDatasetColumnsPreview(datasetId)
        .then((data) => {
          setColumnsPreview(data);
          // If no watermark column is set, use the suggested one
          if (!watermarkColumn && data.suggested_watermark_column) {
            setWatermarkColumn(data.suggested_watermark_column);
          }
          // If no PK columns are set, use the suggested ones
          if (pkColumns.length === 0 && data.suggested_pk_columns?.length > 0) {
            setPkColumns(data.suggested_pk_columns);
          }
        })
        .catch((err) => {
          console.error("Erro ao carregar preview de colunas:", err);
          toast.error("Erro ao carregar colunas da tabela");
        })
        .finally(() => setLoadingPreview(false));
    }
  }, [open, datasetId]);

  const handleValidatePk = async () => {
    if (pkColumns.length === 0) {
      toast.error("Selecione ao menos uma coluna de PK");
      return;
    }
    setValidatingPk(true);
    setPkValidation(null);
    try {
      const result = await api.validatePk(datasetId, pkColumns, "bronze");
      setPkValidation({
        unique: result.unique,
        total_rows: result.total_rows,
        distinct_rows: result.distinct_rows,
        duplicate_count: result.duplicate_count,
        sample_duplicates: result.sample_duplicates || [],
      });
      if (result.unique) {
        toast.success(`PK única confirmada! (${result.total_rows.toLocaleString()} rows)`);
      } else {
        toast.warning(`PK não é única: ${result.duplicate_count.toLocaleString()} duplicatas encontradas`);
      }
    } catch (e: any) {
      console.error("Erro ao validar PK:", e);
      toast.error(e.message || "Erro ao validar unicidade da PK");
    } finally {
      setValidatingPk(false);
    }
  };

  const handleSave = async () => {
    setLoading(true);
    try {
      // Parse existing metadata
      let metadata: any = {};
      try {
        metadata = currentConfig?.incremental_metadata ? JSON.parse(currentConfig.incremental_metadata) : {};
      } catch {
        metadata = {};
      }

      // Update lookback days and watermark column in metadata
      const lookbackDaysNum = parseInt(lookbackDays, 10);
      if (!isNaN(lookbackDaysNum) && lookbackDaysNum > 0) {
        metadata.lookback_days = lookbackDaysNum;
      }
      if (watermarkColumn.trim()) {
        metadata.watermark_column = watermarkColumn.trim();
      }
      if (pkColumns.length > 0) {
        metadata.pk = pkColumns;
      } else {
        delete metadata.pk;
      }

      // Call API to update
      const payload: any = {
        enable_incremental: enableIncremental,
        bronze_mode: bronzeMode,
        incremental_metadata: metadata,
      };

      // Include strategy if changed
      if (incrementalStrategy && incrementalStrategy !== currentConfig?.incremental_strategy) {
        payload.incremental_strategy = incrementalStrategy;
      }

      // Only include watermark override if it's not empty
      if (overrideWatermark.trim()) {
        payload.override_watermark_value = overrideWatermark.trim();
      } else {
        // Clear override if empty
        payload.override_watermark_value = null;
      }

      await fetch(`/api/portal/datasets/${datasetId}/incremental-config`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).then(async (res) => {
        const data = await res.json();
        if (!res.ok) throw new Error(data?.message || `HTTP ${res.status}`);
        return data;
      });

      toast.success("Configurações atualizadas com sucesso!");
      onConfigUpdated?.();
      onOpenChange(false);
    } catch (e: any) {
      console.error("Erro ao atualizar configurações:", e);
      toast.error(e.message || "Erro ao atualizar configurações");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Configurações de Carga Incremental</DialogTitle>
          <DialogDescription>
            Dataset: <span className="font-mono font-medium">{datasetName}</span>
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5 py-4 overflow-y-auto flex-1">
          {/* Enable Incremental */}
          <div className="flex items-center justify-between space-x-2 p-3 bg-muted/30 rounded-lg">
            <div className="space-y-0.5">
              <Label htmlFor="enable-incremental" className="text-base font-medium">
                Habilitar Carga Incremental
              </Label>
              <p className="text-sm text-muted-foreground">
                Se desabilitado, sempre fará carga completa (FULL)
              </p>
            </div>
            <Switch
              id="enable-incremental"
              checked={enableIncremental}
              onCheckedChange={setEnableIncremental}
            />
          </div>

          {/* Bronze Mode */}
          <div className="space-y-2">
            <Label htmlFor="bronze-mode" className="font-medium">
              Modo de Escrita (Bronze)
            </Label>
            <Select value={bronzeMode} onValueChange={setBronzeMode}>
              <SelectTrigger id="bronze-mode">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="SNAPSHOT">
                  <div className="flex flex-col gap-1 py-1">
                    <div className="font-medium">SNAPSHOT - Sobrescreve tudo (OVERWRITE)</div>
                    <div className="text-xs text-muted-foreground">
                      Apaga todos os dados existentes e reinsere do zero. Sem histórico.
                    </div>
                  </div>
                </SelectItem>
                <SelectItem value="CURRENT">
                  <div className="flex flex-col gap-1 py-1">
                    <div className="font-medium">CURRENT - Merge incremental (UPSERT)</div>
                    <div className="text-xs text-muted-foreground">
                      Atualiza registros existentes e insere novos. Mantém estado atual.
                    </div>
                  </div>
                </SelectItem>
                <SelectItem value="APPEND_LOG">
                  <div className="flex flex-col gap-1 py-1">
                    <div className="font-medium">APPEND_LOG - Apenas append (histórico completo)</div>
                    <div className="text-xs text-muted-foreground">
                      Nunca atualiza, apenas insere. Mantém todas as versões históricas.
                    </div>
                  </div>
                </SelectItem>
              </SelectContent>
            </Select>
            
            {/* Detailed explanation based on selected mode */}
            <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg text-xs space-y-2">
              {bronzeMode === "SNAPSHOT" && (
                <>
                  <p className="font-medium text-slate-900">📸 SNAPSHOT (Sobrescrita Completa)</p>
                  <div className="space-y-1 text-slate-700">
                    <p>• <strong>INSERT OVERWRITE</strong>: Apaga toda a tabela Bronze e reinsere</p>
                    <p>• <strong>Deletions</strong>: Automáticas (dados deletados na origem somem)</p>
                    <p>• <strong>Histórico</strong>: ❌ Não mantém versões antigas</p>
                    <p>• <strong>Uso ideal</strong>: Tabelas pequenas, dimensões, carga FULL</p>
                  </div>
                </>
              )}
              {bronzeMode === "CURRENT" && (
                <>
                  <p className="font-medium text-slate-900">🔄 CURRENT (Merge/Upsert)</p>
                  <div className="space-y-1 text-slate-700">
                    <p>• <strong>MERGE (UPSERT)</strong>: UPDATE em registros existentes + INSERT em novos</p>
                    <p>• <strong>Deletions</strong>: Requer flag na origem (ex: IS_DELETED) ou soft delete</p>
                    <p>• <strong>Histórico</strong>: ⚠️ Mantém apenas estado atual (última versão)</p>
                    <p>• <strong>Uso ideal</strong>: Tabelas transacionais, CDC, estado atual dos dados</p>
                    <p>• <strong>Requer</strong>: Chave primária na tabela para identificar registros</p>
                  </div>
                </>
              )}
              {bronzeMode === "APPEND_LOG" && (
                <>
                  <p className="font-medium text-slate-900">📝 APPEND_LOG (Append-Only)</p>
                  <div className="space-y-1 text-slate-700">
                    <p>• <strong>INSERT apenas</strong>: Nunca faz UPDATE, sempre adiciona nova linha</p>
                    <p>• <strong>Deletions</strong>: Aparecem como novo registro com operação DELETE</p>
                    <p>• <strong>Histórico</strong>: ✅ Mantém TODAS as versões (audit trail completo)</p>
                    <p>• <strong>Uso ideal</strong>: Logs, eventos, auditoria, CDC com histórico</p>
                    <p>• <strong>Atenção</strong>: Tabela cresce continuamente (precisa compactação)</p>
                  </div>
                </>
              )}
            </div>
          </div>

          {/* Watermark Column Selector */}
          {enableIncremental && (
            <div className="space-y-2">
              <Label htmlFor="watermark-column" className="font-medium">
                Coluna de Data Delta (Watermark)
              </Label>
              {loadingPreview ? (
                <div className="flex items-center gap-2 p-3 border rounded-lg bg-muted/30">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span className="text-sm text-muted-foreground">Carregando colunas...</span>
                </div>
              ) : columnsPreview ? (
                <>
                  <Select value={watermarkColumn} onValueChange={setWatermarkColumn}>
                    <SelectTrigger id="watermark-column">
                      <SelectValue placeholder="Selecione a coluna de data" />
                    </SelectTrigger>
                    <SelectContent className="max-h-[300px]">
                      {columnsPreview.columns
                        ?.filter((c: any) => c.is_date)
                        .map((col: any) => (
                          <SelectItem key={col.name} value={col.name}>
                            <div className="flex items-center gap-2">
                              {col.is_suggested && (
                                <Sparkles className="h-3.5 w-3.5 text-amber-500" />
                              )}
                              <span className="font-mono text-sm">{col.name}</span>
                              <span className="text-xs text-muted-foreground">({col.type})</span>
                            </div>
                          </SelectItem>
                        ))}
                    </SelectContent>
                  </Select>
              {columnsPreview.preview_warning && (
                    <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs">
                      <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0 text-amber-600" />
                      <p className="text-amber-800">{columnsPreview.preview_warning}</p>
                    </div>
                  )}
              {columnsPreview.suggested_watermark_column && (
                    <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs">
                      <Sparkles className="h-3.5 w-3.5 mt-0.5 flex-shrink-0 text-amber-600" />
                      <div>
                        <p className="font-medium text-amber-900">
                          Sugestão Inteligente: <span className="font-mono">{columnsPreview.suggested_watermark_column}</span>
                        </p>
                        <p className="text-amber-700 mt-0.5">
                          {columnsPreview.suggestion_reason === 'already_configured' && 'Coluna já configurada anteriormente'}
                          {columnsPreview.suggestion_reason === 'name_pattern' && 'Detectado padrão comum de nome (updated_at, modified_at, etc)'}
                          {columnsPreview.suggestion_reason === 'partial_match' && 'Nome contém palavras relacionadas a atualização'}
                          {columnsPreview.suggestion_reason === 'first_date_column' && 'Primeira coluna de data/timestamp encontrada'}
                        </p>
                      </div>
                    </div>
                  )}
                  {watermarkColumn && (
                    <div className="p-3 bg-blue-50 border border-blue-200 rounded">
                      <p className="text-sm font-medium text-blue-900 mb-2">
                        Preview: <span className="font-mono">{watermarkColumn}</span>
                      </p>
                      {(() => {
                        const col = columnsPreview.columns?.find((c: any) => c.name === watermarkColumn);
                        if (col?.sample_values?.length > 0) {
                          return (
                            <div className="space-y-1">
                              {col.sample_values.map((val: any, idx: number) => (
                                <div key={idx} className="text-xs font-mono text-blue-700 bg-blue-100 px-2 py-1 rounded">
                                  {String(val)}
                                </div>
                              ))}
                            </div>
                          );
                        }
                        return <p className="text-xs text-blue-600">Sem dados de amostra disponíveis</p>;
                      })()}
                    </div>
                  )}
                </>
              ) : (
                <Input
                  id="watermark-column"
                  type="text"
                  value={watermarkColumn}
                  onChange={(e) => setWatermarkColumn(e.target.value)}
                  placeholder="Ex: updated_at, modified_at"
                />
              )}
              <div className="flex items-start gap-2 text-xs text-muted-foreground">
                <Info className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                <p>
                  Coluna usada para identificar registros novos/modificados na carga incremental.
                  Deve ser uma coluna de data/timestamp que é atualizada quando o registro muda.
                </p>
              </div>
            </div>
          )}

          {/* PK Column Selector — visible when CURRENT mode */}
          {enableIncremental && bronzeMode === "CURRENT" && (
            <div className="space-y-2">
              <Label className="font-medium flex items-center gap-2">
                <Key className="h-4 w-4" />
                Colunas de Chave Primária (PK)
              </Label>

              {/* PK Confidence Badge */}
              {pkColumns.length > 0 && (
                <div className={`flex items-center gap-2 p-2.5 rounded-lg border text-sm ${
                  pkSource === "DECLARED_CONSTRAINT"
                    ? "bg-green-50 border-green-200 text-green-800"
                    : pkSource === "CANDIDATE_DISCOVERY" && pkConfidence >= 0.90
                      ? "bg-amber-50 border-amber-200 text-amber-800"
                      : pkSource === "CANDIDATE_DISCOVERY"
                        ? "bg-red-50 border-red-200 text-red-800"
                        : "bg-slate-50 border-slate-200 text-slate-700"
                }`}>
                  {pkSource === "DECLARED_CONSTRAINT" ? (
                    <ShieldCheck className="h-4 w-4 text-green-600 flex-shrink-0" />
                  ) : pkSource === "CANDIDATE_DISCOVERY" && pkConfidence >= 0.90 ? (
                    <ShieldAlert className="h-4 w-4 text-amber-600 flex-shrink-0" />
                  ) : pkSource === "CANDIDATE_DISCOVERY" ? (
                    <ShieldX className="h-4 w-4 text-red-600 flex-shrink-0" />
                  ) : (
                    <Key className="h-4 w-4 text-slate-500 flex-shrink-0" />
                  )}
                  <span className="font-medium">
                    {pkSource === "DECLARED_CONSTRAINT"
                      ? "PK: Declarada (Constraint)"
                      : pkSource === "CANDIDATE_DISCOVERY"
                        ? `PK: Candidata (${(pkConfidence * 100).toFixed(0)}%)`
                        : "PK: Manual"}
                  </span>
                  {pkSource === "DECLARED_CONSTRAINT" && (
                    <Badge variant="outline" className="bg-green-100 text-green-800 border-green-300 text-xs ml-auto">100%</Badge>
                  )}
                  {pkSource === "CANDIDATE_DISCOVERY" && (
                    <Badge variant="outline" className={`text-xs ml-auto ${
                      pkConfidence >= 0.90
                        ? "bg-amber-100 text-amber-800 border-amber-300"
                        : "bg-red-100 text-red-800 border-red-300"
                    }`}>{(pkConfidence * 100).toFixed(0)}%</Badge>
                  )}
                </div>
              )}
              {pkColumns.length === 0 && enableIncremental && bronzeMode === "CURRENT" && (
                <div className="flex items-center gap-2 p-2.5 rounded-lg border bg-red-50 border-red-200 text-red-800 text-sm">
                  <ShieldX className="h-4 w-4 text-red-600 flex-shrink-0" />
                  <span className="font-medium">PK: Não definida</span>
                  <span className="text-xs text-red-600 ml-auto">Obrigatória para CURRENT</span>
                </div>
              )}
              {loadingPreview ? (
                <div className="flex items-center gap-2 p-3 border rounded-lg bg-muted/30">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span className="text-sm text-muted-foreground">Carregando colunas...</span>
                </div>
              ) : columnsPreview ? (
                <>
                  <div className="border rounded-lg max-h-[200px] overflow-y-auto">
                    {columnsPreview.columns
                      ?.filter((c: any) => !c.name.startsWith('_'))  // Filter technical columns
                      .map((col: any) => {
                        const isSelected = pkColumns.includes(col.name);
                        const isSuggested = col.is_pk;
                        return (
                          <label
                            key={col.name}
                            className={`flex items-center gap-3 px-3 py-2 cursor-pointer hover:bg-muted/50 border-b last:border-b-0 transition-colors ${
                              isSelected ? 'bg-blue-50' : ''
                            }`}
                          >
                            <input
                              type="checkbox"
                              checked={isSelected}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  setPkColumns([...pkColumns, col.name]);
                                } else {
                                  setPkColumns(pkColumns.filter(pk => pk !== col.name));
                                }
                              }}
                              className="rounded border-gray-300"
                            />
                            <div className="flex items-center gap-2 flex-1 min-w-0">
                              {isSuggested && (
                                <Sparkles className="h-3.5 w-3.5 text-amber-500 flex-shrink-0" />
                              )}
                              <span className="font-mono text-sm truncate">{col.name}</span>
                              <span className="text-xs text-muted-foreground flex-shrink-0">({col.type})</span>
                            </div>
                            {isSelected && (
                              <Badge variant="secondary" className="text-xs flex-shrink-0">PK</Badge>
                            )}
                          </label>
                        );
                      })}
                  </div>
                  {pkColumns.length > 0 && (
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-xs text-muted-foreground">Selecionadas:</span>
                      {pkColumns.map(pk => (
                        <Badge key={pk} variant="outline" className="font-mono text-xs">
                          {pk}
                          <button
                            type="button"
                            className="ml-1 hover:text-destructive"
                            onClick={() => setPkColumns(pkColumns.filter(p => p !== pk))}
                          >
                            ×
                          </button>
                        </Badge>
                      ))}
                    </div>
                  )}
                  {columnsPreview.suggested_pk_columns?.length > 0 && (
                    <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs">
                      <Sparkles className="h-3.5 w-3.5 mt-0.5 flex-shrink-0 text-amber-600" />
                      <div>
                        <p className="font-medium text-amber-900">
                          Sugestão Inteligente: <span className="font-mono">{columnsPreview.suggested_pk_columns.join(', ')}</span>
                        </p>
                        <p className="text-amber-700 mt-0.5">
                          {columnsPreview.pk_suggestion_reason === 'already_configured' && 'PK já configurada anteriormente'}
                          {columnsPreview.pk_suggestion_reason === 'source_detected' && 'Detectada automaticamente da tabela de origem (PostgreSQL)'}
                          {columnsPreview.pk_suggestion_reason === 'id_column_fallback' && 'Coluna "id" encontrada como provável chave primária'}
                        </p>
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <Input
                  type="text"
                  value={pkColumns.join(', ')}
                  onChange={(e) => setPkColumns(e.target.value.split(',').map(s => s.trim()).filter(Boolean))}
                  placeholder="Ex: id, codigo"
                />
              )}
              {/* Validate PK Button + Results */}
              {pkColumns.length > 0 && (
                <div className="space-y-2">
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={handleValidatePk}
                    disabled={validatingPk || pkColumns.length === 0}
                    className="w-full"
                  >
                    {validatingPk ? (
                      <><Loader2 className="h-3.5 w-3.5 mr-2 animate-spin" /> Validando unicidade...</>
                    ) : (
                      <><CheckCircle2 className="h-3.5 w-3.5 mr-2" /> Validar Unicidade (Bronze)</>
                    )}
                  </Button>

                  {pkValidation && (
                    <div className={`p-3 rounded-lg border text-xs space-y-1.5 ${
                      pkValidation.unique
                        ? "bg-green-50 border-green-200 text-green-800"
                        : "bg-red-50 border-red-200 text-red-800"
                    }`}>
                      <p className="font-medium">
                        {pkValidation.unique
                          ? `✅ PK única confirmada`
                          : `❌ PK não é única — ${pkValidation.duplicate_count.toLocaleString()} duplicatas`}
                      </p>
                      <p>Total: {pkValidation.total_rows.toLocaleString()} rows | Distintos: {pkValidation.distinct_rows.toLocaleString()}</p>
                      {pkValidation.sample_duplicates.length > 0 && (
                        <div className="pt-1 border-t border-red-200">
                          <p className="font-medium mb-1">Exemplos de duplicatas:</p>
                          {pkValidation.sample_duplicates.map((dup, i) => (
                            <p key={i} className="font-mono">
                              {pkColumns.map(c => `${c}=${dup[c]}`).join(', ')} ({dup._count}x)
                            </p>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}

              <div className="flex items-start gap-2 text-xs text-muted-foreground">
                <Info className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                <p>
                  Colunas de chave primária usadas para identificar registros únicos no MERGE/UPSERT.
                  Obrigatório para o modo CURRENT. Selecione uma ou mais colunas.
                </p>
              </div>
            </div>
          )}

          {/* Lookback Days */}
          {enableIncremental && (
            <div className="space-y-2">
              <Label htmlFor="lookback-days" className="font-medium">
                Lookback Days (Dias Retroativos)
              </Label>
              <Input
                id="lookback-days"
                type="number"
                min="1"
                max="365"
                value={lookbackDays}
                onChange={(e) => setLookbackDays(e.target.value)}
                placeholder="3"
              />
              <div className="flex items-start gap-2 text-xs text-muted-foreground">
                <Info className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                <p>
                  Número de dias retroativos para buscar dados incrementais da data delta. 
                  Padrão: 3 dias. Use valores maiores (ex: 30) para reprocessamento histórico.
                </p>
              </div>
            </div>
          )}

          {/* Watermark Override */}
          {enableIncremental && (
            <div className="space-y-2">
              <Label htmlFor="watermark-override" className="font-medium">
                Override Watermark (Opcional)
              </Label>
              <Input
                id="watermark-override"
                type="text"
                value={overrideWatermark}
                onChange={(e) => setOverrideWatermark(e.target.value)}
                placeholder="Ex: 2024-01-01 00:00:00"
              />
              <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
                <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                <p>
                  <strong>Avançado:</strong> Define manualmente o watermark inicial para reprocessamento histórico.
                  Deixe vazio para usar o watermark normal do sistema.
                </p>
              </div>
            </div>
          )}

          {/* Strategy Selector */}
          {enableIncremental && (
            <div className="space-y-2">
              <Label htmlFor="incremental-strategy" className="font-medium">
                Estratégia de Descoberta
              </Label>
              <Select value={incrementalStrategy} onValueChange={setIncrementalStrategy}>
                <SelectTrigger id="incremental-strategy">
                  <SelectValue placeholder="Definida automaticamente pelo discovery" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="WATERMARK">
                    <div className="flex flex-col gap-1 py-1">
                      <div className="font-medium">WATERMARK</div>
                      <div className="text-xs text-muted-foreground">Usa coluna de data/timestamp para identificar dados novos/modificados</div>
                    </div>
                  </SelectItem>
                  <SelectItem value="HASH_MERGE">
                    <div className="flex flex-col gap-1 py-1">
                      <div className="font-medium">HASH_MERGE</div>
                      <div className="text-xs text-muted-foreground">Compara hash dos registros para detectar mudanças (requer PK)</div>
                    </div>
                  </SelectItem>
                  <SelectItem value="SNAPSHOT">
                    <div className="flex flex-col gap-1 py-1">
                      <div className="font-medium">SNAPSHOT</div>
                      <div className="text-xs text-muted-foreground">Sobrescreve tudo (sem incremental)</div>
                    </div>
                  </SelectItem>
                  <SelectItem value="APPEND_LOG">
                    <div className="flex flex-col gap-1 py-1">
                      <div className="font-medium">APPEND_LOG</div>
                      <div className="text-xs text-muted-foreground">Apenas adiciona (nunca atualiza, mantém histórico)</div>
                    </div>
                  </SelectItem>
                </SelectContent>
              </Select>

              {/* Strategy validation warnings */}
              {incrementalStrategy === "WATERMARK" && !watermarkColumn && (
                <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
                  <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                  <p>WATERMARK requer uma coluna de data delta (watermark) configurada acima.</p>
                </div>
              )}
              {incrementalStrategy === "WATERMARK" && pkColumns.length === 0 && bronzeMode === "CURRENT" && (
                <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
                  <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                  <p>WATERMARK + CURRENT requer PK para MERGE. Sem PK, usará lookback window (sem MERGE).</p>
                </div>
              )}
              {incrementalStrategy === "HASH_MERGE" && pkColumns.length === 0 && (
                <div className="flex items-start gap-2 p-2 bg-red-50 border border-red-200 rounded text-xs text-red-800">
                  <AlertCircle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" />
                  <p><strong>HASH_MERGE requer PK definida.</strong> Selecione colunas de PK no modo CURRENT.</p>
                </div>
              )}

              <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg text-xs text-blue-700">
                <p>
                  💡 <strong>Dica</strong>: O "Modo de Escrita" controla <em>como</em> os dados são gravados no Bronze,
                  enquanto a estratégia controla <em>como</em> o sistema identifica dados novos/modificados na origem.
                  {!incrementalStrategy && " A estratégia é definida automaticamente pelo discovery na primeira execução."}
                </p>
              </div>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={loading}>
            Cancelar
          </Button>
          <Button onClick={handleSave} disabled={loading}>
            {loading && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            Salvar Configurações
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
