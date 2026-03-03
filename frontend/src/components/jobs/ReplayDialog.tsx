import { useState, useEffect, useMemo } from 'react';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Loader2, RotateCcw, CheckCircle2, XCircle, Clock, AlertTriangle, Database,
} from 'lucide-react';
import * as api from '@/lib/api';
import type { ReplayMode, ReplayPreviewDataset } from '@/lib/api';

interface ReplayDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  jobId: string;
  jobName: string;
  executionId: string;
  onSuccess: () => void;
}

const STATUS_ICON: Record<string, React.ReactNode> = {
  SUCCEEDED: <CheckCircle2 className="h-3.5 w-3.5 text-green-600" />,
  FAILED: <XCircle className="h-3.5 w-3.5 text-red-600" />,
  PENDING: <Clock className="h-3.5 w-3.5 text-amber-500" />,
  NOT_ENQUEUED: <AlertTriangle className="h-3.5 w-3.5 text-gray-400" />,
};

const STATUS_LABEL: Record<string, string> = {
  SUCCEEDED: 'Sucesso',
  FAILED: 'Falhou',
  PENDING: 'Pendente',
  NOT_ENQUEUED: 'Não executado',
};

const MODE_LABELS: Record<ReplayMode, string> = {
  REMAINING_TODAY: 'Pendentes do dia',
  FAILED_ONLY: 'Somente falhos',
  ALL: 'Todos os datasets',
  SELECTED: 'Seleção manual',
};

export function ReplayDialog({
  open, onOpenChange, jobId, jobName, executionId, onSuccess,
}: ReplayDialogProps) {
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [preview, setPreview] = useState<api.ReplayPreviewResponse | null>(null);
  const [mode, setMode] = useState<ReplayMode>('REMAINING_TODAY');
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [error, setError] = useState<string | null>(null);

  // Load preview when dialog opens
  useEffect(() => {
    if (!open || !jobId || !executionId) return;
    setLoading(true);
    setError(null);
    api.getReplayPreview(jobId, executionId)
      .then((data) => {
        setPreview(data);
        // Pre-select non-succeeded datasets
        const preSelected = new Set(
          data.datasets
            .filter((d) => d.replay_status !== 'SUCCEEDED')
            .map((d) => d.dataset_id)
        );
        setSelectedIds(preSelected);
        // Auto-select best mode
        if (data.summary.failed > 0 && data.summary.not_enqueued === 0) {
          setMode('FAILED_ONLY');
        } else {
          setMode('REMAINING_TODAY');
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [open, jobId, executionId]);

  // Compute datasets to replay based on mode
  const datasetsToReplay = useMemo(() => {
    if (!preview) return [];
    switch (mode) {
      case 'REMAINING_TODAY':
        return preview.datasets.filter((d) => d.replay_status !== 'SUCCEEDED');
      case 'FAILED_ONLY':
        return preview.datasets.filter((d) => d.replay_status === 'FAILED');
      case 'SELECTED':
        return preview.datasets.filter((d) => selectedIds.has(d.dataset_id));
      case 'ALL':
        return preview.datasets;
      default:
        return [];
    }
  }, [preview, mode, selectedIds]);

  const handleToggle = (datasetId: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(datasetId)) next.delete(datasetId);
      else next.add(datasetId);
      return next;
    });
  };

  const handleSubmit = async () => {
    setSubmitting(true);
    setError(null);
    try {
      await api.executePartialReplay(jobId, {
        execution_id: executionId,
        mode,
        dataset_ids: mode === 'SELECTED' ? Array.from(selectedIds) : undefined,
      });
      onOpenChange(false);
      onSuccess();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <RotateCcw className="h-5 w-5 text-blue-600" />
            Retomada Parcial
          </DialogTitle>
          <DialogDescription>
            Re-execute apenas os datasets pendentes do job <strong>"{jobName}"</strong>.
          </DialogDescription>
        </DialogHeader>

        {loading && (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <span className="ml-2 text-sm text-muted-foreground">Carregando preview...</span>
          </div>
        )}

        {error && (
          <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
            {error}
          </div>
        )}

        {preview && !loading && (
          <div className="space-y-4">
            {/* Summary badges */}
            <div className="flex flex-wrap gap-2">
              <Badge variant="outline" className="gap-1">
                <Database className="h-3 w-3" /> {preview.summary.total} total
              </Badge>
              {preview.summary.succeeded > 0 && (
                <Badge className="gap-1 bg-green-100 text-green-800 hover:bg-green-100">
                  <CheckCircle2 className="h-3 w-3" /> {preview.summary.succeeded} sucesso
                </Badge>
              )}
              {preview.summary.failed > 0 && (
                <Badge className="gap-1 bg-red-100 text-red-800 hover:bg-red-100">
                  <XCircle className="h-3 w-3" /> {preview.summary.failed} falhou
                </Badge>
              )}
              {preview.summary.pending > 0 && (
                <Badge className="gap-1 bg-amber-100 text-amber-800 hover:bg-amber-100">
                  <Clock className="h-3 w-3" /> {preview.summary.pending} pendente
                </Badge>
              )}
              {preview.summary.not_enqueued > 0 && (
                <Badge className="gap-1 bg-gray-100 text-gray-700 hover:bg-gray-100">
                  <AlertTriangle className="h-3 w-3" /> {preview.summary.not_enqueued} não executado
                </Badge>
              )}
            </div>

            {/* Mode selector */}
            <div className="space-y-2">
              <p className="text-sm font-medium">Modo de replay</p>
              <div className="grid grid-cols-2 gap-2">
                {(['REMAINING_TODAY', 'FAILED_ONLY', 'SELECTED', 'ALL'] as ReplayMode[]).map((m) => {
                  const count =
                    m === 'REMAINING_TODAY'
                      ? preview.datasets.filter((d) => d.replay_status !== 'SUCCEEDED').length
                      : m === 'FAILED_ONLY'
                      ? preview.summary.failed
                      : m === 'SELECTED'
                      ? selectedIds.size
                      : preview.summary.total;
                  const isDisabled =
                    (m === 'FAILED_ONLY' && preview.summary.failed === 0) ||
                    (m === 'REMAINING_TODAY' && preview.summary.succeeded === preview.summary.total);

                  return (
                    <button
                      key={m}
                      disabled={isDisabled}
                      onClick={() => setMode(m)}
                      className={`flex items-center justify-between gap-2 p-2.5 rounded-lg border text-left text-sm transition-colors
                        ${mode === m
                          ? 'border-blue-500 bg-blue-50 text-blue-800'
                          : isDisabled
                          ? 'border-gray-200 bg-gray-50 text-gray-400 cursor-not-allowed'
                          : 'border-gray-200 hover:border-gray-300 cursor-pointer'
                        }`}
                    >
                      <span>{MODE_LABELS[m]}</span>
                      <Badge variant="secondary" className="text-xs">{count}</Badge>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Dataset list (detailed for SELECTED mode, summary for others) */}
            <div className="space-y-1">
              <p className="text-sm font-medium">
                {mode === 'SELECTED' ? 'Selecione os datasets' : 'Datasets a serem re-executados'}
                <span className="text-muted-foreground font-normal ml-1">
                  ({datasetsToReplay.length})
                </span>
              </p>
              <div className="max-h-48 overflow-y-auto border rounded-lg divide-y">
                {preview.datasets.map((ds) => {
                  const willReplay = datasetsToReplay.some((d) => d.dataset_id === ds.dataset_id);
                  return (
                    <div
                      key={ds.dataset_id}
                      className={`flex items-center gap-2 px-3 py-2 text-xs ${
                        willReplay ? '' : 'opacity-50'
                      }`}
                    >
                      {mode === 'SELECTED' && (
                        <Checkbox
                          checked={selectedIds.has(ds.dataset_id)}
                          onCheckedChange={() => handleToggle(ds.dataset_id)}
                        />
                      )}
                      {STATUS_ICON[ds.replay_status]}
                      <span className="flex-1 truncate font-medium">{ds.dataset_name}</span>
                      <span className="text-muted-foreground">{STATUS_LABEL[ds.replay_status]}</span>
                      {ds.error_class && (
                        <span className="text-red-500 truncate max-w-[120px]" title={ds.error_message || ''}>
                          {ds.error_class}
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
            Cancelar
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={submitting || loading || datasetsToReplay.length === 0}
          >
            {submitting ? (
              <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Executando...</>
            ) : (
              <><RotateCcw className="h-4 w-4 mr-1" /> Executar {datasetsToReplay.length} dataset(s)</>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
