import { useState, useEffect, useRef, useCallback } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  CheckCircle, XCircle, Copy, AlertTriangle, Loader2, Clock,
} from "lucide-react";
import * as api from "@/lib/api";
import { toast } from "sonner";

type BatchResult = {
  dataset_name: string;
  status: string;
  message?: string;
  dataset_id?: string;
};

type BatchStatus = {
  batch_id: string;
  status: "RUNNING" | "COMPLETED" | "FAILED";
  total: number;
  processed: number;
  created: number;
  failed: number;
  exists: number;
  duplicate: number;
  error: number;
  elapsed_ms: number;
  error_message?: string | null;
  results: BatchResult[];
};

type BatchSummary = {
  total: number;
  created: number;
  failed: number;
  exists: number;
  duplicate: number;
  error: number;
};

interface BatchProgressOverlayProps {
  batchId: string;
  onComplete: (results: BatchResult[], summary: BatchSummary) => void;
}

const statusConfig: Record<string, { label: string; color: string; icon: typeof CheckCircle }> = {
  VALID: { label: "Válido", color: "text-green-600 bg-green-50", icon: CheckCircle },
  CREATED: { label: "Criado", color: "text-green-600 bg-green-50", icon: CheckCircle },
  ERROR: { label: "Erro", color: "text-red-600 bg-red-50", icon: XCircle },
  DUPLICATE: { label: "Duplicado", color: "text-yellow-600 bg-yellow-50", icon: Copy },
  EXISTS: { label: "Já existe", color: "text-orange-600 bg-orange-50", icon: AlertTriangle },
};

function formatElapsed(ms: number): string {
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  const remainSecs = secs % 60;
  return `${mins}m ${remainSecs}s`;
}

const BatchProgressOverlay = ({ batchId, onComplete }: BatchProgressOverlayProps) => {
  const [data, setData] = useState<BatchStatus | null>(null);
  const [pollError, setPollError] = useState("");
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const completedRef = useRef(false);

  const poll = useCallback(async () => {
    try {
      const res = await api.getBatchStatus(batchId);
      setData(res);
      setPollError("");

      if ((res.status === "COMPLETED" || res.status === "FAILED") && !completedRef.current) {
        completedRef.current = true;

        // Stop polling
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = null;
        }

        // Toast notification
        if (res.status === "COMPLETED") {
          const hasErrors = (res.failed || 0) + (res.error || 0) > 0;
          if (hasErrors) {
            toast.warning(
              `Criação concluída: ${res.created} criados, ${(res.failed || 0) + (res.error || 0)} erros`,
              { duration: 6000 }
            );
          } else {
            toast.success(
              `Criação concluída com sucesso (${res.created} datasets criados)`,
              { duration: 5000 }
            );
          }
        } else {
          toast.error(
            res.error_message || "Erro fatal durante a criação em massa.",
            { duration: 8000 }
          );
        }

        // Build summary compatible with existing bulkResult format
        const summary: BatchSummary = {
          total: res.total,
          created: res.created,
          failed: res.failed,
          exists: res.exists,
          duplicate: res.duplicate,
          error: res.error,
        };

        // Small delay so user can see 100%
        setTimeout(() => onComplete(res.results, summary), 600);
      }
    } catch {
      setPollError("Erro ao consultar status. Tentando novamente...");
    }
  }, [batchId, onComplete]);

  useEffect(() => {
    // Initial poll
    poll();
    // Start interval
    intervalRef.current = setInterval(poll, 2000);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [poll]);

  const pct = data ? Math.round((data.processed / Math.max(data.total, 1)) * 100) : 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <Card className="w-full max-w-2xl mx-4 shadow-2xl">
        <CardContent className="p-6 space-y-5">
          {/* Header */}
          <div className="flex items-center gap-3">
            <div className="p-2 bg-primary/10 rounded-lg">
              <Loader2 className={`h-6 w-6 text-primary ${data?.status === "RUNNING" ? "animate-spin" : ""}`} />
            </div>
            <div>
              <h2 className="text-lg font-bold">
                {data?.status === "COMPLETED"
                  ? "Criação concluída!"
                  : data?.status === "FAILED"
                  ? "Criação falhou"
                  : "Criando datasets..."}
              </h2>
              <p className="text-sm text-muted-foreground">
                {data ? `${data.processed} / ${data.total} processados` : "Iniciando..."}
              </p>
            </div>
          </div>

          {/* Progress bar */}
          <div className="space-y-2">
            <Progress value={pct} className="h-3" />
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>{pct}%</span>
              {data && (
                <span className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  Tempo decorrido: {formatElapsed(data.elapsed_ms)}
                </span>
              )}
            </div>
          </div>

          {/* Summary badges */}
          {data && data.processed > 0 && (
            <div className="flex items-center gap-3 flex-wrap">
              {data.created > 0 && (
                <Badge className="bg-green-100 text-green-800 gap-1">
                  <CheckCircle className="h-3 w-3" /> {data.created} criados
                </Badge>
              )}
              {data.exists > 0 && (
                <Badge className="bg-orange-100 text-orange-800 gap-1">
                  <AlertTriangle className="h-3 w-3" /> {data.exists} já existem
                </Badge>
              )}
              {data.duplicate > 0 && (
                <Badge className="bg-yellow-100 text-yellow-800 gap-1">
                  <Copy className="h-3 w-3" /> {data.duplicate} duplicados
                </Badge>
              )}
              {(data.failed + data.error) > 0 && (
                <Badge variant="destructive" className="gap-1">
                  <XCircle className="h-3 w-3" /> {data.failed + data.error} erros
                </Badge>
              )}
            </div>
          )}

          {/* Per-item status table */}
          {data && data.results.length > 0 && (
            <div className="border rounded-lg max-h-[280px] overflow-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-background border-b">
                  <tr>
                    <th className="text-left p-2 font-medium w-10">#</th>
                    <th className="text-left p-2 font-medium">Dataset</th>
                    <th className="text-left p-2 font-medium w-28">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {data.results.map((r, i) => {
                    const cfg = statusConfig[r.status] || statusConfig.ERROR;
                    const Icon = cfg.icon;
                    return (
                      <tr key={i} className="border-b hover:bg-muted/30">
                        <td className="p-2 text-muted-foreground text-xs">{i + 1}</td>
                        <td className="p-2 font-mono text-xs">{r.dataset_name}</td>
                        <td className="p-2">
                          <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
                            <Icon className="h-3 w-3" /> {cfg.label}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {/* Remaining items indicator */}
          {data && data.status === "RUNNING" && data.processed < data.total && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" />
              <span>
                {data.total - data.processed} restante{data.total - data.processed > 1 ? "s" : ""}...
              </span>
            </div>
          )}

          {/* Info message */}
          {data?.status === "RUNNING" && (
            <p className="text-xs text-muted-foreground bg-blue-50 text-blue-700 p-3 rounded-lg">
              A criação continuará em segundo plano mesmo que você saia desta tela.
            </p>
          )}

          {/* Poll error */}
          {pollError && (
            <p className="text-xs text-destructive">{pollError}</p>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default BatchProgressOverlay;
