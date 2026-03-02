import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  CheckCircle, XCircle, RefreshCw, Shield, AlertTriangle, Plus, Minus, ArrowRightLeft,
} from "lucide-react";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import * as api from "@/lib/api";

const Approvals = () => {
  const [items, setItems] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [comments, setComments] = useState<Record<string, string>>({});

  const fetchApprovals = async () => {
    setLoading(true);
    try {
      const data = await api.getPendingApprovals();
      setItems(data.items || []);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchApprovals();
  }, []);

  const handleApprove = async (datasetId: string, schemaVersion: number) => {
    const key = `${datasetId}-${schemaVersion}`;
    setActionLoading(key);
    try {
      await api.approveSchema(datasetId, schemaVersion, comments[key] || "");
      setItems((prev) => prev.filter((it) => it.dataset_id !== datasetId));
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setActionLoading(null);
    }
  };

  const handleReject = async (datasetId: string, schemaVersion: number) => {
    const key = `${datasetId}-${schemaVersion}`;
    setActionLoading(key);
    try {
      await api.rejectSchema(datasetId, schemaVersion, comments[key] || "");
      setItems((prev) => prev.filter((it) => it.dataset_id !== datasetId));
    } catch (e: any) {
      alert(`Erro: ${e.message}`);
    } finally {
      setActionLoading(null);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <LoadingSpinner size="lg" text="Carregando aprovações..." />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Shield className="h-6 w-6" /> Aprovações de Schema
          </h1>
          <p className="text-muted-foreground mt-1">
            Revise e aprove mudanças de schema detectadas pelo orquestrador
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={fetchApprovals}>
          <RefreshCw className="h-4 w-4 mr-2" /> Atualizar
        </Button>
      </div>

      {items.length === 0 ? (
        <Card>
          <CardContent className="py-16 text-center">
            <CheckCircle className="h-12 w-12 mx-auto mb-4 text-green-500 opacity-50" />
            <p className="text-lg font-medium text-muted-foreground">Nenhuma aprovação pendente</p>
            <p className="text-sm text-muted-foreground mt-1">
              Todos os schemas estão aprovados e atualizados.
            </p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {items.map((item) => {
            const key = `${item.dataset_id}-${item.pending?.schema_version}`;
            const isProcessing = actionLoading === key;

            return (
              <Card key={key} className="border-l-4 border-l-yellow-500">
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle className="text-lg">{item.dataset_name}</CardTitle>
                      <p className="text-sm text-muted-foreground font-mono mt-1">
                        {item.dataset_id}
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline">{item.execution_state}</Badge>
                      <Badge className="bg-yellow-500 text-white">
                        <AlertTriangle className="h-3 w-3 mr-1" /> PENDING
                      </Badge>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Schema Info */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                    <div>
                      <p className="text-muted-foreground">Schema Atual</p>
                      <p className="font-medium">
                        {item.active ? `v${item.active.schema_version}` : "—"}
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Schema Pendente</p>
                      <p className="font-bold text-yellow-700">v{item.pending?.schema_version}</p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Criado por</p>
                      <p className="font-medium">{item.pending?.created_by || "—"}</p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Data</p>
                      <p className="font-medium">
                        {item.pending?.created_at ? new Date(item.pending.created_at).toLocaleString("pt-BR") : "—"}
                      </p>
                    </div>
                  </div>

                  {/* Diff Summary */}
                  {item.diff_summary && item.diff_summary.total > 0 && (
                    <div className="bg-muted/30 p-4 rounded-lg">
                      <p className="font-medium text-sm mb-2">Resumo das Mudanças:</p>
                      <div className="flex items-center gap-4 text-sm">
                        {item.diff_summary.add > 0 && (
                          <span className="flex items-center gap-1 text-green-700">
                            <Plus className="h-3 w-3" /> {item.diff_summary.add} adicionada(s)
                          </span>
                        )}
                        {item.diff_summary.remove > 0 && (
                          <span className="flex items-center gap-1 text-red-700">
                            <Minus className="h-3 w-3" /> {item.diff_summary.remove} removida(s)
                          </span>
                        )}
                        {item.diff_summary.type_change > 0 && (
                          <span className="flex items-center gap-1 text-orange-700">
                            <ArrowRightLeft className="h-3 w-3" /> {item.diff_summary.type_change} tipo alterado
                          </span>
                        )}
                        {item.diff_summary.nullability_change > 0 && (
                          <span className="flex items-center gap-1 text-blue-700">
                            {item.diff_summary.nullability_change} nullability
                          </span>
                        )}
                      </div>

                      {/* Diff Preview */}
                      {(item.diff_preview || []).length > 0 && (
                        <div className="mt-3 space-y-1">
                          {item.diff_preview.slice(0, 8).map((d: any, i: number) => (
                            <p key={i} className="text-xs font-mono text-muted-foreground">
                              {d.type}: <span className="font-bold">{d.column}</span>
                              {d.type === "TYPE_CHANGE" && ` (${d.active?.type} → ${d.pending?.type})`}
                              {d.type === "ADD_COLUMN" && ` [${d.pending?.type}]`}
                            </p>
                          ))}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Comments + Actions */}
                  <div className="flex items-end gap-4">
                    <div className="flex-1">
                      <Input
                        placeholder="Comentário (opcional)"
                        value={comments[key] || ""}
                        onChange={(e) => setComments({ ...comments, [key]: e.target.value })}
                        disabled={isProcessing}
                      />
                    </div>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => handleReject(item.dataset_id, item.pending?.schema_version)}
                      disabled={isProcessing}
                    >
                      <XCircle className="h-4 w-4 mr-1" /> Rejeitar
                    </Button>
                    <Button
                      size="sm"
                      onClick={() => handleApprove(item.dataset_id, item.pending?.schema_version)}
                      disabled={isProcessing}
                      className="bg-green-600 hover:bg-green-700"
                    >
                      <CheckCircle className="h-4 w-4 mr-1" /> Aprovar
                    </Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default Approvals;
