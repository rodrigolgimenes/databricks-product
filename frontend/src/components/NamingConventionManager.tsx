import { useState, useEffect } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Check, Loader2, AlertTriangle } from "lucide-react";
import * as api from "@/lib/api";

interface NamingConventionManagerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Called when user selects a convention — receives the naming_version */
  onSelect?: (version: number) => void;
  /** Currently selected convention version (for highlighting) */
  selectedVersion?: number | null;
  onConventionChanged?: () => void;
}

export const NamingConventionManager = ({
  open,
  onOpenChange,
  onSelect,
  selectedVersion,
  onConventionChanged,
}: NamingConventionManagerProps) => {
  const [conventions, setConventions] = useState<api.NamingConvention[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (open) {
      loadConventions();
    }
  }, [open]);

  const loadConventions = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.getNamingConventions();
      setConventions(res.items || []);
    } catch (e: any) {
      setError(e.message || "Erro ao carregar convenções");
    } finally {
      setLoading(false);
    }
  };

  const handleSelect = (version: number) => {
    onSelect?.(version);
    onConventionChanged?.();
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Selecionar Convenção de Nomenclatura</DialogTitle>
        </DialogHeader>

        <p className="text-sm text-muted-foreground -mt-1">
          Clique na convenção que deseja utilizar para este dataset.
        </p>

        {error && (
          <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
            <AlertTriangle className="h-5 w-5 text-red-600 mt-0.5 flex-shrink-0" />
            <p className="text-sm text-red-900">{error}</p>
          </div>
        )}

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : conventions.length === 0 ? (
          <div className="text-center py-12 text-muted-foreground">
            <p>Nenhuma convenção cadastrada</p>
          </div>
        ) : (
          <div className="space-y-3">
            {conventions.map((convention) => {
              const isSelected = selectedVersion === convention.naming_version;

              return (
                <Card
                  key={convention.naming_version}
                  className={`cursor-pointer transition-all hover:shadow-md ${
                    isSelected
                      ? "border-primary border-2 bg-primary/5 ring-1 ring-primary/20"
                      : "hover:border-primary/50"
                  }`}
                  onClick={() => handleSelect(convention.naming_version)}
                >
                  <CardContent className="p-4 space-y-3">
                    {/* Header */}
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <h3 className="font-semibold text-lg">v{convention.naming_version}</h3>
                        {convention.is_active && (
                          <Badge className="bg-green-600 text-white">PADRÃO</Badge>
                        )}
                        {isSelected && (
                          <Badge variant="outline" className="border-primary text-primary">
                            <Check className="h-3 w-3 mr-1" />
                            Selecionada
                          </Badge>
                        )}
                      </div>
                      {/* Selection indicator */}
                      <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center transition-colors ${
                        isSelected ? "border-primary bg-primary" : "border-muted-foreground/30"
                      }`}>
                        {isSelected && <Check className="h-3 w-3 text-white" />}
                      </div>
                    </div>

                    {/* Content */}
                    <div className="space-y-2 text-sm">
                      <div>
                        <p className="text-xs text-muted-foreground font-medium">Bronze:</p>
                        <p className="font-mono text-xs text-gray-700 break-all">{convention.bronze_pattern}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground font-medium">Silver:</p>
                        <p className="font-mono text-xs text-gray-700 break-all">{convention.silver_pattern}</p>
                      </div>
                      {convention.notes && (
                        <div>
                          <p className="text-xs text-muted-foreground font-medium">Notas:</p>
                          <p className="text-xs text-gray-700">{convention.notes}</p>
                        </div>
                      )}
                      <div className="pt-1">
                        <p className="text-[11px] text-muted-foreground">
                          Criada em {new Date(convention.created_at).toLocaleString("pt-BR")} por {convention.created_by}
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
};
