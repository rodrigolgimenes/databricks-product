import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { 
  Database, 
  Table, 
  Search, 
  CheckCircle, 
  Loader2, 
  AlertCircle,
  RefreshCw,
  Info
} from "lucide-react";
import * as api from "@/lib/api";
import { toast } from "sonner";

type TableInfo = {
  table_name: string;
  schema_name: string;
  table_type: string;
  row_count?: number;
  size_mb?: number;
  description?: string;
};

type Props = {
  onTablesSelected: (tables: string[]) => void;
  selectedTables?: string[];
};

export const SupabaseTableSelector = ({ onTablesSelected, selectedTables = [] }: Props) => {
  const [loading, setLoading] = useState(true);
  const [testing, setTesting] = useState(false);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string>("public");
  const [searchTerm, setSearchTerm] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set(selectedTables));
  const [connectionStatus, setConnectionStatus] = useState<"unknown" | "success" | "error">("unknown");
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    testConnection();
  }, []);

  useEffect(() => {
    if (connectionStatus === "success") {
      loadSchemas();
    }
  }, [connectionStatus]);

  useEffect(() => {
    if (selectedSchema && connectionStatus === "success") {
      loadTables();
    }
  }, [selectedSchema, connectionStatus]);

  const testConnection = async () => {
    setTesting(true);
    setErrorMessage("");
    try {
      await api.testSupabaseConnection();
      setConnectionStatus("success");
      toast.success("Conexão Supabase estabelecida com sucesso!");
    } catch (e: any) {
      setConnectionStatus("error");
      setErrorMessage(e.message || "Erro ao conectar com Supabase");
      toast.error("Falha ao conectar com Supabase");
    } finally {
      setTesting(false);
    }
  };

  const loadSchemas = async () => {
    try {
      const data = await api.getSupabaseSchemas();
      setSchemas(data.schemas || ["public"]);
      if (data.schemas && data.schemas.length > 0) {
        setSelectedSchema(data.schemas.includes("public") ? "public" : data.schemas[0]);
      }
    } catch (e: any) {
      toast.error("Erro ao carregar schemas");
      setSchemas(["public"]);
    }
  };

  const loadTables = async () => {
    setLoading(true);
    setErrorMessage("");
    try {
      const data = await api.getSupabaseTables(selectedSchema);
      setTables(data.tables || []);
    } catch (e: any) {
      setErrorMessage(e.message || "Erro ao carregar tabelas");
      toast.error("Erro ao carregar tabelas");
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  const toggleTable = (tableName: string) => {
    const fullName = `${selectedSchema}.${tableName}`;
    const newSelected = new Set(selected);
    if (newSelected.has(fullName)) {
      newSelected.delete(fullName);
    } else {
      newSelected.add(fullName);
    }
    setSelected(newSelected);
    onTablesSelected(Array.from(newSelected));
  };

  const selectAll = () => {
    const filtered = getFilteredTables();
    const newSelected = new Set(selected);
    filtered.forEach(t => newSelected.add(`${selectedSchema}.${t.table_name}`));
    setSelected(newSelected);
    onTablesSelected(Array.from(newSelected));
  };

  const deselectAll = () => {
    const filtered = getFilteredTables();
    const newSelected = new Set(selected);
    filtered.forEach(t => newSelected.delete(`${selectedSchema}.${t.table_name}`));
    setSelected(newSelected);
    onTablesSelected(Array.from(newSelected));
  };

  const getFilteredTables = () => {
    if (!searchTerm) return tables;
    const term = searchTerm.toLowerCase();
    return tables.filter(t => 
      t.table_name.toLowerCase().includes(term) ||
      t.description?.toLowerCase().includes(term)
    );
  };

  if (testing) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center p-12">
          <div className="text-center space-y-4">
            <Loader2 className="h-12 w-12 animate-spin text-primary mx-auto" />
            <p className="text-muted-foreground">Testando conexão com Supabase...</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (connectionStatus === "error") {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-center space-y-4">
            <AlertCircle className="h-12 w-12 text-red-500 mx-auto" />
            <div>
              <h3 className="text-lg font-semibold">Erro de Conexão</h3>
              <p className="text-sm text-muted-foreground mt-2">{errorMessage}</p>
            </div>
            <Button onClick={testConnection} variant="outline">
              <RefreshCw className="h-4 w-4 mr-2" />
              Tentar Novamente
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  }

  const filteredTables = getFilteredTables();
  const selectedInCurrentSchema = Array.from(selected).filter(s => s.startsWith(`${selectedSchema}.`)).length;

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-green-100 rounded-lg">
              <Database className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <CardTitle>Selecionar Tabelas do Supabase</CardTitle>
              <CardDescription>
                Escolha as tabelas que deseja ingerir no Databricks
              </CardDescription>
            </div>
          </div>
          <Badge variant="outline" className="gap-1">
            <CheckCircle className="h-3 w-3 text-green-600" />
            {selected.size} selecionadas
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Filtros */}
        <div className="flex items-center gap-3">
          <div className="flex-1">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Buscar tabelas..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9"
              />
            </div>
          </div>
          <Select value={selectedSchema} onValueChange={setSelectedSchema}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Schema" />
            </SelectTrigger>
            <SelectContent>
              {schemas.map(schema => (
                <SelectItem key={schema} value={schema}>
                  {schema}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button onClick={loadTables} variant="outline" size="icon">
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>

        {/* Ações em massa */}
        {filteredTables.length > 0 && (
          <div className="flex items-center justify-between p-3 bg-muted/50 rounded-lg">
            <div className="text-sm text-muted-foreground">
              {selectedInCurrentSchema} de {filteredTables.length} tabelas selecionadas neste schema
            </div>
            <div className="flex gap-2">
              <Button onClick={selectAll} variant="ghost" size="sm">
                Selecionar todas
              </Button>
              <Button onClick={deselectAll} variant="ghost" size="sm">
                Limpar seleção
              </Button>
            </div>
          </div>
        )}

        {/* Lista de tabelas */}
        {loading ? (
          <div className="space-y-3">
            {[1, 2, 3, 4, 5].map(i => (
              <div key={i} className="flex items-center gap-3 p-3 border rounded-lg">
                <Skeleton className="h-5 w-5" />
                <div className="flex-1 space-y-2">
                  <Skeleton className="h-4 w-48" />
                  <Skeleton className="h-3 w-32" />
                </div>
              </div>
            ))}
          </div>
        ) : filteredTables.length === 0 ? (
          <div className="text-center py-12 text-muted-foreground">
            <Table className="h-12 w-12 mx-auto mb-4 opacity-30" />
            <p>Nenhuma tabela encontrada</p>
            {searchTerm && <p className="text-sm mt-2">Tente ajustar o filtro de busca</p>}
          </div>
        ) : (
          <div className="space-y-2 max-h-[400px] overflow-y-auto">
            {filteredTables.map((table) => {
              const fullName = `${selectedSchema}.${table.table_name}`;
              const isSelected = selected.has(fullName);
              
              return (
                <div
                  key={table.table_name}
                  className={`flex items-start gap-3 p-3 border rounded-lg cursor-pointer transition-all hover:border-primary/50 hover:bg-muted/30 ${
                    isSelected ? "bg-primary/5 border-primary" : ""
                  }`}
                  onClick={() => toggleTable(table.table_name)}
                >
                  <Checkbox
                    checked={isSelected}
                    onCheckedChange={() => toggleTable(table.table_name)}
                    onClick={(e) => e.stopPropagation()}
                  />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <Table className="h-4 w-4 text-primary flex-shrink-0" />
                      <span className="font-mono text-sm font-medium">{table.table_name}</span>
                      {table.table_type !== "BASE TABLE" && (
                        <Badge variant="outline" className="text-xs">
                          {table.table_type}
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-3 mt-1 text-xs text-muted-foreground">
                      {table.row_count !== undefined && (
                        <span>{table.row_count.toLocaleString()} linhas</span>
                      )}
                      {table.size_mb !== undefined && (
                        <span>{table.size_mb.toFixed(2)} MB</span>
                      )}
                      {table.description && (
                        <div className="flex items-center gap-1">
                          <Info className="h-3 w-3" />
                          <span className="truncate max-w-[300px]">{table.description}</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Informações adicionais */}
        {selected.size > 0 && (
          <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <div className="flex gap-2">
              <Info className="h-5 w-5 text-blue-600 flex-shrink-0 mt-0.5" />
              <div className="text-sm text-blue-900">
                <p className="font-medium">Próximos passos:</p>
                <p className="mt-1 text-blue-700">
                  As tabelas selecionadas serão configuradas como datasets e carregadas para a camada Bronze do Databricks.
                </p>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};
