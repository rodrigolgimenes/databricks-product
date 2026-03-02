import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { RefreshCw, Search, Plus, Database } from 'lucide-react';
import { DataOpsGrid } from '@/components/DataOpsGrid';
import { datasetColumns } from '@/components/dataset/ColumnDefinitions';
import { QuickFilters } from '@/components/dataset/QuickFilters';
import * as api from '@/lib/api';

export const DatasetsDataOps = () => {
  const navigate = useNavigate();
  
  const [datasets, setDatasets] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [dataOpsFilters, setDataOpsFilters] = useState<Record<string, any>>({});

  // Fetch datasets with DataOps filters
  const fetchDatasets = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, any> = { page: 1, page_size: 1000 }; // Get all for AG Grid to handle
      
      if (search) params.search = search;
      
      // Apply DataOps filters
      if (dataOpsFilters.load_type) params.load_type = dataOpsFilters.load_type;
      if (dataOpsFilters.has_watermark) params.has_watermark = dataOpsFilters.has_watermark;
      if (dataOpsFilters.stale_days) params.stale_days = dataOpsFilters.stale_days;

      const data = await api.getDatasets(params);
      setDatasets(data.items || []);
    } catch (e) {
      console.error('Erro ao carregar datasets:', e);
    } finally {
      setLoading(false);
    }
  }, [search, dataOpsFilters]);

  useEffect(() => {
    const timer = setTimeout(fetchDatasets, 300);
    return () => clearTimeout(timer);
  }, [fetchDatasets]);

  const handleRowClick = (event: any) => {
    const datasetId = event.data.dataset_id;
    if (datasetId) {
      navigate(`/datasets/${datasetId}`);
    }
  };

  const handleFilterChange = (newFilters: Record<string, any>) => {
    setDataOpsFilters(newFilters);
  };

  const handleClearFilters = () => {
    setDataOpsFilters({});
    setSearch('');
  };

  return (
    <div className="space-y-4 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Database className="h-8 w-8 text-primary" />
          <div>
            <h1 className="text-3xl font-bold">Datasets - Painel DataOps</h1>
            <p className="text-muted-foreground text-sm mt-0.5">
              Governança e monitoramento de cargas incrementais
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={fetchDatasets}>
            <RefreshCw className="h-4 w-4 mr-2" />
            Atualizar
          </Button>
          <Button size="sm" onClick={() => navigate('/create')}>
            <Plus className="h-4 w-4 mr-2" />
            Novo Dataset
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="relative w-96">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Buscar datasets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-10"
        />
      </div>

      {/* Quick Filters */}
      <QuickFilters
        onFilterChange={handleFilterChange}
        activeFilters={dataOpsFilters}
        onClearFilters={handleClearFilters}
      />

      {/* Stats Bar */}
      <div className="flex items-center gap-6 px-4 py-3 bg-blue-50 rounded-lg border border-blue-200">
        <div className="flex items-center gap-2">
          <span className="text-2xl font-bold text-blue-700">{datasets.length}</span>
          <span className="text-sm text-blue-600">Datasets</span>
        </div>
        <div className="h-8 w-px bg-blue-300" />
        <div className="flex items-center gap-2">
          <span className="text-xl">🔄</span>
          <span className="text-sm text-blue-600">
            {datasets.filter(d => d.load_type === 'INCREMENTAL').length} Incrementais
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xl">🔴</span>
          <span className="text-sm text-blue-600">
            {datasets.filter(d => !d.watermark_column).length} Sem Watermark
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xl">⚠️</span>
          <span className="text-sm text-blue-600">
            {datasets.filter(d => {
              if (!d.last_success_at) return true;
              const daysSince = (Date.now() - new Date(d.last_success_at).getTime()) / (1000 * 60 * 60 * 24);
              return daysSince > 3;
            }).length} Parados {'>'}3 dias
          </span>
        </div>
      </div>

      {/* DataOps Grid */}
      <DataOpsGrid
        columnDefs={datasetColumns}
        rowData={datasets}
        onRowClicked={handleRowClick}
        loading={loading}
      />
    </div>
  );
};

export default DatasetsDataOps;
