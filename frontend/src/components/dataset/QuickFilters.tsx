import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

interface QuickFiltersProps {
  onFilterChange: (filters: Record<string, any>) => void;
  activeFilters: Record<string, any>;
  onClearFilters: () => void;
}

export const QuickFilters = ({ onFilterChange, activeFilters, onClearFilters }: QuickFiltersProps) => {
  const isActive = (filterKey: string, filterValue: any) => {
    return activeFilters[filterKey] === filterValue;
  };

  const filterButtons = [
    {
      key: 'has_watermark',
      value: 'false',
      label: '🔴 Sem Incremental',
      description: 'Datasets sem coluna watermark configurada'
    },
    {
      key: 'stale_days',
      value: 3,
      label: '⚠️ Parados >3 dias',
      description: 'Datasets sem execução há mais de 3 dias'
    },
    {
      key: 'load_type',
      value: 'FULL',
      label: '📦 Carga Full',
      description: 'Datasets com carga completa'
    },
    {
      key: 'load_type',
      value: 'INCREMENTAL',
      label: '🔄 Carga Incremental',
      description: 'Datasets com carga incremental'
    },
    {
      key: 'load_type',
      value: 'SNAPSHOT',
      label: '📸 Snapshot',
      description: 'Datasets com estratégia snapshot'
    }
  ];

  const hasActiveFilters = Object.keys(activeFilters).length > 0;

  return (
    <div className="flex flex-wrap items-center gap-2 p-4 bg-gray-50 rounded-lg border border-gray-200">
      <span className="text-sm font-medium text-gray-700 mr-2">Filtros Rápidos:</span>
      
      {filterButtons.map((filter) => {
        const active = isActive(filter.key, filter.value);
        return (
          <Button
            key={`${filter.key}-${filter.value}`}
            variant={active ? 'default' : 'outline'}
            size="sm"
            onClick={() => {
              if (active) {
                const newFilters = { ...activeFilters };
                delete newFilters[filter.key];
                onFilterChange(newFilters);
              } else {
                onFilterChange({ ...activeFilters, [filter.key]: filter.value });
              }
            }}
            title={filter.description}
            className="text-xs"
          >
            {filter.label}
            {active && <span className="ml-2 text-xs">✓</span>}
          </Button>
        );
      })}

      {hasActiveFilters && (
        <>
          <div className="h-6 w-px bg-gray-300 mx-2" />
          <Button
            variant="ghost"
            size="sm"
            onClick={onClearFilters}
            className="text-xs text-gray-600 hover:text-gray-900"
          >
            🔄 Limpar Filtros
          </Button>
          <Badge variant="secondary" className="text-xs">
            {Object.keys(activeFilters).length} ativo(s)
          </Badge>
        </>
      )}
    </div>
  );
};
