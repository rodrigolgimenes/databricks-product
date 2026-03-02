import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Search, RefreshCw, X } from "lucide-react";

export interface FilterOption {
  value: string;
  label: string;
}

export interface FilterDefinition {
  key: string;
  label: string;
  options: FilterOption[];
  placeholder?: string;
}

interface FilterBarProps {
  filters?: FilterDefinition[];
  filterValues?: Record<string, string>;
  onFilterChange?: (key: string, value: string) => void;
  searchPlaceholder?: string;
  searchValue?: string;
  onSearchChange?: (value: string) => void;
  onRefresh?: () => void;
  refreshing?: boolean;
  children?: React.ReactNode;
}

export function FilterBar({
  filters = [],
  filterValues = {},
  onFilterChange,
  searchPlaceholder = "Buscar...",
  searchValue = "",
  onSearchChange,
  onRefresh,
  refreshing,
  children,
}: FilterBarProps) {
  const [localSearch, setLocalSearch] = useState(searchValue);

  const handleSearchKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      onSearchChange?.(localSearch);
    }
  };

  const clearSearch = () => {
    setLocalSearch("");
    onSearchChange?.("");
  };

  const hasActiveFilters = Object.values(filterValues).some((v) => v && v !== "all") || localSearch;

  return (
    <div className="flex flex-wrap items-center gap-2 mb-4">
      {/* Search */}
      {onSearchChange && (
        <div className="relative flex-1 min-w-[200px] max-w-sm">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder={searchPlaceholder}
            value={localSearch}
            onChange={(e) => setLocalSearch(e.target.value)}
            onKeyDown={handleSearchKeyDown}
            className="pl-9 pr-8 h-9"
          />
          {localSearch && (
            <button
              onClick={clearSearch}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      )}

      {/* Filter Selects */}
      {filters.map((filter) => (
        <Select
          key={filter.key}
          value={filterValues[filter.key] || "all"}
          onValueChange={(v) => onFilterChange?.(filter.key, v)}
        >
          <SelectTrigger className="h-9 w-auto min-w-[130px]">
            <SelectValue placeholder={filter.placeholder || filter.label} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{filter.label}</SelectItem>
            {filter.options.map((opt) => (
              <SelectItem key={opt.value} value={opt.value}>
                {opt.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      ))}

      {children}

      <div className="flex items-center gap-2 ml-auto">
        {hasActiveFilters && (
          <Button
            variant="ghost"
            size="sm"
            className="h-9 text-xs"
            onClick={() => {
              clearSearch();
              filters.forEach((f) => onFilterChange?.(f.key, "all"));
            }}
          >
            <X className="h-3.5 w-3.5 mr-1" /> Limpar
          </Button>
        )}
        {onRefresh && (
          <Button variant="outline" size="sm" className="h-9" onClick={onRefresh} disabled={refreshing}>
            <RefreshCw className={`h-4 w-4 mr-1.5 ${refreshing ? "animate-spin" : ""}`} />
            Atualizar
          </Button>
        )}
      </div>
    </div>
  );
}
