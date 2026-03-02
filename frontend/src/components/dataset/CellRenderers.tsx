import { ICellRendererParams } from 'ag-grid-community';
import { Badge } from '@/components/ui/badge';
import { formatDateRelative } from '@/lib/utils';

// Status Badge Renderer
export const StatusBadgeRenderer = (params: ICellRendererParams) => {
  const status = params.value;
  const colors: Record<string, string> = {
    ACTIVE: 'bg-green-500 text-white',
    PAUSED: 'bg-yellow-500 text-white',
    DRAFT: 'bg-blue-500 text-white',
    BLOCKED_SCHEMA_CHANGE: 'bg-red-500 text-white',
    DEPRECATED: 'bg-gray-500 text-white'
  };
  
  return (
    <Badge className={`${colors[status] || 'bg-gray-200 text-gray-800'} text-xs`}>
      {status}
    </Badge>
  );
};

// Source Badge Renderer
export const SourceBadgeRenderer = (params: ICellRendererParams) => {
  const source = params.value;
  const colors: Record<string, string> = {
    ORACLE: 'bg-orange-100 text-orange-800',
    SUPABASE: 'bg-green-100 text-green-800',
    SHAREPOINT: 'bg-purple-100 text-purple-800'
  };
  
  return (
    <Badge variant="outline" className={`${colors[source] || 'bg-gray-100 text-gray-800'} text-xs`}>
      {source}
    </Badge>
  );
};

// Load Type Badge Renderer
export const LoadTypeBadgeRenderer = (params: ICellRendererParams) => {
  const type = params.value;
  const colors: Record<string, string> = {
    FULL: 'bg-gray-100 text-gray-800 border-gray-300',
    INCREMENTAL: 'bg-green-100 text-green-800 border-green-300',
    SNAPSHOT: 'bg-purple-100 text-purple-800 border-purple-300'
  };
  
  const icons: Record<string, string> = {
    FULL: '📦',
    INCREMENTAL: '🔄',
    SNAPSHOT: '📸'
  };
  
  return (
    <Badge variant="outline" className={`${colors[type] || 'bg-gray-100 text-gray-800'} text-xs font-medium`}>
      <span className="mr-1">{icons[type]}</span>
      {type}
    </Badge>
  );
};

// Bronze Mode Renderer
export const BronzeModeRenderer = (params: ICellRendererParams) => {
  const mode = params.value;
  if (!mode) return <span className="text-gray-400">-</span>;
  
  const colors: Record<string, string> = {
    SNAPSHOT: 'bg-purple-50 text-purple-700 border-purple-200',
    CURRENT: 'bg-blue-50 text-blue-700 border-blue-200',
    APPEND_LOG: 'bg-amber-50 text-amber-700 border-amber-200'
  };
  
  const icons: Record<string, string> = {
    SNAPSHOT: '📸',
    CURRENT: '🔄',
    APPEND_LOG: '📝'
  };
  
  return (
    <Badge variant="outline" className={`${colors[mode] || 'bg-gray-100 text-gray-800'} text-xs`}>
      <span className="mr-1">{icons[mode]}</span>
      {mode}
    </Badge>
  );
};

// Last Execution Renderer with Health Status
export const LastExecRenderer = (params: ICellRendererParams) => {
  const date = params.value;
  
  if (!date) {
    return (
      <div className="flex items-center gap-2 text-gray-400">
        <span className="text-lg">⚪</span>
        <span className="text-xs">Nunca executado</span>
      </div>
    );
  }
  
  const now = Date.now();
  const execDate = new Date(date).getTime();
  const daysSince = (now - execDate) / (1000 * 60 * 60 * 24);
  
  let statusIcon = '🟢';
  let statusColor = 'text-green-700';
  
  if (daysSince > 3) {
    statusIcon = '🔴';
    statusColor = 'text-red-700';
  } else if (daysSince > 1) {
    statusIcon = '🟡';
    statusColor = 'text-yellow-700';
  }
  
  return (
    <div className={`flex items-center gap-2 ${statusColor}`}>
      <span className="text-lg">{statusIcon}</span>
      <span className="text-xs font-medium">{formatDateRelative(date)}</span>
    </div>
  );
};
