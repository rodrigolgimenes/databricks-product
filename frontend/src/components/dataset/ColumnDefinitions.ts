import { ColDef } from 'ag-grid-community';
import { formatDate, formatDateRelative } from '@/lib/utils';

export const datasetColumns: ColDef[] = [
  // ================== FIXAS (pinned left) ==================
  { 
    field: 'dataset_name', 
    headerName: 'Dataset',
    pinned: 'left',
    width: 300,
    filter: 'agTextColumnFilter',
    filterParams: { 
      buttons: ['reset', 'apply'],
      debounceMs: 300
    },
    cellStyle: { fontWeight: 500 }
  },
  { 
    field: 'execution_state', 
    headerName: 'Status',
    pinned: 'left',
    width: 120,
    cellRenderer: 'StatusBadgeRenderer',
    filter: 'agTextColumnFilter',
    filterParams: { 
      buttons: ['reset', 'apply'],
      debounceMs: 200
    }
  },
  { 
    field: 'source_type', 
    headerName: 'Source',
    pinned: 'left',
    width: 120,
    cellRenderer: 'SourceBadgeRenderer',
    filter: 'agTextColumnFilter',
    filterParams: { 
      buttons: ['reset', 'apply'],
      debounceMs: 200
    }
  },
  
  // ================== CONTEXTO ==================
  { 
    field: 'project_id', 
    headerName: 'Projeto', 
    width: 150,
    filter: 'agTextColumnFilter'
  },
  { 
    field: 'area_id', 
    headerName: 'Área', 
    width: 150,
    filter: 'agTextColumnFilter'
  },
  { 
    field: 'load_type', 
    headerName: 'Tipo Carga',
    width: 140,
    cellRenderer: 'LoadTypeBadgeRenderer',
    filter: 'agTextColumnFilter',
    filterParams: { 
      buttons: ['reset', 'apply'],
      debounceMs: 200
    }
  },
  
  // ================== OPERACIONAL (GOVERNANCE) ==================
  { 
    field: 'watermark_column', 
    headerName: 'Coluna Incremental',
    width: 180,
    filter: 'agTextColumnFilter',
    cellStyle: params => params.value ? {} : { color: '#999', fontStyle: 'italic' },
    valueFormatter: params => params.value || '(sem incremental)'
  },
  { 
    field: 'lookback_days', 
    headerName: 'Lookback Days',
    width: 140,
    type: 'numericColumn',
    filter: 'agNumberColumnFilter',
    cellStyle: { textAlign: 'center' },
    valueFormatter: params => params.value ? String(params.value) : '-'
  },
  { 
    field: 'created_at', 
    headerName: 'Criado em',
    width: 160,
    filter: 'agDateColumnFilter',
    valueFormatter: params => params.value ? formatDate(params.value) : '-'
  },
  { 
    field: 'last_success_at', 
    headerName: 'Última Execução',
    width: 180,
    filter: 'agDateColumnFilter',
    cellRenderer: 'LastExecRenderer',
    valueFormatter: params => params.value ? formatDateRelative(params.value) : 'Nunca executado'
  },
  
  // ================== ESTRUTURAL ==================
  { 
    field: 'bronze_table', 
    headerName: 'Bronze', 
    width: 250,
    filter: 'agTextColumnFilter',
    cellStyle: { fontFamily: 'monospace', fontSize: '12px' }
  },
  { 
    field: 'silver_table', 
    headerName: 'Silver', 
    width: 250,
    filter: 'agTextColumnFilter',
    cellStyle: { fontFamily: 'monospace', fontSize: '12px' }
  },
  { 
    field: 'bronze_mode', 
    headerName: 'Bronze Mode', 
    width: 140,
    filter: 'agTextColumnFilter',
    filterParams: { 
      buttons: ['reset', 'apply'],
      debounceMs: 200
    },
    cellRenderer: 'BronzeModeRenderer'
  },
  { 
    field: 'incremental_strategy', 
    headerName: 'Strategy', 
    width: 140,
    filter: 'agTextColumnFilter',
    filterParams: { 
      buttons: ['reset', 'apply'],
      debounceMs: 200
    }
  }
];
