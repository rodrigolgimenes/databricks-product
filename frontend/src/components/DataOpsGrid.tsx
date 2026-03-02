import { useRef } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, ModuleRegistry, AllCommunityModule } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-theme-alpine.css';
import './DataOpsGrid.css';

// Register AG Grid modules
ModuleRegistry.registerModules([AllCommunityModule]);

import {
  StatusBadgeRenderer,
  SourceBadgeRenderer,
  LoadTypeBadgeRenderer,
  BronzeModeRenderer,
  LastExecRenderer
} from './dataset/CellRenderers';

interface DataOpsGridProps {
  columnDefs: ColDef[];
  rowData: any[];
  onRowClicked?: (event: any) => void;
  loading?: boolean;
}

export const DataOpsGrid = ({ columnDefs, rowData, onRowClicked, loading = false }: DataOpsGridProps) => {
  const gridRef = useRef<AgGridReact>(null);

  return (
    <div className="dataops-grid-container">
      {/* AG Grid */}
      <div className="ag-theme-alpine dataops-grid" style={{ height: '75vh', width: '100%' }}>
        <AgGridReact
          ref={gridRef}
          columnDefs={columnDefs}
          rowData={rowData}
          theme="legacy"
          defaultColDef={{
            sortable: true,
            filter: 'agTextColumnFilter',
            resizable: true,
            floatingFilter: true,
            minWidth: 100
          }}
          components={{
            StatusBadgeRenderer,
            SourceBadgeRenderer,
            LoadTypeBadgeRenderer,
            BronzeModeRenderer,
            LastExecRenderer
          }}
          suppressHorizontalScroll={false}
          domLayout="normal"
          rowHeight={36}
          headerHeight={40}
          onRowClicked={onRowClicked}
          animateRows={true}
          enableCellTextSelection={true}
          suppressMenuHide={false}
          loading={loading}
        />
      </div>
    </div>
  );
};
