import { useState, useRef, useCallback, useEffect } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { ChevronDown, ChevronRight, ChevronLeft, ChevronsLeft, ChevronsRight, ArrowUpDown, ArrowUp, ArrowDown, Inbox } from "lucide-react";
import { cn } from "@/lib/utils";

export interface DataTableColumn<T = any> {
  key: string;
  header: string;
  render?: (row: T, index: number) => React.ReactNode;
  sortable?: boolean;
  className?: string;
  headerClassName?: string;
  /** Minimum column width in px (default: 60) */
  minWidth?: number;
  /** Initial column width in px */
  width?: number;
}

interface DataTableProps<T = any> {
  columns: DataTableColumn<T>[];
  data: T[];
  /** Total number of items (for pagination). If not provided, no pagination is shown. */
  total?: number;
  page?: number;
  pageSize?: number;
  onPageChange?: (page: number) => void;
  onPageSizeChange?: (size: number) => void;
  pageSizeOptions?: number[];
  sortKey?: string;
  sortDir?: "asc" | "desc";
  onSort?: (key: string, dir: "asc" | "desc") => void;
  loading?: boolean;
  emptyIcon?: React.ReactNode;
  emptyMessage?: string;
  expandableRow?: (row: T, index: number) => React.ReactNode;
  rowKey?: (row: T, index: number) => string;
  rowClassName?: (row: T, index: number) => string;
  onRowClick?: (row: T, index: number) => void;
}

export function DataTable<T = any>({
  columns,
  data,
  total,
  page = 1,
  pageSize = 25,
  onPageChange,
  onPageSizeChange,
  pageSizeOptions = [10, 25, 50, 100],
  sortKey,
  sortDir,
  onSort,
  loading,
  emptyIcon,
  emptyMessage = "Nenhum registro encontrado.",
  expandableRow,
  rowKey,
  rowClassName,
  onRowClick,
}: DataTableProps<T>) {
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const totalPages = total != null ? Math.max(1, Math.ceil(total / pageSize)) : undefined;

  /* ── Column resizing ─────────────────────────── */
  const [colWidths, setColWidths] = useState<Record<string, number>>(() => {
    const init: Record<string, number> = {};
    for (const col of columns) {
      if (col.width) init[col.key] = col.width;
    }
    return init;
  });
  const resizeRef = useRef<{
    colKey: string;
    startX: number;
    startW: number;
    minW: number;
  } | null>(null);

  const onResizeMouseDown = useCallback(
    (e: React.MouseEvent, colKey: string, minW: number) => {
      e.preventDefault();
      e.stopPropagation();
      const th = (e.target as HTMLElement).closest('th');
      const startW = colWidths[colKey] ?? th?.offsetWidth ?? 120;
      resizeRef.current = { colKey, startX: e.clientX, startW, minW };

      const onMouseMove = (ev: MouseEvent) => {
        if (!resizeRef.current) return;
        const delta = ev.clientX - resizeRef.current.startX;
        const newW = Math.max(resizeRef.current.minW, resizeRef.current.startW + delta);
        setColWidths((prev) => ({ ...prev, [resizeRef.current!.colKey]: newW }));
      };
      const onMouseUp = () => {
        resizeRef.current = null;
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', onMouseUp);
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      };
      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup', onMouseUp);
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
    },
    [colWidths]
  );

  // Reset widths when columns change (different page/tab)
  useEffect(() => {
    setColWidths((prev) => {
      const init: Record<string, number> = {};
      for (const col of columns) {
        init[col.key] = prev[col.key] ?? col.width ?? 0; // 0 = auto
      }
      return init;
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [columns.map((c) => c.key).join(',')]);

  const toggleExpand = (key: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const handleSort = (key: string) => {
    if (!onSort) return;
    const newDir = sortKey === key && sortDir === "asc" ? "desc" : "asc";
    onSort(key, newDir);
  };

  const SortIcon = ({ colKey }: { colKey: string }) => {
    if (sortKey !== colKey) return <ArrowUpDown className="h-3 w-3 ml-1 opacity-30" />;
    return sortDir === "asc"
      ? <ArrowUp className="h-3 w-3 ml-1" />
      : <ArrowDown className="h-3 w-3 ml-1" />;
  };

  return (
    <div>
      <div className="border rounded-lg overflow-hidden">
        <Table className="table-fixed">
          <TableHeader>
            <TableRow className="bg-muted/30">
              {expandableRow && <TableHead className="w-8" />}
              {columns.map((col) => {
                const w = colWidths[col.key];
                return (
                  <TableHead
                    key={col.key}
                    className={cn(
                      "relative group/th",
                      col.sortable && "cursor-pointer select-none hover:bg-muted/50",
                      col.headerClassName
                    )}
                    style={w ? { width: w, minWidth: col.minWidth ?? 60 } : { minWidth: col.minWidth ?? 60 }}
                    onClick={() => col.sortable && handleSort(col.key)}
                  >
                    <div className="flex items-center overflow-hidden">
                      <span className="truncate">{col.header}</span>
                      {col.sortable && <SortIcon colKey={col.key} />}
                    </div>
                    {/* Resize handle */}
                    <div
                      className="absolute right-0 top-0 h-full w-2 cursor-col-resize z-10
                        flex items-center justify-center
                        opacity-0 group-hover/th:opacity-100 transition-opacity"
                      onMouseDown={(e) => onResizeMouseDown(e, col.key, col.minWidth ?? 60)}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="w-px h-4 bg-border group-hover/th:bg-primary/40" />
                    </div>
                  </TableHead>
                );
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              Array.from({ length: Math.min(pageSize, 5) }).map((_, i) => (
                <TableRow key={`skeleton-${i}`}>
                  {expandableRow && <TableCell><Skeleton className="h-4 w-4" /></TableCell>}
                  {columns.map((col) => (
                    <TableCell key={col.key}>
                      <Skeleton className="h-4 w-full max-w-[120px]" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : data.length === 0 ? (
              <TableRow>
                <TableCell colSpan={columns.length + (expandableRow ? 1 : 0)} className="py-12">
                  <div className="flex flex-col items-center justify-center text-muted-foreground gap-2">
                    {emptyIcon || <Inbox className="h-10 w-10" />}
                    <p className="text-sm">{emptyMessage}</p>
                  </div>
                </TableCell>
              </TableRow>
            ) : (
              data.map((row, idx) => {
                const key = rowKey ? rowKey(row, idx) : String(idx);
                const isExpanded = expandedRows.has(key);
                return (
                  <>
                    <TableRow
                      key={key}
                      className={cn(
                        "transition-colors",
                        (onRowClick || expandableRow) && "cursor-pointer hover:bg-muted/30",
                        isExpanded && "bg-muted/20",
                        rowClassName?.(row, idx)
                      )}
                      onClick={() => {
                        if (expandableRow) toggleExpand(key);
                        onRowClick?.(row, idx);
                      }}
                    >
                      {expandableRow && (
                        <TableCell className="w-8 px-2">
                          {isExpanded
                            ? <ChevronDown className="h-4 w-4 text-muted-foreground" />
                            : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                        </TableCell>
                      )}
                      {columns.map((col) => (
                        <TableCell key={col.key} className={cn("overflow-hidden", col.className)}>
                          <div className="truncate">
                            {col.render
                              ? col.render(row, idx)
                              : String((row as any)[col.key] ?? "—")}
                          </div>
                        </TableCell>
                      ))}
                    </TableRow>
                    {expandableRow && isExpanded && (
                      <TableRow key={`${key}-expanded`}>
                        <TableCell colSpan={columns.length + 1} className="p-0 bg-muted/10">
                          <div className="p-4 border-t">
                            {expandableRow(row, idx)}
                          </div>
                        </TableCell>
                      </TableRow>
                    )}
                  </>
                );
              })
            )}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      {totalPages != null && totalPages > 0 && (
        <div className="flex items-center justify-between mt-3 text-sm">
          <div className="flex items-center gap-2 text-muted-foreground">
            <span>Itens por página:</span>
            <Select
              value={String(pageSize)}
              onValueChange={(v) => {
                onPageSizeChange?.(Number(v));
                onPageChange?.(1);
              }}
            >
              <SelectTrigger className="h-8 w-[70px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {pageSizeOptions.map((s) => (
                  <SelectItem key={s} value={String(s)}>{s}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            {total != null && (
              <span className="ml-2">
                {((page - 1) * pageSize) + 1}–{Math.min(page * pageSize, total)} de {total}
              </span>
            )}
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon"
              className="h-8 w-8"
              disabled={page <= 1}
              onClick={() => onPageChange?.(1)}
            >
              <ChevronsLeft className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              className="h-8 w-8"
              disabled={page <= 1}
              onClick={() => onPageChange?.(page - 1)}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="px-3 text-muted-foreground">
              {page} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="icon"
              className="h-8 w-8"
              disabled={page >= totalPages}
              onClick={() => onPageChange?.(page + 1)}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              className="h-8 w-8"
              disabled={page >= totalPages}
              onClick={() => onPageChange?.(totalPages)}
            >
              <ChevronsRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
