import { useViewMode } from "@/contexts/ViewModeContext";
import { cn } from "@/lib/utils";

export function ViewModeToggle() {
  const { mode, setMode } = useViewMode();

  return (
    <div className="inline-flex rounded-lg border p-0.5 text-xs font-medium">
      <button
        onClick={() => setMode("business")}
        className={cn(
          "rounded-md px-3 py-1.5 transition-colors",
          mode === "business"
            ? "bg-primary text-primary-foreground shadow-sm"
            : "text-muted-foreground hover:text-foreground"
        )}
      >
        Visão Negócio
      </button>
      <button
        onClick={() => setMode("engineering")}
        className={cn(
          "rounded-md px-3 py-1.5 transition-colors",
          mode === "engineering"
            ? "bg-primary text-primary-foreground shadow-sm"
            : "text-muted-foreground hover:text-foreground"
        )}
      >
        Visão Engenharia
      </button>
    </div>
  );
}
