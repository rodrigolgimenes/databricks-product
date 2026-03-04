import { useState, useEffect, useRef } from "react";
import { Loader2 } from "lucide-react";
import * as api from "@/lib/api";
import { getCurrentPhaseLabel } from "@/components/PipelineStepper";

interface RunningPhaseBadgeProps {
  runId: string;
}

/**
 * Lightweight inline badge that shows the current pipeline phase
 * for a RUNNING execution. Polls steps every 8s.
 */
export function RunningPhaseBadge({ runId }: RunningPhaseBadgeProps) {
  const [label, setLabel] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchPhase = async () => {
    try {
      const data = await api.getRunSteps(runId);
      const items = data.items || [];
      const phase = getCurrentPhaseLabel(items, "RUNNING");
      setLabel(phase);
    } catch {
      /* ignore */
    }
  };

  useEffect(() => {
    fetchPhase();
    intervalRef.current = setInterval(fetchPhase, 8000);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  if (!label) return null;

  return (
    <span className="inline-flex items-center gap-1 text-[10px] text-blue-700 bg-blue-50 border border-blue-200 rounded px-1.5 py-0.5 font-medium whitespace-nowrap">
      <Loader2 className="h-2.5 w-2.5 animate-spin" />
      {label}
    </span>
  );
}
