import { Badge } from "@/components/ui/badge";
import { CheckCircle, AlertCircle, Loader2, Clock, XCircle } from "lucide-react";
import { cn } from "@/lib/utils";

type StatusType = "SUCCEEDED" | "FAILED" | "RUNNING" | "CLAIMED" | "PENDING" | "QUEUED" | string;

interface StatusBadgeProps {
  status: StatusType;
  showIcon?: boolean;
  className?: string;
}

const statusConfig: Record<string, {
  variant: "default" | "destructive" | "secondary" | "outline";
  icon: React.ElementType;
  iconClass: string;
  label?: string;
}> = {
  SUCCEEDED: { variant: "default", icon: CheckCircle, iconClass: "text-green-500" },
  FAILED: { variant: "destructive", icon: AlertCircle, iconClass: "text-red-500" },
  RUNNING: { variant: "secondary", icon: Loader2, iconClass: "text-blue-500 animate-spin" },
  CLAIMED: { variant: "secondary", icon: Loader2, iconClass: "text-blue-500 animate-spin" },
  PENDING: { variant: "secondary", icon: Clock, iconClass: "text-yellow-500" },
  QUEUED: { variant: "secondary", icon: Clock, iconClass: "text-yellow-500" },
  SUCCEEDED_WITH_ISSUES: { variant: "secondary", icon: AlertCircle, iconClass: "text-amber-600" },
  ORPHANED: { variant: "secondary", icon: AlertCircle, iconClass: "text-orange-600" },
  INCONSISTENT: { variant: "outline", icon: AlertCircle, iconClass: "text-violet-600" },
  TIMED_OUT: { variant: "destructive", icon: AlertCircle, iconClass: "text-orange-700" },
  CANCELLED: { variant: "outline", icon: XCircle, iconClass: "text-zinc-500" },
};

const fallbackConfig = { variant: "outline" as const, icon: XCircle, iconClass: "text-gray-400" };

export function StatusBadge({ status, showIcon = true, className }: StatusBadgeProps) {
  const s = String(status || "").toUpperCase();
  const config = statusConfig[s] || fallbackConfig;
  const Icon = config.icon;

  return (
    <Badge variant={config.variant} className={cn("gap-1", className)}>
      {showIcon && <Icon className={cn("h-3 w-3", config.iconClass)} />}
      {s}
    </Badge>
  );
}

/** Standalone icon for compact displays */
export function StatusIcon({ status, className }: { status: string; className?: string }) {
  const s = String(status || "").toUpperCase();
  const config = statusConfig[s] || fallbackConfig;
  const Icon = config.icon;
  return <Icon className={cn("h-4 w-4", config.iconClass, className)} />;
}
