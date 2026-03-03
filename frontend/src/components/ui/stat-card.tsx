import { Card, CardContent } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { cn } from "@/lib/utils";

interface StatCardProps {
  title: string;
  value: string | number;
  icon?: React.ReactNode;
  trend?: string;
  trendType?: "positive" | "negative" | "neutral";
  description?: string;
  className?: string;
  loading?: boolean;
}

const trendConfig = {
  positive: { icon: TrendingUp, color: "text-green-600" },
  negative: { icon: TrendingDown, color: "text-red-600" },
  neutral: { icon: Minus, color: "text-muted-foreground" },
};

export function StatCard({ title, value, icon, trend, trendType = "neutral", description, className, loading }: StatCardProps) {
  const trendInfo = trendConfig[trendType];
  const TrendIcon = trendInfo.icon;

  const tooltipText = description || title;

  return (
    <Card className={cn("hover:shadow-md transition-shadow", className)}>
      <CardContent className="flex items-center gap-4 p-5">
        {icon && (
          <div className="flex-shrink-0 p-2.5 bg-muted/50 rounded-lg">
            {icon}
          </div>
        )}
        <div className="min-w-0 flex-1">
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger asChild>
                <p className="text-xs font-medium text-muted-foreground truncate cursor-help">{title}</p>
              </TooltipTrigger>
              <TooltipContent side="top">
                <p className="text-xs">{tooltipText}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
          {loading ? (
            <div className="h-7 w-16 bg-muted animate-pulse rounded mt-1" />
          ) : (
            <div className="flex items-baseline gap-2 mt-0.5">
              <p className="text-2xl font-bold tabular-nums">{value}</p>
              {trend && (
                <span className={cn("flex items-center gap-0.5 text-xs font-medium", trendInfo.color)}>
                  <TrendIcon className="h-3 w-3" />
                  {trend}
                </span>
              )}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
