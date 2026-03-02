import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { TrendingUp, TrendingDown, Clock, Activity, Database, Minus } from 'lucide-react';
import { JobMetrics, formatDuration } from './helpers';
import { LabelWithHelp } from './InfoTooltip';
import {
  StatusDistribution,
  DurationAnalysis,
  SparklinePoint,
} from '@/lib/job-health';

interface JobMetricsCardsProps {
  metrics: JobMetrics;
  datasetCount: number;
  statusDist?: StatusDistribution;
  durationAnalysis?: DurationAnalysis;
  sparkline?: SparklinePoint[];
}

/* ── Mini Sparkline SVG ─────────────────────────── */

const SPARKLINE_COLORS: Record<string, string> = {
  success: '#16a34a',
  failure: '#dc2626',
  running: '#2563eb',
  other: '#9ca3af',
};

function MiniSparkline({ points }: { points: SparklinePoint[] }) {
  if (points.length === 0) return null;
  const w = 80;
  const h = 28;
  const barW = Math.max(3, (w - (points.length - 1) * 2) / points.length);
  const gap = 2;

  return (
    <svg width={w} height={h} className="flex-shrink-0">
      {points.map((p, i) => {
        const barH = Math.max(2, p.normalizedHeight * (h - 2));
        return (
          <rect
            key={i}
            x={i * (barW + gap)}
            y={h - barH}
            width={barW}
            height={barH}
            rx={1}
            fill={SPARKLINE_COLORS[p.status]}
            opacity={0.85}
          />
        );
      })}
    </svg>
  );
}

/* ── Variation Badge ────────────────────────────── */

function VariationBadge({ analysis }: { analysis: DurationAnalysis }) {
  if (analysis.variationPercent === 0 && analysis.avgMs === 0) return null;

  const pct = analysis.variationPercent;
  const isSlower = pct > 0;
  const Icon = pct === 0 || analysis.trend === 'stable' ? Minus : isSlower ? TrendingUp : TrendingDown;
  const color =
    analysis.trend === 'stable' ? 'text-muted-foreground' :
    isSlower ? 'text-amber-600' : 'text-green-600';

  return (
    <span className={`inline-flex items-center gap-0.5 text-[10px] ${color}`}>
      <Icon className="h-3 w-3" />
      {Math.abs(pct)}% {isSlower ? 'mais lento' : 'mais rápido'}
    </span>
  );
}

/* ── Progress Bar ───────────────────────────────── */

function SuccessBar({ rate }: { rate: number }) {
  const color = rate >= 80 ? 'bg-green-500' : rate >= 50 ? 'bg-amber-500' : 'bg-red-500';
  return (
    <div className="w-full h-1.5 bg-muted rounded-full mt-2">
      <div className={`h-full rounded-full transition-all ${color}`} style={{ width: `${rate}%` }} />
    </div>
  );
}

/* ── Status Distribution Badges ─────────────────── */

function StatusBadges({ dist }: { dist: StatusDistribution }) {
  return (
    <div className="flex items-center gap-1 mt-1.5 flex-wrap">
      {dist.succeeded > 0 && <Badge className="bg-green-600 text-[10px] px-1.5 py-0">{dist.succeeded} ok</Badge>}
      {dist.failed > 0 && <Badge className="bg-red-600 text-[10px] px-1.5 py-0">{dist.failed} erro</Badge>}
      {dist.running > 0 && <Badge className="bg-blue-600 text-[10px] px-1.5 py-0">{dist.running} exec</Badge>}
    </div>
  );
}

/* ── Main Component ─────────────────────────────── */

export function JobMetricsCards({
  metrics,
  datasetCount,
  statusDist,
  durationAnalysis,
  sparkline,
}: JobMetricsCardsProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
      {/* Success Rate */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex-1">
              <LabelWithHelp label="Taxa de Sucesso" fieldKey="success_rate" className="text-xs text-muted-foreground" />
              <p className="text-2xl font-bold">{metrics.successRate}%</p>
              <SuccessBar rate={metrics.successRate} />
            </div>
            <TrendingUp className="h-8 w-8 text-green-600 opacity-50" />
          </div>
        </CardContent>
      </Card>

      {/* Average Duration */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex-1">
              <LabelWithHelp label="Duração Média" fieldKey="avg_duration" className="text-xs text-muted-foreground" />
              <p className="text-2xl font-bold">{formatDuration(metrics.avgDuration)}</p>
              {durationAnalysis && <VariationBadge analysis={durationAnalysis} />}
            </div>
            {sparkline && sparkline.length > 0 ? (
              <MiniSparkline points={sparkline} />
            ) : (
              <Clock className="h-8 w-8 text-blue-600 opacity-50" />
            )}
          </div>
        </CardContent>
      </Card>

      {/* Total Runs */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex-1">
              <LabelWithHelp label="Total de Execuções" fieldKey="total_runs" className="text-xs text-muted-foreground" />
              <p className="text-2xl font-bold">{metrics.totalRuns}</p>
              {statusDist && <StatusBadges dist={statusDist} />}
            </div>
            <Activity className="h-8 w-8 text-purple-600 opacity-50" />
          </div>
        </CardContent>
      </Card>

      {/* Datasets */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div>
              <LabelWithHelp label="Datasets" fieldKey="dataset_count" className="text-xs text-muted-foreground" />
              <p className="text-2xl font-bold">{datasetCount}</p>
            </div>
            <Database className="h-8 w-8 text-orange-600 opacity-50" />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
