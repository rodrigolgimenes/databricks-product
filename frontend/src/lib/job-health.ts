/**
 * Pure functions for job health & risk calculations.
 * No React dependencies — testable and memoizable.
 */

export type RiskLevel = 'stable' | 'unstable' | 'critical';

export interface StatusDistribution {
  succeeded: number;
  failed: number;
  running: number;
  other: number;
  total: number;
}

export interface DurationAnalysis {
  avgMs: number;
  lastMs: number;
  variationPercent: number; // positive = slower than avg, negative = faster
  trend: 'faster' | 'slower' | 'stable';
}

export interface SparklinePoint {
  status: 'success' | 'failure' | 'running' | 'other';
  durationMs: number;
  normalizedHeight: number; // 0-1 for SVG rendering
}

/* ── Status Distribution ─────────────────────────── */

export function getStatusDistribution(runs: any[]): StatusDistribution {
  const dist: StatusDistribution = { succeeded: 0, failed: 0, running: 0, other: 0, total: runs.length };

  for (const r of runs) {
    const s = String(r.status || '').toUpperCase();
    if (s === 'SUCCESS' || s === 'SUCCEEDED') dist.succeeded++;
    else if (s === 'FAILED') dist.failed++;
    else if (s === 'RUNNING' || s === 'PENDING' || s === 'CLAIMED') dist.running++;
    else dist.other++;
  }

  return dist;
}

/* ── Duration Analysis ───────────────────────────── */

export function calculateDurationVariance(runs: any[]): DurationAnalysis {
  const withDuration = runs.filter((r) => Number(r.duration_ms) > 0);
  if (withDuration.length === 0) {
    return { avgMs: 0, lastMs: 0, variationPercent: 0, trend: 'stable' };
  }

  const avgMs = Math.round(
    withDuration.reduce((sum, r) => sum + Number(r.duration_ms), 0) / withDuration.length
  );

  const lastMs = Number(withDuration[0]?.duration_ms) || 0;

  const variationPercent = avgMs > 0
    ? Math.round(((lastMs - avgMs) / avgMs) * 100)
    : 0;

  const trend: DurationAnalysis['trend'] =
    Math.abs(variationPercent) < 15 ? 'stable' :
    variationPercent > 0 ? 'slower' : 'faster';

  return { avgMs, lastMs, variationPercent, trend };
}

/* ── Operational Risk ────────────────────────────── */

export function calculateOperationalRisk(
  runs: any[],
  jobEnabled: boolean
): { level: RiskLevel; reasons: string[] } {
  if (runs.length === 0) {
    return { level: 'stable', reasons: ['Sem histórico de execuções'] };
  }

  const reasons: string[] = [];
  const dist = getStatusDistribution(runs);
  const successRate = dist.total > 0 ? (dist.succeeded / dist.total) * 100 : 0;
  const lastStatus = String(runs[0]?.status || '').toUpperCase();

  // Critical checks
  if (lastStatus === 'FAILED') {
    reasons.push('Última execução falhou');
  }
  if (successRate < 50 && dist.total >= 3) {
    reasons.push(`Taxa de sucesso muito baixa (${Math.round(successRate)}%)`);
  }
  if (!jobEnabled && dist.failed > 0) {
    reasons.push('Job desabilitado com falhas recentes');
  }

  if (reasons.length > 0) {
    return { level: 'critical', reasons };
  }

  // Unstable checks
  const recent5 = runs.slice(0, 5);
  const recentFailures = recent5.filter(
    (r) => String(r.status || '').toUpperCase() === 'FAILED'
  ).length;

  if (recentFailures >= 2) {
    reasons.push(`${recentFailures} falhas nas últimas 5 execuções`);
  }

  const durationAnalysis = calculateDurationVariance(runs);
  if (durationAnalysis.trend === 'slower' && durationAnalysis.variationPercent > 100) {
    reasons.push(`Última execução ${durationAnalysis.variationPercent}% mais lenta que a média`);
  }

  if (reasons.length > 0) {
    return { level: 'unstable', reasons };
  }

  return { level: 'stable', reasons: [] };
}

/* ── Sparkline Data ──────────────────────────────── */

export function buildSparklineData(runs: any[], maxPoints = 10): SparklinePoint[] {
  const recent = runs.slice(0, maxPoints).reverse(); // oldest → newest for left→right
  if (recent.length === 0) return [];

  const maxDuration = Math.max(...recent.map((r) => Number(r.duration_ms) || 0), 1);

  return recent.map((r) => {
    const s = String(r.status || '').toUpperCase();
    const dur = Number(r.duration_ms) || 0;
    return {
      status:
        s === 'SUCCESS' || s === 'SUCCEEDED' ? 'success' :
        s === 'FAILED' ? 'failure' :
        s === 'RUNNING' || s === 'PENDING' ? 'running' : 'other',
      durationMs: dur,
      normalizedHeight: Math.max(0.08, dur / maxDuration),
    };
  });
}
