import { StatCard } from "@/components/ui/stat-card";
import {
  Activity, CheckCircle, AlertCircle, Clock, Timer, TrendingUp,
} from "lucide-react";

interface MonitorKPIsProps {
  stats: Record<string, number>;
  metrics24h: {
    total: number;
    success: number;
    failed: number;
    avg_duration_sec: number;
    success_rate: number;
  } | null;
  loading?: boolean;
}

const formatDuration = (seconds: number) => {
  if (!seconds || isNaN(seconds)) return "—";
  const m = Math.floor(seconds / 60);
  const sec = Math.round(seconds % 60);
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
};

export function MonitorKPIs({ stats, metrics24h, loading }: MonitorKPIsProps) {
  const running = (stats.RUNNING || 0) + (stats.CLAIMED || 0);
  const pending = stats.PENDING || 0;
  const m = metrics24h || { total: 0, success: 0, failed: 0, avg_duration_sec: 0, success_rate: 0 };

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      <StatCard
        title="Executando Agora"
        description="Datasets em processamento (RUNNING + CLAIMED) na fila"
        value={running}
        icon={<Activity className="h-5 w-5 text-blue-600" />}
        loading={loading}
        className={running > 0 ? "border-blue-200" : ""}
      />
      <StatCard
        title="Sucesso (24h)"
        description="Datasets processados com sucesso nas últimas 24 horas"
        value={m.success}
        icon={<CheckCircle className="h-5 w-5 text-green-600" />}
        loading={loading}
      />
      <StatCard
        title="Falha (24h)"
        description="Datasets que falharam nas últimas 24 horas"
        value={m.failed}
        icon={<AlertCircle className="h-5 w-5 text-red-600" />}
        loading={loading}
        className={m.failed > 0 ? "border-red-200" : ""}
      />
      <StatCard
        title="Na Fila"
        description="Datasets aguardando processamento (PENDING)"
        value={pending}
        icon={<Clock className="h-5 w-5 text-yellow-600" />}
        loading={loading}
      />
      <StatCard
        title="Tempo Médio"
        description="Duração média de processamento nas últimas 24h"
        value={formatDuration(m.avg_duration_sec)}
        icon={<Timer className="h-5 w-5 text-purple-600" />}
        loading={loading}
      />
      <StatCard
        title="Taxa de Sucesso"
        description="Porcentagem de sucesso nas últimas 24 horas"
        value={`${m.success_rate}%`}
        icon={<TrendingUp className="h-5 w-5 text-emerald-600" />}
        trendType={m.success_rate >= 95 ? "positive" : m.success_rate >= 80 ? "neutral" : "negative"}
        loading={loading}
      />
    </div>
  );
}
