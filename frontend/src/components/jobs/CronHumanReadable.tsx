import cronstrue from 'cronstrue/i18n';
import { Calendar } from 'lucide-react';

interface CronHumanReadableProps {
  expression: string;
  className?: string;
}

function parseCron(expression: string): string {
  if (!expression) return '—';
  try {
    return cronstrue.toString(expression, {
      locale: 'pt_BR',
      use24HourTimeFormat: true,
      verbose: false,
    });
  } catch {
    return expression;
  }
}

export function CronHumanReadable({ expression, className }: CronHumanReadableProps) {
  const humanText = parseCron(expression);
  const isRaw = humanText === expression; // couldn't parse

  return (
    <div className={`flex items-start gap-2 ${className || ''}`}>
      <Calendar className="h-4 w-4 text-blue-500 mt-0.5 flex-shrink-0" />
      <div>
        <p className="text-sm font-medium text-foreground">{humanText}</p>
        {!isRaw && expression && (
          <code className="text-[10px] text-muted-foreground font-mono">{expression}</code>
        )}
      </div>
    </div>
  );
}
