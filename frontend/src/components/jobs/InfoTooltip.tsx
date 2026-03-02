import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { HelpCircle } from 'lucide-react';
import { FIELD_EXPLANATIONS } from '@/lib/field-explanations';

interface InfoTooltipProps {
  /** Key from FIELD_EXPLANATIONS dictionary */
  fieldKey: string;
  /** Override text (used when not in dictionary) */
  text?: string;
  className?: string;
}

export function InfoTooltip({ fieldKey, text, className }: InfoTooltipProps) {
  const explanation = text || FIELD_EXPLANATIONS[fieldKey];
  if (!explanation) return null;

  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <HelpCircle className={`h-3.5 w-3.5 text-muted-foreground/60 hover:text-muted-foreground cursor-help inline-block ml-1 ${className || ''}`} />
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs text-xs leading-relaxed">
          {explanation}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** Label with integrated tooltip — convenience wrapper */
export function LabelWithHelp({
  label,
  fieldKey,
  text,
  className,
}: {
  label: string;
  fieldKey: string;
  text?: string;
  className?: string;
}) {
  return (
    <span className={`inline-flex items-center gap-0.5 ${className || ''}`}>
      {label}
      <InfoTooltip fieldKey={fieldKey} text={text} />
    </span>
  );
}
