import { useState } from 'react';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { AlertTriangle, Play, Loader2 } from 'lucide-react';

interface RunNowDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  jobName: string;
  hasActiveExecution: boolean;
  maxConcurrentRuns: number;
  onConfirm: () => Promise<void>;
}

export function RunNowDialog({
  open, onOpenChange, jobName,
  hasActiveExecution, maxConcurrentRuns, onConfirm,
}: RunNowDialogProps) {
  const [running, setRunning] = useState(false);

  const handleConfirm = async () => {
    setRunning(true);
    try {
      await onConfirm();
      onOpenChange(false);
    } catch {
      // error handled by parent
    } finally {
      setRunning(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Play className="h-5 w-5 text-blue-600" />
            Executar Job Agora
          </DialogTitle>
          <DialogDescription>
            O job <strong>"{jobName}"</strong> será executado imediatamente, fora do agendamento normal.
            Todos os datasets associados serão processados.
          </DialogDescription>
        </DialogHeader>

        {hasActiveExecution && (
          <div className="flex items-start gap-3 p-3 bg-amber-50 border border-amber-200 rounded-lg">
            <AlertTriangle className="h-5 w-5 text-amber-600 mt-0.5 flex-shrink-0" />
            <div className="text-sm">
              <p className="font-medium text-amber-800">Execução já em andamento</p>
              <p className="text-amber-700 text-xs mt-1">
                {maxConcurrentRuns > 1
                  ? `Este job permite até ${maxConcurrentRuns} execuções paralelas. Uma nova execução será criada em paralelo.`
                  : 'Este job permite apenas 1 execução por vez. A nova execução pode aguardar na fila ou conflitar com a atual.'}
              </p>
            </div>
          </div>
        )}

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={running}>
            Cancelar
          </Button>
          <Button onClick={handleConfirm} disabled={running}>
            {running ? (
              <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Iniciando...</>
            ) : (
              <><Play className="h-4 w-4 mr-1" /> Executar Agora</>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
