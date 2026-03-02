import { useState } from 'react';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { AlertTriangle, Trash2, Loader2 } from 'lucide-react';

interface DeleteJobDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  jobName: string;
  onConfirm: () => Promise<void>;
}

export function DeleteJobDialog({ open, onOpenChange, jobName, onConfirm }: DeleteJobDialogProps) {
  const [confirmText, setConfirmText] = useState('');
  const [deleting, setDeleting] = useState(false);
  const isMatch = confirmText === jobName;

  const handleConfirm = async () => {
    if (!isMatch) return;
    setDeleting(true);
    try {
      await onConfirm();
    } catch {
      // error handled by parent
    } finally {
      setDeleting(false);
    }
  };

  const handleOpenChange = (v: boolean) => {
    if (!v) setConfirmText('');
    onOpenChange(v);
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-red-700">
            <Trash2 className="h-5 w-5" />
            Excluir Job Permanentemente
          </DialogTitle>
          <DialogDescription>
            Esta ação é <strong>irreversível</strong>. O job será removido do portal e do Databricks.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3">
          <div className="flex items-start gap-3 p-3 bg-red-50 border border-red-200 rounded-lg">
            <AlertTriangle className="h-5 w-5 text-red-600 mt-0.5 flex-shrink-0" />
            <div className="text-xs text-red-700 space-y-1">
              <p>• A configuração do job será apagada permanentemente</p>
              <p>• O job será removido do Databricks</p>
              <p>• O histórico de execuções será perdido</p>
              <p>• Os datasets NÃO serão excluídos (apenas desassociados)</p>
            </div>
          </div>

          <div>
            <p className="text-sm mb-2">
              Para confirmar, digite o nome do job: <strong className="font-mono">{jobName}</strong>
            </p>
            <Input
              value={confirmText}
              onChange={(e) => setConfirmText(e.target.value)}
              placeholder={jobName}
              className={isMatch ? 'border-red-500 focus-visible:ring-red-500' : ''}
              autoComplete="off"
            />
          </div>
        </div>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="outline" onClick={() => handleOpenChange(false)} disabled={deleting}>
            Cancelar
          </Button>
          <Button variant="destructive" onClick={handleConfirm} disabled={!isMatch || deleting}>
            {deleting ? (
              <><Loader2 className="h-4 w-4 mr-1 animate-spin" /> Excluindo...</>
            ) : (
              <><Trash2 className="h-4 w-4 mr-1" /> Excluir Permanentemente</>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
