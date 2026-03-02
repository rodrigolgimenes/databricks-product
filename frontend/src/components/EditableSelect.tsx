import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Pencil, Check, X } from "lucide-react";

type Props = {
  label: string;
  value: string;
  onChange: (value: string) => void;
  options: Array<{ value: string; label: string; disabled?: boolean }>;
  disabled?: boolean;
  placeholder?: string;
  allowCustom?: boolean;
  customLabel?: string;
};

export const EditableSelect = ({
  label,
  value,
  onChange,
  options,
  disabled = false,
  placeholder,
  allowCustom = true,
  customLabel,
}: Props) => {
  const [isEditing, setIsEditing] = useState(false);
  const [customValue, setCustomValue] = useState("");
  const [isCustomMode, setIsCustomMode] = useState(false);

  const selectedOption = options.find((opt) => opt.value === value);
  const displayValue = selectedOption?.label || customLabel || value;

  const handleSaveCustom = () => {
    if (customValue.trim()) {
      onChange(customValue.trim());
      setIsEditing(false);
      setIsCustomMode(false);
      setCustomValue("");
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setIsCustomMode(false);
    setCustomValue("");
  };

  if (isEditing && isCustomMode) {
    return (
      <div className="space-y-2">
        <Label>{label} (Personalizado)</Label>
        <div className="flex gap-2">
          <Input
            value={customValue}
            onChange={(e) => setCustomValue(e.target.value)}
            placeholder="Digite o nome personalizado"
            autoFocus
            onKeyDown={(e) => {
              if (e.key === "Enter") handleSaveCustom();
              if (e.key === "Escape") handleCancel();
            }}
          />
          <Button
            size="icon"
            variant="ghost"
            onClick={handleSaveCustom}
            disabled={!customValue.trim()}
          >
            <Check className="h-4 w-4 text-green-600" />
          </Button>
          <Button size="icon" variant="ghost" onClick={handleCancel}>
            <X className="h-4 w-4 text-red-600" />
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <Label>{label}</Label>
        {allowCustom && value && !disabled && (
          <Button
            variant="ghost"
            size="sm"
            className="h-6 text-xs"
            onClick={() => {
              setIsEditing(true);
              setIsCustomMode(true);
              setCustomValue(displayValue);
            }}
          >
            <Pencil className="h-3 w-3 mr-1" />
            Editar
          </Button>
        )}
      </div>
      <Select value={value} onValueChange={onChange} disabled={disabled}>
        <SelectTrigger>
          <SelectValue placeholder={placeholder} />
        </SelectTrigger>
        <SelectContent>
          {options.map((opt) => (
            <SelectItem
              key={opt.value}
              value={opt.value}
              disabled={opt.disabled}
            >
              {opt.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {value && displayValue && (
        <p className="text-xs text-muted-foreground">
          Selecionado: <span className="font-medium">{displayValue}</span>
        </p>
      )}
    </div>
  );
};
