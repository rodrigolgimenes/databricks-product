import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

export type ViewMode = "engineering" | "business";

interface ViewModeContextValue {
  mode: ViewMode;
  isEngineering: boolean;
  isBusiness: boolean;
  toggle: () => void;
  setMode: (m: ViewMode) => void;
}

const ViewModeContext = createContext<ViewModeContextValue | null>(null);

const STORAGE_KEY = "ops-view-mode";

export function ViewModeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ViewMode>(() => {
    try {
      return (localStorage.getItem(STORAGE_KEY) as ViewMode) || "business";
    } catch {
      return "business";
    }
  });

  const setMode = useCallback((m: ViewMode) => {
    setModeState(m);
    try {
      localStorage.setItem(STORAGE_KEY, m);
    } catch {
      /* noop */
    }
  }, []);

  const toggle = useCallback(() => {
    setMode(mode === "engineering" ? "business" : "engineering");
  }, [mode, setMode]);

  return (
    <ViewModeContext.Provider
      value={{
        mode,
        isEngineering: mode === "engineering",
        isBusiness: mode === "business",
        toggle,
        setMode,
      }}
    >
      {children}
    </ViewModeContext.Provider>
  );
}

export function useViewMode() {
  const ctx = useContext(ViewModeContext);
  if (!ctx) throw new Error("useViewMode must be used within ViewModeProvider");
  return ctx;
}
