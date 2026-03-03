import { Database, Sparkles } from "lucide-react";
import { Link } from "react-router-dom";

export const Header = () => {
  return (
    <header className="h-16 bg-card border-b border-border shadow-sm px-6 flex items-center glass-card">
      <Link to="/" className="flex items-center space-x-3 hover:opacity-80 transition-opacity">
        <div className="relative">
          <div className="h-10 w-10 rounded-xl bg-gradient-to-r from-primary to-blue-500 flex items-center justify-center shadow-lg">
            <Database className="h-6 w-6 text-primary-foreground" />
            <Sparkles className="absolute -top-1 -right-1 h-4 w-4 text-yellow-400" />
          </div>
        </div>
        <div>
          <h1 className="text-xl font-bold gradient-text">Ingestão Governada</h1>
          <p className="text-xs text-muted-foreground">Plataforma de Dados · Databricks</p>
        </div>
      </Link>
    </header>
  );
};
