import { Bell, User, Settings, LogOut, Database, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

export const Header = () => {
  return (
    <header className="h-16 bg-card border-b border-border shadow-sm px-6 flex items-center justify-between glass-card">
      <div className="flex items-center justify-between w-full">
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

        <div className="hidden md:flex items-center space-x-2 text-xs text-muted-foreground">
          <span>Powered by</span>
          <span className="font-semibold text-primary">Databricks SQL</span>
        </div>
      </div>

      <div className="flex items-center space-x-4 ml-6">
        <Button variant="ghost" size="icon" className="relative hover-lift">
          <Bell className="h-5 w-5" />
          <span className="absolute top-0 right-0 h-2 w-2 bg-red-500 rounded-full animate-pulse"></span>
        </Button>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" className="flex items-center space-x-3 hover-lift">
              <div className="h-8 w-8 bg-gradient-to-r from-primary to-blue-500 rounded-full flex items-center justify-center shadow-md">
                <User className="h-4 w-4 text-primary-foreground" />
              </div>
              <div className="text-left hidden sm:block">
                <p className="text-sm font-medium">Usuário</p>
                <p className="text-xs text-muted-foreground">Portal</p>
              </div>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56 glass-card">
            <DropdownMenuItem className="hover:bg-accent/50">
              <Settings className="mr-2 h-4 w-4" />
              Configurações
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem className="hover:bg-destructive/10 focus:bg-destructive/10">
              <LogOut className="mr-2 h-4 w-4" />
              Sair
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
};
