import { useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  Database,
  Plus,
  BarChart3,
  ChevronLeft,
  ChevronRight,
  Activity,
  CheckCircle,
  Settings,
  Sparkles,
  Calendar,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const navigationItems = [
  {
    title: "Dashboard",
    href: "/dashboard",
    icon: BarChart3,
    description: "Visão geral da plataforma",
    badge: null,
  },
  {
    title: "Datasets",
    href: "/datasets",
    icon: Database,
    description: "Gerenciar datasets",
    badge: null,
  },
  {
    title: "Criar Dataset",
    href: "/create",
    icon: Plus,
    description: "Novo dataset de ingestão",
    badge: null,
  },
  {
    title: "Jobs Agendados",
    href: "/jobs",
    icon: Calendar,
    description: "Automação de ingestões",
    badge: null,
  },
  {
    title: "Monitoramento",
    href: "/monitor",
    icon: Activity,
    description: "Execuções em tempo real",
    badge: null,
  },
  {
    title: "Aprovações",
    href: "/approvals",
    icon: CheckCircle,
    description: "Schema changes pendentes",
    badge: "Gov",
  },
  {
    title: "Configurações",
    href: "/settings",
    icon: Settings,
    description: "Naming conventions",
    badge: null,
  },
];

export const Sidebar = () => {
  const [collapsed, setCollapsed] = useState(false);
  const location = useLocation();

  return (
    <aside
      className={cn(
        "bg-card border-r border-border transition-all duration-300 flex flex-col glass-card",
        collapsed ? "w-16" : "w-72"
      )}
    >
      {/* Toggle */}
      <div className="p-4 border-b border-border">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => setCollapsed(!collapsed)}
          className="w-full justify-center hover-lift"
        >
          {collapsed ? (
            <ChevronRight className="h-4 w-4" />
          ) : (
            <ChevronLeft className="h-4 w-4" />
          )}
        </Button>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4">
        <ul className="space-y-1">
          {navigationItems.map((item) => {
            const isActive = location.pathname === item.href;
            return (
              <li key={item.href}>
                <NavLink
                  to={item.href}
                  className={cn(
                    "group flex items-center space-x-3 px-4 py-3 rounded-xl text-sm font-medium transition-all duration-200 relative overflow-hidden",
                    isActive
                      ? "bg-gradient-to-r from-primary/10 to-blue-500/10 text-primary border border-primary/20 shadow-md"
                      : "text-muted-foreground hover:bg-accent hover:text-accent-foreground hover-lift"
                  )}
                  title={collapsed ? item.title : undefined}
                >
                  {isActive && (
                    <div className="absolute left-0 top-0 bottom-0 w-1 bg-gradient-to-b from-primary to-blue-500 rounded-r-full" />
                  )}

                  <div
                    className={cn(
                      "flex items-center justify-center h-8 w-8 rounded-lg transition-all",
                      isActive
                        ? "bg-gradient-to-br from-primary to-blue-500 text-primary-foreground shadow-lg"
                        : "bg-muted/50 group-hover:bg-accent"
                    )}
                  >
                    <item.icon className="h-4 w-4" />
                  </div>

                  {!collapsed && (
                    <div className="flex-1 min-w-0 flex items-center justify-between">
                      <div>
                        <p className="font-semibold">{item.title}</p>
                        <p className="text-xs opacity-70 truncate">
                          {item.description}
                        </p>
                      </div>
                      {item.badge && (
                        <span className="px-2 py-1 text-xs font-semibold rounded-full bg-primary/20 text-primary">
                          {item.badge}
                        </span>
                      )}
                    </div>
                  )}
                </NavLink>
              </li>
            );
          })}
        </ul>
      </nav>

      {/* Status indicator */}
      {!collapsed && (
        <div className="p-4 border-t border-border">
          <div className="bg-gradient-to-r from-green-50 to-emerald-50 dark:from-green-950/20 dark:to-emerald-950/20 border border-green-200 dark:border-green-800 rounded-xl p-3 shadow-sm">
            <div className="flex items-center space-x-2">
              <div className="relative">
                <div className="h-2 w-2 bg-green-500 rounded-full animate-pulse"></div>
                <div className="absolute inset-0 h-2 w-2 bg-green-400 rounded-full animate-ping"></div>
              </div>
              <span className="text-sm font-semibold text-green-700 dark:text-green-400">
                Sistema Online
              </span>
            </div>
            <div className="flex items-center justify-between mt-2">
              <p className="text-xs text-green-600 dark:text-green-500">
                Databricks conectado
              </p>
              <div className="flex items-center space-x-1">
                <Sparkles className="h-3 w-3 text-yellow-500" />
                <span className="text-xs font-bold text-green-700 dark:text-green-400">
                  Ativo
                </span>
              </div>
            </div>
          </div>
        </div>
      )}
    </aside>
  );
};
