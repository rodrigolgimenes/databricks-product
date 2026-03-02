export const Footer = () => {
  return (
    <footer className="bg-card border-t border-border py-6 px-6">
      <div className="max-w-7xl mx-auto">
        <div className="flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center space-x-2 text-sm text-muted-foreground">
            <span>Plataforma de Ingestão Governada</span>
          </div>

          <div className="flex items-center space-x-1 text-sm text-muted-foreground">
            <span>© 2026 · Desenvolvido para Civil Master</span>
          </div>

          <div className="flex items-center space-x-6 text-sm">
            <a href="#" className="text-muted-foreground hover:text-primary transition-colors">
              Documentação
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
};
