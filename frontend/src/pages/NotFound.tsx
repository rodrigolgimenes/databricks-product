import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { ArrowLeft } from "lucide-react";

const NotFound = () => {
  const navigate = useNavigate();
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] text-center">
      <h1 className="text-6xl font-bold gradient-text mb-4">404</h1>
      <p className="text-xl text-muted-foreground mb-6">Página não encontrada</p>
      <Button onClick={() => navigate("/dashboard")}>
        <ArrowLeft className="h-4 w-4 mr-2" /> Voltar ao Dashboard
      </Button>
    </div>
  );
};

export default NotFound;
