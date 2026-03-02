import { Database, Shield, Activity, TrendingUp, ArrowRight, CheckCircle, Clock, Layers } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { useNavigate } from "react-router-dom";

const Home = () => {
  const navigate = useNavigate();

  const features = [
    {
      icon: Database,
      title: "Ingestão Governada",
      description: "Datasets com controle de estado, versionamento de schema e aprovações automáticas.",
    },
    {
      icon: Shield,
      title: "Governança de Schema",
      description: "Detecção de drift, aprovação de mudanças e auditoria completa de alterações.",
    },
    {
      icon: Activity,
      title: "Monitoramento Real-Time",
      description: "Acompanhe execuções, filas e performance com refresh automático.",
    },
  ];

  const stats = [
    { value: "Bronze", label: "Landing → Raw", description: "Ingestão bruta" },
    { value: "Silver", label: "Curated Data", description: "Dados tratados" },
    { value: "99.9%", label: "Uptime", description: "Disponibilidade" },
    { value: "24/7", label: "Monitoramento", description: "Vigilância contínua" },
  ];

  return (
    <div className="space-y-12">
      {/* Hero */}
      <section className="relative">
        <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-background to-blue-500/5 rounded-3xl" />
        <div className="relative p-12 text-center">
          <div className="max-w-4xl mx-auto space-y-6">
            <Badge
              variant="secondary"
              className="px-4 py-2 text-sm font-semibold bg-gradient-to-r from-primary/10 to-blue-500/10 text-primary border border-primary/20"
            >
              <Layers className="h-4 w-4 mr-2" />
              Arquitetura Medalha · Databricks
            </Badge>

            <h1 className="text-5xl md:text-6xl font-bold bg-gradient-to-r from-primary via-blue-600 to-purple-600 bg-clip-text text-transparent leading-tight">
              Plataforma de Ingestão Governada
            </h1>

            <p className="text-xl md:text-2xl text-muted-foreground max-w-3xl mx-auto leading-relaxed">
              Gerencie datasets, controle schemas, monitore execuções e governe toda a cadeia de
              ingestão de dados com segurança e rastreabilidade.
            </p>

            <div className="flex flex-col sm:flex-row gap-4 justify-center pt-6">
              <Button
                size="lg"
                onClick={() => navigate("/dashboard")}
                className="px-8 py-4 text-lg font-semibold bg-gradient-to-r from-primary to-blue-600 hover:from-primary/90 hover:to-blue-600/90 shadow-lg hover:shadow-xl transition-all duration-300 transform hover:scale-105"
              >
                <ArrowRight className="h-5 w-5 mr-2" />
                Ir para Dashboard
              </Button>

              <Button
                size="lg"
                variant="outline"
                onClick={() => navigate("/create")}
                className="px-8 py-4 text-lg font-semibold border-2 hover:bg-accent/50"
              >
                <Database className="h-5 w-5 mr-2" />
                Criar Dataset
              </Button>
            </div>
          </div>
        </div>
      </section>

      {/* Features */}
      <section className="space-y-8">
        <div className="text-center space-y-4">
          <h2 className="text-3xl md:text-4xl font-bold">Funcionalidades</h2>
          <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
            Tudo que você precisa para governar a ingestão de dados no Databricks.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {features.map((feature, index) => (
            <Card
              key={index}
              className="relative overflow-hidden border-2 hover:border-primary/20 transition-all duration-300 hover:shadow-lg group"
            >
              <div className="absolute inset-0 bg-gradient-to-br from-primary/5 to-blue-500/5 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <CardHeader className="relative">
                <div className="h-12 w-12 rounded-xl bg-gradient-to-br from-primary to-blue-500 flex items-center justify-center shadow-lg mb-4">
                  <feature.icon className="h-6 w-6 text-primary-foreground" />
                </div>
                <CardTitle className="text-xl">{feature.title}</CardTitle>
              </CardHeader>
              <CardContent className="relative">
                <CardDescription className="text-base leading-relaxed">
                  {feature.description}
                </CardDescription>
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      {/* Stats */}
      <section className="bg-gradient-to-r from-primary/5 via-background to-blue-500/5 rounded-3xl p-8 md:p-12">
        <div className="text-center space-y-8">
          <h2 className="text-3xl md:text-4xl font-bold">Arquitetura</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            {stats.map((stat, index) => (
              <div key={index} className="text-center space-y-2">
                <div className="text-4xl md:text-5xl font-bold bg-gradient-to-r from-primary to-blue-600 bg-clip-text text-transparent">
                  {stat.value}
                </div>
                <div className="text-lg font-semibold text-foreground">{stat.label}</div>
                <div className="text-sm text-muted-foreground">{stat.description}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="text-center space-y-6 py-8">
        <h2 className="text-3xl font-bold">Comece Agora</h2>
        <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
          Crie seu primeiro dataset e veja a ingestão governada em ação.
        </p>
        <div className="flex items-center justify-center space-x-6 pt-4 text-sm text-muted-foreground">
          <div className="flex items-center space-x-2">
            <CheckCircle className="h-4 w-4 text-green-500" />
            <span>Governança automática</span>
          </div>
          <div className="flex items-center space-x-2">
            <Shield className="h-4 w-4 text-blue-500" />
            <span>Schema versionado</span>
          </div>
          <div className="flex items-center space-x-2">
            <Clock className="h-4 w-4 text-purple-500" />
            <span>Monitoramento 24/7</span>
          </div>
        </div>
      </section>
    </div>
  );
};

export default Home;
