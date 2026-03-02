# Script para criar cluster compartilhado (Shared Access Mode)
# Este cluster pode ser usado por múltiplos usuários

$databricksHost = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
$token = $env:DATABRICKS_TOKEN

Write-Host "Criando cluster compartilhado..."

$clusterConfig = @{
    cluster_name = "cm-orchestrator-shared"
    spark_version = "13.3.x-scala2.12"  # Ajuste conforme necessário
    node_type_id = "Standard_DS3_v2"     # Ajuste conforme necessário
    num_workers = 2
    autotermination_minutes = 30
    data_security_mode = "USER_ISOLATION"  # <-- Modo compartilhado!
    spark_conf = @{
        "spark.databricks.cluster.profile" = "serverless"
        "spark.databricks.repl.allowedLanguages" = "python,sql"
    }
    custom_tags = @{
        "Project" = "cm-databricks"
        "Purpose" = "orchestrator"
        "Owner" = "reportflex.rodrigo@civilmaster.com.br"
    }
} | ConvertTo-Json -Depth 10

$createUrl = "$databricksHost/api/2.0/clusters/create"
try {
    $result = Invoke-RestMethod -Uri $createUrl -Method POST `
        -Headers @{
            "Authorization" = "Bearer $token"
            "Content-Type" = "application/json"
        } `
        -Body $clusterConfig
    
    Write-Host "✓ Cluster criado com sucesso!"
    Write-Host "`nCluster ID: $($result.cluster_id)"
    Write-Host "URL: $databricksHost/#setting/clusters/$($result.cluster_id)"
    
    Write-Host "`nPróximos passos:"
    Write-Host "1. Aguarde o cluster iniciar (pode levar 3-5 minutos)"
    Write-Host "2. Edite update-orchestrator-cluster.ps1 e substitua:"
    Write-Host "   `$newClusterId = `"$($result.cluster_id)`""
    Write-Host "3. Execute: .\update-orchestrator-cluster.ps1"
    
} catch {
    Write-Host "❌ Erro ao criar cluster: $_"
    Write-Host "`nDetalhes:"
    Write-Host $_.Exception.Message
}
