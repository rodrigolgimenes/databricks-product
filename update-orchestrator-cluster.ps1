# Script para atualizar o cluster do job orchestrator
# Substitua NEW_CLUSTER_ID com o ID do seu cluster

$databricksHost = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
$token = $env:DATABRICKS_TOKEN
$jobId = "690887429046802"

# Novo cluster (substitua com o ID do cluster que você criou)
$newClusterId = "SEU_CLUSTER_ID_AQUI"  # Ex: "0220-112345-abcdef12"

Write-Host "Atualizando configuração do job..."

# 1. Buscar configuração atual do job
$getUrl = "$databricksHost/api/2.1/jobs/get?job_id=$jobId"
try {
    $jobConfig = Invoke-RestMethod -Uri $getUrl -Method GET `
        -Headers @{
            "Authorization" = "Bearer $token"
        }
    
    Write-Host "✓ Configuração atual obtida"
    Write-Host "  Cluster atual: $($jobConfig.settings.tasks[0].existing_cluster_id)"
    
} catch {
    Write-Host "❌ Erro ao buscar config: $_"
    exit 1
}

# 2. Atualizar configuração com novo cluster
$updateBody = @{
    job_id = [int]$jobId
    new_settings = @{
        tasks = @(
            @{
                task_key = $jobConfig.settings.tasks[0].task_key
                notebook_task = $jobConfig.settings.tasks[0].notebook_task
                existing_cluster_id = $newClusterId  # <-- TROCAR AQUI
                timeout_seconds = $jobConfig.settings.tasks[0].timeout_seconds
                max_retries = $jobConfig.settings.tasks[0].max_retries
                min_retry_interval_millis = $jobConfig.settings.tasks[0].min_retry_interval_millis
                retry_on_timeout = $jobConfig.settings.tasks[0].retry_on_timeout
            }
        )
        format = "MULTI_TASK"
        name = $jobConfig.settings.name
        max_concurrent_runs = $jobConfig.settings.max_concurrent_runs
        timeout_seconds = $jobConfig.settings.timeout_seconds
    }
} | ConvertTo-Json -Depth 10

Write-Host "`nAtualizando para cluster: $newClusterId"

$updateUrl = "$databricksHost/api/2.1/jobs/update"
try {
    Invoke-RestMethod -Uri $updateUrl -Method POST `
        -Headers @{
            "Authorization" = "Bearer $token"
            "Content-Type" = "application/json"
        } `
        -Body $updateBody
    
    Write-Host "✓ Job atualizado com sucesso!"
    Write-Host "`nPróximos passos:"
    Write-Host "1. Vá no Databricks e verifique: $databricksHost/jobs/$jobId"
    Write-Host "2. Clique em 'Run now' para testar"
    
} catch {
    Write-Host "❌ Erro ao atualizar: $_"
    Write-Host "`nDetalhes:"
    Write-Host $_.Exception.Message
}
