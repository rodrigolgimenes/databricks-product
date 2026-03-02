# Script para reiniciar o orchestrator
# Substitua com seu token

$databricksHost = "https://dbc-c9eab3b3-1f5f.cloud.databricks.com"
$token = $env:DATABRICKS_TOKEN
$jobId = "690887429046802"

# Cancelar run atual (se houver)
Write-Host "Cancelando runs ativas..."
$cancelUrl = "$databricksHost/api/2.1/jobs/runs/cancel-all"
$cancelBody = @{
    job_id = [int]$jobId
} | ConvertTo-Json

try {
    Invoke-RestMethod -Uri $cancelUrl -Method POST `
        -Headers @{
            "Authorization" = "Bearer $token"
            "Content-Type" = "application/json"
        } `
        -Body $cancelBody
    Write-Host "✓ Runs canceladas"
} catch {
    Write-Host "⚠ Nenhuma run ativa para cancelar: $_"
}

# Aguardar 5 segundos
Start-Sleep -Seconds 5

# Iniciar nova run
Write-Host "Iniciando nova run do orchestrator..."
$runUrl = "$databricksHost/api/2.1/jobs/run-now"
$runBody = @{
    job_id = [int]$jobId
} | ConvertTo-Json

try {
    $result = Invoke-RestMethod -Uri $runUrl -Method POST `
        -Headers @{
            "Authorization" = "Bearer $token"
            "Content-Type" = "application/json"
        } `
        -Body $runBody
    
    Write-Host "✓ Orchestrator reiniciado!"
    Write-Host "Run ID: $($result.run_id)"
    Write-Host "URL: $databricksHost/jobs/$jobId/runs/$($result.run_id)"
} catch {
    Write-Host "❌ Erro ao iniciar: $_"
}
