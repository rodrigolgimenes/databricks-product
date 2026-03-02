# Install Databricks Library
# =========================
# Script para instalar bibliotecas em clusters Databricks via API

param(
    [Parameter(Mandatory=$true)]
    [string]$ClusterId,
    
    [Parameter(Mandatory=$true)]
    [string]$MavenCoordinates,
    
    [string]$Token = $env:DATABRICKS_TOKEN,
    [string]$Host = $env:DATABRICKS_HOST
)

# Validações
if (-not $Token) {
    Write-Host "❌ DATABRICKS_TOKEN não configurado" -ForegroundColor Red
    exit 1
}

if (-not $Host) {
    Write-Host "❌ DATABRICKS_HOST não configurado" -ForegroundColor Red
    exit 1
}

Write-Host "📦 Instalando biblioteca no Databricks..." -ForegroundColor Yellow
Write-Host "  Cluster: $ClusterId" -ForegroundColor Cyan
Write-Host "  Library: $MavenCoordinates" -ForegroundColor Cyan

$headers = @{
    "Authorization" = "Bearer $Token"
    "Content-Type" = "application/json"
}

$body = @{
    cluster_id = $ClusterId
    libraries = @(
        @{
            maven = @{
                coordinates = $MavenCoordinates
            }
        }
    )
} | ConvertTo-Json -Depth 10

try {
    $response = Invoke-RestMethod `
        -Uri "$Host/api/2.0/libraries/install" `
        -Method Post `
        -Headers $headers `
        -Body $body
    
    Write-Host "`n✅ Biblioteca instalada com sucesso!" -ForegroundColor Green
    Write-Host "⚠️  Reinicie o cluster para carregar a biblioteca" -ForegroundColor Yellow
    
} catch {
    Write-Host "`n❌ Erro ao instalar: $($_.Exception.Message)" -ForegroundColor Red
    
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $responseBody = $reader.ReadToEnd()
        Write-Host "Detalhes: $responseBody" -ForegroundColor Red
    }
    
    exit 1
}

# Exemplo de uso:
# .\install-databricks-library.ps1 `
#     -ClusterId "0207-135304-xe6bgxe8" `
#     -MavenCoordinates "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
