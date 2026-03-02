# Script para rodar ambiente de desenvolvimento
# Backend na porta 3001, Frontend (Vite) na porta 3000

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CM Databricks - Ambiente de Dev" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar se está na pasta correta
if (-not (Test-Path "server.js")) {
    Write-Host "❌ Erro: Execute este script na pasta raiz do projeto (C:\dev\cm-databricks)" -ForegroundColor Red
    exit 1
}

Write-Host "🚀 Iniciando Backend (porta 3001)..." -ForegroundColor Green
Write-Host "🚀 Iniciando Frontend (porta 3000)..." -ForegroundColor Green
Write-Host ""
Write-Host "📌 Acesse: http://localhost:3000" -ForegroundColor Yellow
Write-Host "📌 API Backend: http://localhost:3001" -ForegroundColor Yellow
Write-Host ""
Write-Host "⚠️  Pressione Ctrl+C para parar os servidores" -ForegroundColor Yellow
Write-Host ""

# Criar jobs em background
$backendJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    $env:PORT = 3001
    node server.js
}

$frontendJob = Start-Job -ScriptBlock {
    Set-Location "$using:PWD\frontend"
    npm run dev
}

# Aguardar inicialização
Start-Sleep -Seconds 3

# Mostrar logs em tempo real
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Logs dos Servidores" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

try {
    while ($true) {
        # Backend logs
        $backendOutput = Receive-Job -Job $backendJob
        if ($backendOutput) {
            Write-Host "[BACKEND] $backendOutput" -ForegroundColor Blue
        }

        # Frontend logs
        $frontendOutput = Receive-Job -Job $frontendJob
        if ($frontendOutput) {
            Write-Host "[FRONTEND] $frontendOutput" -ForegroundColor Magenta
        }

        Start-Sleep -Milliseconds 500
    }
}
finally {
    Write-Host ""
    Write-Host "🛑 Parando servidores..." -ForegroundColor Yellow
    Stop-Job -Job $backendJob, $frontendJob
    Remove-Job -Job $backendJob, $frontendJob
    Write-Host "✅ Servidores parados" -ForegroundColor Green
}
