# test-jobs-api.ps1 - Testes end-to-end da API de Jobs
# Uso: powershell -File tests/test-jobs-api.ps1

$BaseUrl = "http://localhost:3000/api/portal"
$pass = 0
$fail = 0
$jobId = $null

function Write-Pass($msg) { Write-Host "[PASS] $msg" -ForegroundColor Green; $script:pass++ }
function Write-Fail($msg) { Write-Host "[FAIL] $msg" -ForegroundColor Red; $script:fail++ }

Write-Host "`n========== JOBS API TEST SUITE ==========" -ForegroundColor Cyan

# 1. Health check
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/health" -Method GET
    if ($r.ok) { Write-Pass "Health Check" } else { Write-Fail "Health Check" }
} catch { Write-Fail "Health Check: $_" }

# 2. Create Job
try {
    $body = '{"job_name":"Test Auto ' + (Get-Date -Format "HHmmss") + '","description":"Teste automatizado","project_id":"mega","area_id":"glo","schedule_type":"DAILY","cron_expression":"30 08 * * *","timezone":"America/Sao_Paulo","dataset_ids":["851b5968-f628-42f7-a7ec-a80541db7274"],"max_concurrent_runs":1,"timeout_seconds":3600}'
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs" -Method POST -ContentType "application/json" -Body $body
    if ($r.ok) { Write-Pass "Create Job" } else { Write-Fail "Create Job: $($r.message)" }
    $jobId = $r.job_id
    Write-Host "  -> job_id: $jobId, databricks_job_id: $($r.databricks_job_id)"
} catch { Write-Fail "Create Job: $_" }

if (-not $jobId) {
    Write-Host "`n[ABORT] Job creation failed." -ForegroundColor Red
    exit 1
}

# 3. Get Job Details
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId" -Method GET
    if ($r.ok) { Write-Pass "Get Job Details" } else { Write-Fail "Get Job Details" }
    
    # 3b. Verify nested response
    if ($r.job -and $r.job.job_name) { Write-Pass "Nested job object has job_name" } else { Write-Fail "Nested job.job_name missing" }
    
    # 3c. Verify databricks sync
    if ($r.job -and $r.job.databricks_job_id) { Write-Pass "databricks_job_id is set (sync OK)" } else { Write-Fail "databricks_job_id is null" }
    
    # 3d. Verify datasets
    if ($r.datasets -and $r.datasets.Count -gt 0) { Write-Pass "Datasets returned ($($r.datasets.Count))" } else { Write-Fail "No datasets returned" }
} catch { Write-Fail "Get Job Details: $_" }

# 4. Edit Job (PATCH)
try {
    $body = '{"description":"Editado por teste","cron_expression":"15 10 * * *"}'
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId" -Method PATCH -ContentType "application/json" -Body $body
    if ($r.ok) { Write-Pass "Edit Job (PATCH)" } else { Write-Fail "Edit Job: $($r.message)" }
} catch { Write-Fail "Edit Job: $_" }

# 4b. Verify edit persisted
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId" -Method GET
    if ($r.job.cron_expression -eq "15 10 * * *") { Write-Pass "Edit persisted (cron updated)" } else { Write-Fail "Edit not persisted (cron=$($r.job.cron_expression))" }
} catch { Write-Fail "Verify edit: $_" }

# 5. Run Job Now
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId/run-now" -Method POST -ContentType "application/json"
    if ($r.ok -and $r.databricks_run_id) { Write-Pass "Run Job Now (run_id=$($r.databricks_run_id))" } else { Write-Fail "Run Job Now: $($r.message)" }
} catch { Write-Fail "Run Job Now: $_" }

# 6. Toggle disable
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId/toggle" -Method POST -ContentType "application/json"
    if ($r.ok -and $r.enabled -eq $false) { Write-Pass "Toggle disable" } else { Write-Fail "Toggle disable: enabled=$($r.enabled)" }
} catch { Write-Fail "Toggle disable: $_" }

# 7. Toggle re-enable
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId/toggle" -Method POST -ContentType "application/json"
    if ($r.ok -and $r.enabled -eq $true) { Write-Pass "Toggle re-enable" } else { Write-Fail "Toggle re-enable: enabled=$($r.enabled)" }
} catch { Write-Fail "Toggle re-enable: $_" }

# 8. Get Runs
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId/runs" -Method GET
    if ($r.ok) { Write-Pass "Get Job Runs" } else { Write-Fail "Get Job Runs" }
} catch { Write-Fail "Get Job Runs: $_" }

# 9. Sync Status
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/sync-status" -Method GET
    if ($r.ok) { Write-Pass "Sync Status" } else { Write-Fail "Sync Status" }
} catch { Write-Fail "Sync Status: $_" }

# 10. Delete Job
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId" -Method DELETE -ContentType "application/json"
    if ($r.ok) { Write-Pass "Delete Job" } else { Write-Fail "Delete Job: $($r.message)" }
} catch { Write-Fail "Delete Job: $_" }

# 10b. Verify 404 after delete
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/jobs/$jobId" -Method GET
    Write-Fail "Job still exists after delete"
} catch {
    Write-Pass "Job returns 404 after delete"
}

# Summary
Write-Host "`n========== RESULTS ==========" -ForegroundColor Cyan
Write-Host "PASSED: $pass" -ForegroundColor Green
if ($fail -gt 0) { Write-Host "FAILED: $fail" -ForegroundColor Red } else { Write-Host "FAILED: $fail" -ForegroundColor Green }
Write-Host "TOTAL:  $($pass + $fail)"

if ($fail -gt 0) { exit 1 } else { exit 0 }
