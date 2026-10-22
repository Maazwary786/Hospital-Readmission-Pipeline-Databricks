$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $ProjectRoot

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "=== $Text ===" -ForegroundColor Cyan
}

function Wait-ForHttp {
    param(
        [string]$Name,
        [string]$Uri,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                Write-Host "$Name is ready ($Uri)" -ForegroundColor Green
                return
            }
        } catch {
            Start-Sleep -Seconds 3
        }
    }

    throw "$Name did not become ready at $Uri within $TimeoutSeconds seconds."
}

Write-Host "HOSPITAL READMISSION PROJECT - LIVE PRESENTATION" -ForegroundColor Magenta
Write-Host "Project: $ProjectRoot" -ForegroundColor DarkGray

Write-Section "Starting Airflow and Spark"
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed. Check that Docker Desktop is running."
}

Write-Section "Starting Kafka and the dashboard"
docker start kafka
if ($LASTEXITCODE -ne 0) {
    throw "Could not start the existing Kafka container."
}
docker start hospital-readmission-dashboard-current
if ($LASTEXITCODE -ne 0) {
    throw "Could not start the dashboard container."
}

$kafkaReady = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list *> $null
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $kafkaReady) {
    throw "Kafka did not become ready at localhost:9092."
}

Write-Section "Setting and verifying the local Airflow login"
$airflowPassword = "HospitalDemo-" + [Guid]::NewGuid().ToString("N").Substring(0, 20) + "!"
docker compose exec -T airflow airflow users reset-password --username admin --password $airflowPassword
if ($LASTEXITCODE -ne 0) {
    throw "Could not reset the Airflow admin password."
}

$passwordCheck = "import os; from airflow.www.app import create_app; from werkzeug.security import check_password_hash; app=create_app(testing=True); user=app.appbuilder.sm.find_user(username='admin'); valid=bool(user and check_password_hash(user.password, os.environ['AIRFLOW_DEMO_PASSWORD'])); print('Airflow admin password:', 'VERIFIED' if valid else 'FAILED'); assert valid"
docker compose exec -T -e "AIRFLOW_DEMO_PASSWORD=$airflowPassword" airflow python -c $passwordCheck
if ($LASTEXITCODE -ne 0) {
    throw "Airflow rejected the configured demo password."
}

docker compose exec -T airflow airflow dags unpause healthcare_patient_vitals_master_dag
if ($LASTEXITCODE -ne 0) {
    throw "Could not unpause the project Airflow DAG."
}

Write-Section "Starting a real Spark demo job"
Write-Host "Run spark-demo.cmd in a second terminal after the Spark master and worker are visible." -ForegroundColor Yellow

Wait-ForHttp -Name "Airflow webserver" -Uri "http://localhost:8080/health"
Wait-ForHttp -Name "Spark master UI" -Uri "http://localhost:8081/json/"
Wait-ForHttp -Name "Spark worker UI" -Uri "http://localhost:8082"
Wait-ForHttp -Name "Healthcare dashboard" -Uri "http://localhost:5000/"

Write-Section "Opening the presentation pages"
$pages = @(
    "http://localhost:8080/dags/healthcare_patient_vitals_master_dag/grid",
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:5000"
)
foreach ($page in $pages) {
    Start-Process $page
}

Write-Host ""
Write-Host "AIRFLOW LOGIN" -ForegroundColor Yellow
Write-Host "URL:      http://localhost:8080" -ForegroundColor White
Write-Host "Username: admin" -ForegroundColor White
Write-Host "Password: $airflowPassword" -ForegroundColor White
Write-Host "Open healthcare_patient_vitals_master_dag, then choose Graph/Grid or Trigger DAG." -ForegroundColor White

Write-Host ""
Write-Host "SPARK" -ForegroundColor Yellow
Write-Host "Master UI:      http://localhost:8081" -ForegroundColor White
Write-Host "Worker UI:      http://localhost:8082" -ForegroundColor White
Write-Host "Application UI: http://localhost:4040 (open after starting spark-demo.cmd)" -ForegroundColor White
Write-Host "Run spark-demo.cmd in a second terminal to show a real Spark job for three minutes." -ForegroundColor White

Write-Host ""
Write-Host "Dashboard: http://localhost:5000 | Kafka broker: localhost:9092 (ready)" -ForegroundColor DarkCyan
Write-Host "Airflow/Spark are bound to localhost only; the demo password is for this local presentation setup." -ForegroundColor DarkGray
Write-Host ""
docker compose ps
