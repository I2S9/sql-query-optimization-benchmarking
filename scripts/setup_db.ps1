# Setup script for PostgreSQL database (PowerShell version)
# This script starts the PostgreSQL container and waits for it to be ready

Write-Host "Starting PostgreSQL container..." -ForegroundColor Green
docker-compose up -d db

Write-Host "Waiting for PostgreSQL to be ready..." -ForegroundColor Yellow
$timeout = 30
$counter = 0
$ready = $false

while (-not $ready -and $counter -lt $timeout) {
    $result = docker-compose exec -T db pg_isready -U benchmark 2>&1
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
    } else {
        Write-Host "Waiting for database... ($counter/$timeout)" -ForegroundColor Gray
        Start-Sleep -Seconds 1
        $counter++
    }
}

if (-not $ready) {
    Write-Host "Error: PostgreSQL did not become ready within $timeout seconds" -ForegroundColor Red
    exit 1
}

Write-Host "PostgreSQL is ready!" -ForegroundColor Green
Write-Host ""
Write-Host "Connection details:" -ForegroundColor Cyan
Write-Host "  Host: localhost"
Write-Host "  Port: 5432"
Write-Host "  Database: benchmark_db"
Write-Host "  User: benchmark"
Write-Host "  Password: benchmark"
Write-Host ""
Write-Host "To connect using psql:" -ForegroundColor Cyan
Write-Host "  docker-compose exec db psql -U benchmark -d benchmark_db"
Write-Host ""
Write-Host "To stop the database:" -ForegroundColor Cyan
Write-Host "  docker-compose down"

