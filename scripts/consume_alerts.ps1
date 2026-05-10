$ErrorActionPreference = "Stop"

Write-Host "Consuming alerts from critical_traffic topic..."
docker compose exec `
  kafka `
  kafka-console-consumer `
  --bootstrap-server kafka:9092 `
  --topic critical_traffic `
  --from-beginning
