$ErrorActionPreference = "Stop"

Write-Host "Consuming alerts from critical_traffic topic..."
docker exec -it $(docker ps --filter "name=kafka" --format "{{.ID}}") `
  kafka-console-consumer `
  --bootstrap-server kafka:9092 `
  --topic critical_traffic `
  --from-beginning
