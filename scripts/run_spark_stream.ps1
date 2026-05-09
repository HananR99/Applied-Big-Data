$ErrorActionPreference = "Stop"

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.postgresql:postgresql:42.2.23"

Write-Host "Submitting Spark streaming job..."
$masterId = $(docker ps --filter "name=spark-master" --format "{{.ID}}")
$kafkaIp = $(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" project-kafka-1)
$postgresIp = $(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" project-postgres-1)

Write-Host "Kafka broker: ${kafkaIp}:9092"
Write-Host "Postgres JDBC: jdbc:postgresql://${postgresIp}:5432/traffic"

docker exec -it `
  -e KAFKA_BOOTSTRAP="${kafkaIp}:9092" `
  -e POSTGRES_JDBC_URL="jdbc:postgresql://${postgresIp}:5432/traffic" `
  $masterId `
  /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages $packages `
  /opt/spark-apps/traffic_stream.py
