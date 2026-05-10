$ErrorActionPreference = "Stop"

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.postgresql:postgresql:42.2.23"
$kafkaBootstrap = "kafka:9092"
$postgresJdbc = "jdbc:postgresql://postgres:5432/traffic"

Write-Host "Submitting Spark streaming job..."
Write-Host "Kafka broker: ${kafkaBootstrap}"
Write-Host "Postgres JDBC: ${postgresJdbc}"

docker compose exec `
  -e KAFKA_BOOTSTRAP="${kafkaBootstrap}" `
  -e POSTGRES_JDBC_URL="${postgresJdbc}" `
  spark-master `
  /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages $packages `
  /opt/spark-apps/traffic_stream.py
