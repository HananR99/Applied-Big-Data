# Smart City Traffic & Congestion Pipeline

This repository implements Scenario 1, the Smart City Traffic & Congestion System, for the Applied Big Data Engineering mini project.

The system simulates traffic sensors at four Colombo junctions. Sensor events are ingested through Kafka, processed using Spark Structured Streaming, stored in PostgreSQL, and used by Airflow to generate a scheduled analytical report.

## Architecture

```text
Traffic Sensor Producer
        |
        v
Kafka Topic: traffic_raw
        |
        v
Spark Structured Streaming
        |----------------------------|
        v                            v
PostgreSQL                    Kafka Topic: critical_traffic
traffic_raw                   real-time low-speed alerts
traffic_window_agg
        |
        v
Airflow DAG
        |
        v
PDF/CSV Traffic Report
```

Architecture diagram source: [docs/architecture.mmd](docs/architecture.mmd)

## Technology Stack

- **Apache Kafka**: real-time ingestion using topics and partitions.
- **Apache Spark Structured Streaming**: event-time stream processing, watermarking, 5-minute tumbling windows, congestion calculation, and alert detection.
- **PostgreSQL**: persistent storage for raw events and windowed stream aggregates.
- **Apache Airflow**: orchestration for scheduled analytical report generation.
- **Docker Compose**: local environment for Kafka, Spark, PostgreSQL, and Airflow.

## Repository Structure

```text
airflow/dags/traffic_report_dag.py   Airflow DAG for report generation
airflow/requirements.txt             Airflow Python dependencies
db/init.sql                          PostgreSQL initialization script
docs/architecture.mmd                Mermaid architecture diagram
producers/traffic_producer.py        Synthetic traffic sensor producer
producers/requirements.txt           Producer Python dependencies
reports/                             Generated PDF and CSV reports
scripts/consume_alerts.ps1           Kafka alert consumer helper script
scripts/run_spark_stream.ps1         Spark job submission helper script
spark/traffic_stream.py              Spark Structured Streaming application
docker-compose.yml                   Docker Compose stack definition
```

## Prerequisites

The following software is required:

- Docker Desktop
- Python 3.10 or later
- PowerShell
- Git, if the repository is cloned from a remote source

Ensure Docker Desktop is running before executing Docker commands.

## Setup And Execution

Run all commands from the repository root.

### 1. Start The Docker Stack

```powershell
docker compose up -d
```

Wait until the containers are initialized. This may take a few minutes during the first run.

Create and verify the Kafka topics:

```powershell
docker compose up -d kafka-init
```

Available service endpoints:

```text
Kafka external listener: localhost:29092
PostgreSQL:              localhost:5432
Spark Master UI:         http://localhost:8081
Spark Worker UI:         http://localhost:8083
Airflow UI:              http://localhost:8085
```

### 2. Install Producer Dependencies

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r producers\requirements.txt
```

### 3. Start Spark Streaming

Open a PowerShell terminal in the repository root and run:

```powershell
scripts\run_spark_stream.ps1
```

Keep this terminal open while the pipeline is running.

Spark performs the following tasks:

- Reads JSON events from Kafka topic `traffic_raw`.
- Converts producer timestamps into event time.
- Applies a 10-minute watermark.
- Calculates 5-minute tumbling window aggregates.
- Writes raw events to PostgreSQL table `traffic_raw`.
- Writes windowed aggregates to PostgreSQL table `traffic_window_agg`.
- Writes low-speed alerts to Kafka topic `critical_traffic`.

### 4. Start The Traffic Producer

Open a second PowerShell terminal in the repository root:

```powershell
.venv\Scripts\activate
python producers\traffic_producer.py
```

The producer emits one event per junction per second.

Example producer output:

```text
[NORMAL] 2026-05-09T08:48:16.211809+00:00 junction_02 vehicles=15 avg_speed=31.51 km/h
[CRITICAL] 2026-05-09T08:48:15.111429+00:00 junction_04 vehicles=103 avg_speed=7.32 km/h
```

Allow the producer and Spark job to run for several minutes before generating the report.

### 5. Consume Critical Traffic Alerts

This step is optional, but it can be used to verify the real-time alert path.

Open another PowerShell terminal:

```powershell
scripts\consume_alerts.ps1
```

Alerts are produced when:

```text
avg_speed < 10 km/h
```

### 6. Generate The Analytical Report

Open the Airflow UI:

```text
http://localhost:8085
```

Login credentials:

```text
Username: airflow
Password: airflow
```

Trigger the DAG:

```text
traffic_daily_report
```

The DAG is scheduled for `23:00` daily. For assessment execution, it can also be triggered manually from the Airflow UI.

## Final Output

Generated reports are saved in:

```text
reports/
```

Expected report files:

```text
traffic_report_YYYY-MM-DD.pdf
traffic_report_YYYY-MM-DD.csv
```

The PDF report includes:

- Traffic Volume vs. Time of Day chart
- Peak traffic hour for each junction
- Congestion score
- Congestion rank
- Recommended junction for next-day traffic police intervention

The CSV report contains the same recommendation table in tabular format.

## Verification Commands

Check running containers:

```powershell
docker ps
```

Verify Kafka topic partitions:

```powershell
docker exec project-kafka-1 kafka-topics --bootstrap-server 172.28.0.3:9092 --describe --topic traffic_raw
docker exec project-kafka-1 kafka-topics --bootstrap-server 172.28.0.3:9092 --describe --topic critical_traffic
```

Both topics should show:

```text
PartitionCount: 4
```

Check PostgreSQL table row counts:

```powershell
docker exec project-postgres-1 psql -U postgres -d traffic -c "select 'traffic_raw' as table_name, count(*) from traffic_raw union all select 'traffic_window_agg', count(*) from traffic_window_agg;"
```

Check latest ingested event:

```powershell
docker exec project-postgres-1 psql -U postgres -d traffic -c "select count(*) as raw_events, max(event_time) as latest_event from traffic_raw;"
```

Verify Airflow DAG availability:

```powershell
docker exec project-airflow-1 airflow dags list | findstr traffic_daily_report
```

## Data Schema

Kafka topic `traffic_raw` receives JSON events in the following format:

```json
{
  "sensor_id": "junction_01",
  "timestamp": "2026-05-09T08:48:16.211809+00:00",
  "vehicle_count": 15,
  "avg_speed": 31.51
}
```

PostgreSQL tables:

- `traffic_raw`: stores individual traffic sensor events.
- `traffic_window_agg`: stores 5-minute Spark window aggregates.

Kafka topics:

- `traffic_raw`: raw traffic events, 4 partitions.
- `critical_traffic`: low-speed alert events, 4 partitions.

## Event Time And Processing Time

Event time is derived from the producer-generated `timestamp` field. Spark converts this field into `event_time` and applies:

```text
withWatermark("event_time", "10 minutes")
```

The 5-minute tumbling window aggregation is based on event time. This ensures that analytics represent the time at which the traffic event occurred, rather than only the time at which Spark processed the event.

## Congestion And Alert Logic

Spark calculates the following stream aggregates:

```text
total_vehicles = sum(vehicle_count)
avg_speed = average(avg_speed)
congestion_index = total_vehicles / avg_speed
```

Immediate alert condition:

```text
avg_speed < 10 km/h
```

Airflow report ranking:

```text
congestion_score = total_vehicles / avg_speed
```

The junction with the highest congestion score is recommended for next-day traffic police intervention.

## Troubleshooting

If Airflow is unavailable:

```powershell
docker compose restart airflow airflow-scheduler
```

If Spark is not processing data:

1. Confirm the producer is running.
2. Confirm `scripts\run_spark_stream.ps1` is still running.
3. Check PostgreSQL row counts using the verification command above.

If Docker networking or service discovery fails, restart the stack:

```powershell
docker compose down
docker compose up -d
docker compose up -d kafka-init
```

Then start Spark and the producer again.

If the report contains no data, allow the producer and Spark streaming job to run for several minutes, then trigger the Airflow DAG again.

## Stopping The System

Stop the producer and Spark terminals with:

```text
Ctrl+C
```

Stop Docker services:

```powershell
docker compose down
```

## Assessment Artifacts

The following artifacts are included or produced during execution:

- Source code for producer, Spark streaming, and Airflow DAG.
- Docker Compose stack.
- Architecture diagram source file.
- PostgreSQL schema initialization script.
- Generated PDF and CSV analytical reports in `reports/`.

Recommended execution evidence:

- Docker containers running.
- Producer logs showing normal and critical traffic events.
- Kafka topics with 4 partitions.
- Spark UI showing the streaming job.
- PostgreSQL row counts.
- Airflow DAG run success.
- Generated PDF report.
