# Smart City Traffic & Congestion Pipeline

Mini project for Applied Big Data Engineering. The pipeline simulates traffic sensors, streams events through Kafka, processes them in Spark, stores results in PostgreSQL, and generates a daily report with Airflow.

## What This Does

- Simulates four junction sensors (1 event/sec/junction).
- Kafka ingests raw traffic events.
- Spark Structured Streaming aggregates 5-minute windows and emits alerts.
- PostgreSQL stores raw events and window aggregates.
- Airflow generates PDF/CSV daily reports.

Architecture diagram: architecture_diagram.png

## Tech Stack

- Kafka, Spark Structured Streaming, PostgreSQL, Airflow, Docker Compose

## How To Run (Quick)

Run all commands from the repository root.

1. Start containers

```powershell
docker compose up -d
docker compose up -d kafka-init
```

2. Install producer deps

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r producers\requirements.txt
```

3. Start Spark streaming

```powershell
scripts\run_spark_stream.ps1
```

4. Start the producer

```powershell
.venv\Scripts\activate
python producers\traffic_producer.py
```

5. (Optional) Consume alerts

```powershell
scripts\consume_alerts.ps1
```

6. Generate report

- Airflow UI: http://localhost:8085
- Login: airflow / airflow
- Trigger DAG: `traffic_daily_report`

## Output

Reports are written to:

```text
reports/
```

Expected files:

```text
traffic_report_YYYY-MM-DD.pdf
traffic_report_YYYY-MM-DD.csv
```

## Key Logic (Short)

- Windowing: 5-minute tumbling windows with 10-minute watermark
- Congestion score: total_vehicles / avg_speed
- Alert condition: avg_speed < 10 km/h
