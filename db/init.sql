CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\c airflow
ALTER SCHEMA public OWNER TO airflow;
GRANT ALL ON SCHEMA public TO airflow;

\c traffic

CREATE TABLE IF NOT EXISTS traffic_raw (
  id BIGSERIAL PRIMARY KEY,
  sensor_id TEXT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  vehicle_count INTEGER NOT NULL,
  avg_speed DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS traffic_window_agg (
  id BIGSERIAL PRIMARY KEY,
  sensor_id TEXT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  avg_speed DOUBLE PRECISION NOT NULL,
  total_vehicles INTEGER NOT NULL,
  congestion_index DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traffic_raw_event_time
  ON traffic_raw (event_time);

CREATE INDEX IF NOT EXISTS idx_traffic_raw_sensor_time
  ON traffic_raw (sensor_id, event_time);

CREATE INDEX IF NOT EXISTS idx_traffic_window_sensor_start
  ON traffic_window_agg (sensor_id, window_start);
