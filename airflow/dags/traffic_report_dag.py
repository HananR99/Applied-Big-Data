from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

POSTGRES_URL = os.getenv(
    "TRAFFIC_DB_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/traffic",
)
REPORT_DIR = "/opt/airflow/reports"


def generate_report(execution_date=None):
    # Import heavy deps inside task to avoid DAG import timeout
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    
    engine = create_engine(POSTGRES_URL)

    hourly_query = """
    WITH bounds AS (
        SELECT COALESCE(MAX(event_time), NOW()) AS latest_event_time
        FROM traffic_raw
    )
    SELECT
        sensor_id,
        date_trunc('hour', event_time) AS hour_bucket,
        SUM(vehicle_count) AS total_vehicles,
        AVG(avg_speed) AS avg_speed
    FROM traffic_raw, bounds
    WHERE event_time >= bounds.latest_event_time - INTERVAL '1 day'
    GROUP BY sensor_id, hour_bucket
    ORDER BY hour_bucket, sensor_id;
    """

    peak_query = """
    WITH bounds AS (
        SELECT COALESCE(MAX(event_time), NOW()) AS latest_event_time
        FROM traffic_raw
    ), hourly AS (
        SELECT
            sensor_id,
            date_trunc('hour', event_time) AS hour_bucket,
            SUM(vehicle_count) AS total_vehicles,
            AVG(avg_speed) AS avg_speed
        FROM traffic_raw, bounds
        WHERE event_time >= bounds.latest_event_time - INTERVAL '1 day'
        GROUP BY sensor_id, hour_bucket
    ), ranked AS (
        SELECT
            *,
            RANK() OVER (PARTITION BY sensor_id ORDER BY total_vehicles DESC) AS rnk
        FROM hourly
    )
    SELECT
        sensor_id,
        hour_bucket,
        total_vehicles,
        avg_speed
    FROM ranked
    WHERE rnk = 1
    ORDER BY sensor_id;
    """

    hourly_df = pd.read_sql(hourly_query, engine)
    peak_df = pd.read_sql(peak_query, engine)

    if hourly_df.empty or peak_df.empty:
        return

    hourly_df["hour_bucket"] = pd.to_datetime(hourly_df["hour_bucket"])
    peak_df["hour_bucket"] = pd.to_datetime(peak_df["hour_bucket"])
    peak_df["congestion_score"] = peak_df["total_vehicles"] / peak_df["avg_speed"]
    peak_df["congestion_rank"] = peak_df["congestion_score"].rank(
        method="dense",
        ascending=False,
    ).astype(int)
    peak_df["intervention_required"] = peak_df.apply(
        lambda row: "Yes" if row["congestion_rank"] == 1 or row["avg_speed"] < 15 else "Monitor",
        axis=1,
    )
    peak_df["recommendation"] = peak_df.apply(
        lambda row: (
            "Priority police deployment"
            if row["congestion_rank"] == 1
            else "Deploy if speeds drop below 15 km/h"
            if row["avg_speed"] < 15
            else "Continue monitoring"
        ),
        axis=1,
    )

    report_date = datetime.utcnow().strftime("%Y-%m-%d")
    os.makedirs(REPORT_DIR, exist_ok=True)

    csv_path = os.path.join(REPORT_DIR, f"traffic_report_{report_date}.csv")
    peak_df.to_csv(csv_path, index=False)

    fig = plt.figure(figsize=(11, 8.5))
    grid = fig.add_gridspec(2, 1, height_ratios=[2, 1])

    ax_chart = fig.add_subplot(grid[0])
    hourly_plot_df = (
        hourly_df.pivot_table(
            index="hour_bucket",
            columns="sensor_id",
            values="total_vehicles",
            aggfunc="sum",
        )
        .fillna(0)
        .sort_index()
    )

    x = np.arange(len(hourly_plot_df))
    sensor_ids = list(hourly_plot_df.columns)
    group_width = 0.8
    bar_width = group_width / max(len(sensor_ids), 1)
    start = -((group_width - bar_width) / 2)

    for idx, sensor_id in enumerate(sensor_ids):
        ax_chart.bar(
            x + start + idx * bar_width,
            hourly_plot_df[sensor_id].values,
            width=bar_width,
            label=sensor_id,
        )

    hour_labels = hourly_plot_df.index.strftime("%Y-%m-%d %H:%M")
    step = max(1, (len(hour_labels) + 11) // 12)
    tick_idx = x[::step]
    tick_labels = hour_labels[::step]
    ax_chart.set_xticks(tick_idx)
    ax_chart.set_xticklabels(tick_labels, rotation=30, ha="right")

    ax_chart.set_title("Traffic Volume vs. Time of Day (Last 24 Hours)")
    ax_chart.set_xlabel("Time of day")
    ax_chart.set_ylabel("Vehicle volume")
    ax_chart.grid(True, axis="y", alpha=0.3)
    ax_chart.legend(title="Junction")

    table_df = peak_df.copy()
    table_df["hour_bucket"] = table_df["hour_bucket"].dt.strftime("%Y-%m-%d %H:%M")
    table_df["avg_speed"] = table_df["avg_speed"].round(2)
    table_df["congestion_score"] = table_df["congestion_score"].round(2)

    ax_table = fig.add_subplot(grid[1])
    ax_table.axis("off")
    ax_table.set_title("Peak Traffic Hour and Intervention Recommendation", pad=12)
    table = ax_table.table(
        cellText=table_df.values,
        colLabels=table_df.columns,
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(7)
    table.scale(1, 1.3)

    report_path = os.path.join(REPORT_DIR, f"traffic_report_{report_date}.pdf")
    plt.tight_layout()
    plt.savefig(report_path)
    plt.close(fig)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="traffic_daily_report",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["traffic", "report"],
) as dag:
    generate = PythonOperator(
        task_id="generate_pdf_report",
        python_callable=generate_report,
    )

    generate
