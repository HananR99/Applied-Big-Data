import os
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "traffic_raw"

SENSORS = [
    "junction_01",
    "junction_02",
    "junction_03",
    "junction_04",
]

random.seed(42)


def make_event(sensor_id: str) -> dict:
    now = datetime.now(timezone.utc)

    # Base traffic
    vehicle_count = random.randint(5, 60)
    avg_speed = random.uniform(12, 55)

    # Inject occasional critical congestion
    if random.random() < 0.05:
        avg_speed = random.uniform(3, 9)
        vehicle_count = random.randint(60, 120)

    return {
        "sensor_id": sensor_id,
        "timestamp": now.isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": round(avg_speed, 2),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        while True:
            for sensor in SENSORS:
                event = make_event(sensor)
                producer.send(TOPIC, value=event)
                status = "CRITICAL" if event["avg_speed"] < 10 else "NORMAL"
                print(
                    f"[{status}] {event['timestamp']} {event['sensor_id']} "
                    f"vehicles={event['vehicle_count']} avg_speed={event['avg_speed']} km/h",
                    flush=True,
                )
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()


if __name__ == "__main__":
    main()
