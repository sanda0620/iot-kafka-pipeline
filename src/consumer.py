import os
import json
import logging
import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "iot-sensors")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST"),
    "port":     os.getenv("POSTGRES_PORT"),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

INSERT_SQL = """
    INSERT INTO sensor_readings
        (sensor_id, location, temperature, humidity, pressure, air_quality, timestamp)
    VALUES
        (%(sensor_id)s, %(location)s, %(temperature)s, %(humidity)s,
         %(pressure)s, %(air_quality)s, %(timestamp)s)
"""


def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def validate(data: dict) -> bool:
    required = {"sensor_id", "location", "temperature", "humidity",
                "pressure", "air_quality", "timestamp"}
    if not required.issubset(data.keys()):
        return False
    if not (-50 <= data["temperature"] <= 100):
        return False
    if not (0 <= data["humidity"] <= 100):
        return False
    return True


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="iot-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    conn   = get_db_connection()
    cursor = conn.cursor()

    log.info(f"Consumer started — listening on '{TOPIC}'")

    try:
        for message in consumer:
            data = message.value

            if not validate(data):
                log.warning(f" invalid reading dropped: {data}")
                continue

            cursor.execute(INSERT_SQL, data)
            conn.commit()

            log.info(
                f" partition={message.partition} offset={message.offset} | "
                f"{data['sensor_id']} @ {data['location']} | "
                f"temp={data['temperature']}°C"
            )

    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()