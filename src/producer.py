import os
import time
import logging
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from models import SensorReading

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC      = os.getenv("KAFKA_TOPIC",  "iot-sensors")
INTERVAL   = 1.0  # seconds between events


def on_success(metadata):
    log.info(f" sent to topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}")


def on_error(error: KafkaError):
    log.error(f" delivery failed: {error}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=3,
    )

    log.info(f"Producer started — publishing to '{TOPIC}' every {INTERVAL}s")

    try:
        while True:
            reading = SensorReading.generate()
            future  = producer.send(TOPIC, value=reading.to_json())
            future.add_callback(on_success)
            future.add_errback(on_error)
            log.info(f"→ {reading.sensor_id} @ {reading.location} | "
                     f"temp={reading.temperature}°C  "
                     f"humidity={reading.humidity}%  "
                     f"aqi={reading.air_quality}")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        log.info("Shutting down producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()