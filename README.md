# IoT Real-Time Streaming Pipeline

A production-style real-time data pipeline that simulates IoT sensor networks,
streams events through Apache Kafka, persists data in PostgreSQL, and visualises
live metrics in Grafana — all containerised with Docker Compose.

## Dashboard Preview

![IoT Sensor Pipeline Dashboard](docs/dashboard.png)

## Architecture

```
IoT Sensors (simulated)
       │
       ▼
Python Producer  →  Apache Kafka (topic: iot-sensors, 3 partitions)
                              │
                              ▼
                    Python Consumer (validates + processes)
                              │
                              ▼
                         PostgreSQL  →  Grafana (live dashboard, 5s refresh)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message queue | Apache Kafka 7.5 + Zookeeper |
| Data ingestion | Python 3.11, kafka-python |
| Storage | PostgreSQL 16 |
| Visualisation | Grafana 10.2 |
| Infrastructure | Docker Compose |

## Features

- Simulates 5 IoT sensors across 5 locations (temperature, humidity, pressure, air quality)
- Producer publishes 1 event/second into a 3-partition Kafka topic
- Consumer validates readings and persists to PostgreSQL with end-to-end latency tracking
- Grafana dashboard auto-refreshes every 5 seconds with live time-series and aggregation panels
- Fully containerised — entire stack starts with a single `docker compose up -d`

## Project Structure

```
iot-kafka-pipeline/
├── src/
│   ├── models.py        # Sensor data model and reading generator
│   ├── producer.py      # Kafka producer — simulates live sensor stream
│   └── consumer.py      # Kafka consumer — validates and persists data
├── docker-compose.yml   # Full infrastructure definition
├── init.sql             # Database schema (auto-runs on first start)
├── start.sh             # One-command setup script
├── requirements.txt     # Python dependencies
└── README.md
```

## Key Concepts Demonstrated

- **Event streaming** — decoupled producer/consumer architecture via Kafka
- **Partitioning** — 3-partition topic enabling parallel consumption
- **Consumer groups** — offset tracking for fault-tolerant message processing
- **Data validation** — out-of-range readings dropped before storage
- **Infrastructure as code** — entire stack defined in docker-compose.yml
- **Observability** — live Grafana dashboard with time-series and aggregation queries
- **End-to-end latency tracking** — separate `ingested_at` and `timestamp` columns