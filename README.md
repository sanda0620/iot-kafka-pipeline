# IoT Real-Time Streaming Pipeline

A production-style real-time data pipeline built with:
- **Apache Kafka** — distributed message queue
- **Python** — producer and consumer services
- **PostgreSQL** — persistent storage
- **Grafana** — live monitoring dashboard

## Architecture
Simulated IoT sensors → Kafka topic → Python consumer → PostgreSQL → Grafana

## Stack
- Python 3.11
- kafka-python 2.3.2
- PostgreSQL 16
- Docker Compose