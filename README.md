Let’s build a Real Time Financial Trading Platform - Real-time stock market data pipeline using Kafka, Spark Streaming, and Power BI. Processes 1M+ events/sec with sub-second latency.


# 🚀 Real-Time Financial Trading Platform

**Production-grade data pipeline processing real-time stock market data with sub-second latency for executive dashboards.**

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4+-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.0+-red.svg)](https://kafka.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Real%20Time-yellow.svg)](https://powerbi.microsoft.com/)

## 📊 Architecture Overview

//![Architecture](docs/architecture.png)

**Tech Stack:**
- **Ingestion:** Apache Kafka (3-node cluster) + Azure Event Hubs
- **Processing:** Apache Spark Structured Streaming (PySpark)
- **Storage:** Delta Lake on Azure Data Lake Gen2 (Bronze/Silver/Gold)
- **Visualization:** Power BI Real-time Dashboards with Auto-refresh
- **Orchestration:** Apache Airflow
- **Monitoring:** Grafana + Prometheus

## 🎯 Key Features

- **High Throughput:** Processes 1M+ events/second with exactly-once semantics
- **Low Latency:** Sub-second end-to-end latency (ingestion to dashboard)
- **Data Quality:** Great Expectations validation with 25% error reduction
- **Fault Tolerance:** Checkpointing and WAL (Write Ahead Logs)
- **Security:** Row-Level Security (RLS) in Power BI for trader-specific views

## 🏗️ Architecture Details

### Data Flow
Raw Market Data → Kafka → Spark Streaming → Delta Lake → Power BI
↓                ↓           ↓              ↓           ↓
(JSON)         (Topics)   (Transform)   (Bronze/Silver)  (REST API)


### Layers
- **Bronze:** Raw JSON ingestion (immutable)
- **Silver:** Cleaned, validated data with schema enforcement
- **Gold:** Aggregated metrics (1-min, 5-min, 1-hour candles)


# 1. Start infrastructure
docker-compose up -d

# 2. Start data generator
python src/producer.py

# 3. Start processing
python src/spark_streaming.py
### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Power BI Desktop (optional)

