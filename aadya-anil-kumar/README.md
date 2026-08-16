# Medallion Architecture Pipeline
### By Aadya Anil Kumar 

---

## Overview
This project implements a full Medallion Architecture (Bronze → Silver → Gold) using two data pipelines:

- **Real-time pipeline** — Apache NiFi ingests live weather data from OpenWeather API into ClickHouse every 60 seconds
- **Batch pipeline** — Apache Airflow ingests NYC Yellow Taxi data (Parquet), loads it into ClickHouse, then dbt transforms it through Silver and Gold layers

---

## Architecture
Real-time (NiFi):
OpenWeather API → NiFi → bronze_weather (ClickHouse)

Batch (Airflow + dbt):
NYC Taxi Parquet → Airflow → bronze_nyc_taxi → dbt → silver_nyc_taxi → gold_nyc_taxi_daily

---

## Tech Stack
| Tool | Purpose |
|------|---------|
| Apache NiFi 1.23.2 | Real-time data ingestion |
| Apache Airflow 2.9.3 | Batch pipeline orchestration |
| dbt (ClickHouse adapter) | Data transformation |
| ClickHouse 23.8 | Analytical data warehouse |
| Docker + Docker Compose | Containerized environment |
| Python + Pandas + PyArrow | Data processing |
| OpenWeather API | Real-time weather source |
| NYC Taxi Open Data | Batch data source |

---

## Data Sources

### Real-time — OpenWeather API
- **Type:** REST API (JSON)
- **Frequency:** Every 60 seconds
- **Location:** Jakarta, Indonesia
- **Target table:** `medallion.bronze_weather`

### Batch — NYC Yellow Taxi Trip Data
- **Type:** Parquet file
- **Period:** January 2024
- **Rows:** ~5.9 million
- **Source:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Target table:** `medallion.bronze_nyc_taxi`

---

## Medallion Layers

| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| Bronze | bronze_weather | grows ~1/min | Raw weather data from NiFi |
| Bronze | bronze_nyc_taxi | 5,929,248 | Raw taxi trips from Parquet |
| Silver | silver_nyc_taxi | 5,447,576 | Cleaned trips with duration |
| Gold | gold_nyc_taxi_daily | 31 | Daily KPIs for January 2024 |

---

## Project Structure
```
glynac-aadya-medallion-pipeline/
│
├── aadya-anil-kumar/
│   ├── docker-compose.yml
│   ├── .env.sample
│   └── .env
│
├── realtime/
│   └── weather/
│       └── flow.json
│
├── batch/
│   └── nyc-taxi/
│       ├── dags/
│       │   └── nyc_taxi_ingest.py
│       └── dbt/
│           ├── dbt_project.yml
│           ├── profiles.yml
│           └── models/
│               ├── silver/
│               │   └── silver_nyc_taxi.sql
│               └── gold/
│                   └── gold_nyc_taxi_daily.sql
│
├── clickhouse/
│   └── init.sql
│
├── data/
│   └── yellow_tripdata_2024-01.parquet
│
└── README.md


```
---

## Prerequisites
- Docker Desktop installed and running
- OpenWeather API key (free at openweathermap.org)
- NYC Taxi Parquet file in `data/` folder

---

## Setup & Running

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/glynac-aadya-medallion-pipeline.git
cd glynac-aadya-medallion-pipeline
```

### 2. Configure environment
```bash
cd aadya-anil-kumar
cp .env.sample .env
```
Edit `.env` and add your OpenWeather API key.

### 3. Create .env file
```bash
[System.IO.File]::WriteAllText(".env", "AIRFLOW_UID=50000`nAIRFLOW_PROJ_DIR=../../..`nOPENWEATHER_API_KEY=your_key_here`n", [System.Text.Encoding]::UTF8)
```

### 4. Start all services
```bash
cd aadya-anil-kumar
docker compose up -d
```

Wait 2-3 minutes for all services to initialize.

### 5. Initialize ClickHouse tables
```bash
docker exec -i aadya-anil-kumar-clickhouse-1 clickhouse-client --multiquery < clickhouse/init.sql
```

### 6. Access services
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| NiFi | https://localhost:8443/nifi | admin / adminadminadmin |
| ClickHouse | http://localhost:8123/ping | — |

---

## Running the Batch Pipeline

### 1. Trigger the Airflow DAG
1. Open http://localhost:8080
2. Find `nyc_taxi_ingest` DAG
3. Toggle ON → click ▶ to trigger

### 2. DAG Task Flow
file_check → validate_schema → load_bronze → dbt_run

### 3. Verify data
```bash
docker exec -it aadya-anil-kumar-clickhouse-1 clickhouse-client --query \
"SELECT 'bronze' as layer, COUNT(*) FROM medallion.bronze_nyc_taxi
UNION ALL SELECT 'silver', COUNT(*) FROM medallion.silver_nyc_taxi
UNION ALL SELECT 'gold', COUNT(*) FROM medallion.gold_nyc_taxi_daily"
```

---

## Running the Real-time Pipeline

### 1. Import NiFi flow
1. Open https://localhost:8443/nifi
2. Right click canvas → **Upload template** → select `realtime/weather/flow.json`
3. Drag the template onto canvas
4. Start all processors

### 2. Verify data flowing
```bash
docker exec -it aadya-anil-kumar-clickhouse-1 clickhouse-client --query \
"SELECT COUNT(*) FROM medallion.bronze_weather"
```
Count should increase every 60 seconds.

---

## Stopping the Environment
```bash
docker compose down
```

---

## Troubleshooting

**Airflow webserver not starting**
```bash
docker exec -it aadya-anil-kumar-airflow-webserver-1 bash -c "rm -f /opt/airflow/airflow-webserver.pid"
docker restart aadya-anil-kumar-airflow-webserver-1
```

**ClickHouse tables missing after restart**
```bash
docker exec -i aadya-anil-kumar-clickhouse-1 clickhouse-client --multiquery < clickhouse/init.sql
```

**NiFi flow empty after restart**
Import `realtime/weather/flow.json` via NiFi UI → right click canvas → Upload template.

**Worker crashed**
```bash
docker restart aadya-anil-kumar-airflow-worker-1
```

**Permission error on startup**
```bash
echo "AIRFLOW_UID=50000" >> .env
docker compose down
docker compose up -d
```
