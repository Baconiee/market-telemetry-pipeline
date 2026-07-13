# Crypto Market Telemetry Ingestion Pipeline

An automated, containerized **ELT (Extract-Load-Transform)** data engineering pipeline that ingests real-time cryptocurrency metrics from the Binance API, manages database storage, executes analytical transformations, and integrates automated error monitoring.

> **Note:** This repository is a dedicated personal learning project built to master local batch processing infrastructure, container synchronization, and modern analytics engineering frameworks.

---

### Pipeline Workflow
1. **Extraction (Python / Requests):** A script connects to the public Binance API every 5 minutes to stream high-volume asset metrics (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`, `XRPUSDT`).
2. **Orchestration (Apache Airflow):** Manages the Directed Acyclic Graph (DAG), executing task dependencies, tracking historical state, and handling retries.
3. **Storage (PostgreSQL):** Serves as the relational data warehouse housing both raw landing tables and final analytics-ready views.
4. **Transformation & Testing (dbt):** Cleanses, deduplicates, and shapes raw metrics into high-performance reporting models while executing automated data quality checks (`unique`, `not_null`).
5. **Alerting (Python Webhooks):** Intercepts pipeline workflow crashes at the runtime level to generate automated incident alerts.

---

## 🛠️ Tech Stack & Key Concepts

* **Orchestration:** Apache Airflow (Dockerized, Standalone Execution Mode)
* **Data Transformation:** dbt (Data Build Tool) Core + `dbt-postgres`
* **Database:** PostgreSQL 16
---

## 🗂️ Project Directory Structure

```text
├── dags/
│   └── market_ingestion_dag.py        # Main Airflow DAG defining task graphs & alerting logic
├── market_transformations/            # Complete dbt Project Directory
│   ├── dbt_project.yml                # Core dbt configuration file
│   ├── models/
│   │   ├── staging/                   # Raw source layer mapping
│   │   └── analytical_views/          # Cleaned, deduplicated final database models
│   └── schema.yml                     # Data validation constraints (unique, not_null)
├── docker-compose.yml                 # Multi-container engine definition file
├── requirements.txt                   # Pipeline-wide environment dependencies
└── README.md                          # Project documentation
```