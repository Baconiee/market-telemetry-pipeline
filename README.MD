# Crypto Market Telemetry Ingestion Pipeline

An automated, containerized **ELT (Extract-Load-Transform)** data engineering pipeline that ingests real-time cryptocurrency metrics from the Binance API, manages database storage, executes analytical transformations, and integrates automated error monitoring.

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

🚀 Getting Started
Prerequisites

    Docker & Docker Compose installed on your system.

    Python 3.11+ (for local development/testing).

```
Installation & Initialization

    Clone the repository:
    ```bash
    git clone [https://github.com/yourusername/crypto-market-telemetry.git](https://github.com/yourusername/crypto-market-telemetry.git)
    cd crypto-market-telemetry
    ```

    **Configure Environment Variables:**
    Copy the provided environment template file to create your local active configuration file:
    ```bash
    cp .env.example .env
    ```

    Boot the Container Environment:
    Run the stack in detached mode. This single command builds the network, mounts local volumes, boots the services, and automatically injects database configurations directly into Airflow's environment memory:
    Bash

    docker-compose up -d

    Retrieve Airflow Admin Password:
    Grab your automatically generated local admin web server password by running:
    Bash

    docker exec -it compose_market_airflow cat /opt/airflow/standalone_admin_password.txt

    Access the Orchestrator Interface:
    Open your browser and navigate to http://localhost:8081. Log in using the username admin and the password retrieved from the step above. Turn on the market_telemetry_ingestion DAG—the system is fully configured and ready to run.