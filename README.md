# 🚀 SupplyChain360 Data Platform

A production-grade, end-to-end data engineering platform designed to unify fragmented supply chain data and enable real-time analytics for operational efficiency.

---

## 📌 Project Overview

SupplyChain360, a retail distribution company in the United States, faced critical supply chain challenges:

- Frequent stockouts of high-demand products
- Overstocking of slow-moving inventory
- Delayed shipments
- Lack of visibility into:
  - Supplier performance
  - Warehouse efficiency
  - Product demand trends

This project delivers a Unified Supply Chain Data Platform that centralizes data across multiple systems and enables reliable, scalable analytics.

---

# 🏗 Architecture Overview

![architecture diagram](<architecture diagram.png>)


---

# 🔧 Tech Stack

| Category | Tools |
|--------|------|
| Cloud Platform | AWS |
| Storage | S3 (Parquet) |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Programming | Python 3.11 |
| IaC | Terraform |
| Containerization | Docker |
| CI/CD | GitHub Actions |

---

## 🔄 Data Pipeline Flow

1. Data Extraction  
   - S3 (CSV, JSON)  
   - PostgreSQL  
   - Google Sheets  

2. Data Ingestion  
   - Convert to Parquet  
   - Store in S3  

3. Data Loading  
   - Load into Snowflake (Bronze)  

4. Transformation (dbt)  
   - Bronze → Silver → Gold  

5. Data Quality  
   - dbt tests  

6. Analytics Layer  
   - Ready for BI & reporting  

---

## ✨ Key Features

- Incremental processing   
- Idempotent pipelines  
- Data quality checks  
- Logging & monitoring  
- Retry mechanisms  
- API integrations  

---

# 📁 Project Structure

```
├── .github/                # CI/CD
│   ├── workflows/
|        └── main.yaml
├── airflow/                # Orchestration logic
│   ├── dags/               # Airflow DAGs (Ingestion, dbt, Quality)
│   └── docker-compose.yaml # Local development environment
├── dbt/supplychain360/     # Transformation Layer
│   ├── models/             # Bronze (Raw), Silver (Clean), Gold
│   ├── macros/             # DRY utility functions
│   └── profiles.yml        # Warehouse connection config
├── src/                    # Custom Python Library
│   ├── extractors/         # S3, Postgres, and GSheets API logic
│   └── utils/              # Logging and Metadata helpers
├── terraform/              # IaC for S3, IAM, and Warehouse
├── Dockerfile              # Multi-stage production build
├── requirements.txt        # Python dependencies
└── .dockerignore           # Build optimization
```

---

# 📊 Business Insights

The platform enables:

* 📦 Stockout Rate → Identify unreliable products
* 🚚 Supplier OTDR → Optimize supplier performance
* 🏭 Warehouse delays → Detect bottlenecks
* 🌍 Regional demand → Track revenue trends

---

---

# 🐳 Running the Project (Docker Hub)

## ✅ 1. Pull Image from Docker Hub

```
docker pull faruksedik/supplychain360:v1
```

---

## ✅ 2. Run with Docker Compose

Inside the `airflow/` folder:

Create a `.env` file with the following

```
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AWS_FOLDER=/c/Users/USER/.aws

SMTP_USER=example@gmail.com
SMTP_PASSWORD=

```
```
docker compose up airflow-init
docker compose up -d
```

---

## 🌐 Access Airflow UI

```
http://localhost:8080
```

---
---

# ⚠️ Environment Setup


### 🔑 dbt Profile (NOT in repo)

Create:

```
~/.dbt/profiles.yml
```

---
---

# 🏗 Infrastructure (Optional)

If provisioning infrastructure: Ensure to change the name of the s3 bucket

```
cd terraform
terraform init
terraform plan
terraform apply
```

---
---

# 🔄 CI/CD Pipeline

GitHub Actions automates:

### ✅ CI

* flake8 (linting)
* pytest (testing)

### 🚀 CD

* Build Docker image
* Push to Docker Hub

---

# 🔐 Security

Sensitive files are excluded:

* `.env`
* `profiles.yml`
* AWS credentials

In the src/supplychain/utils folder create another .env file:
```
# Snowflake Credentials
USER=
PASSWORD=
ACCOUNT=
WAREHOUSE=
DATABASE=
SCHEMA=
STAGE_NAME=
```

---

# 🛠 Engineering Highlights

* Incremental dbt models (cost optimization)
* Airflow retries (fault tolerance)
* Idempotent pipelines
* Parquet optimization
* Modular Python ingestion

---

# 🚀 Future Improvements

* AWS deployment (ECS/EKS)
* Monitoring (Prometheus + Grafana)
* Data observability

---

# 👨‍💻 Author

Faruk Sedik
Data Engineer | Backend Developer

---
