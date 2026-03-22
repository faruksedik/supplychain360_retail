# 📦 SupplyChain360: Unified Data Lakehouse & Analytics Platform

## 📖 Business Scenario

SupplyChain360 is a fast-growing retail distributor facing:

* Frequent stockouts
* Overstocked inventory
* Delivery delays

These issues were caused by **fragmented data sources**:

* AWS S3 (Logistics)
* Google Sheets (Stores)
* PostgreSQL (Sales)

### 🎯 Solution

This project delivers a **production-grade unified data platform** that centralizes all data into a single analytics system, enabling:

* Data-driven decisions
* Supplier performance optimization
* Reduced operational costs

---

# 🏗 Architecture Overview

Medallion Architecture:

Bronze → Silver → Gold

---

# 🔧 Tech Stack

| Layer            | Tool           |
| ---------------- | -------------- |
| Orchestration    | Apache Airflow |
| Data Warehouse   | Snowflake      |
| Transformation   | dbt Core       |
| Infrastructure   | Terraform      |
| Containerization | Docker         |
| CI/CD            | GitHub Actions |

---

# 📁 Project Structure

```
.
├── airflow/                # DAGs and orchestration
├── dbt/                    # Transformation models
├── src/                    # Python ingestion logic
├── terraform/              # Infrastructure as Code
├── Dockerfile
├── requirements.txt
└── .dockerignore
```

---

# 📊 Business Insights

The platform enables:

* 📦 Stockout Rate → Identify unreliable products
* 🚚 Supplier OTDR → Optimize supplier performance
* 🏭 Warehouse delays → Detect bottlenecks
* 🌍 Regional demand → Track revenue trends

---

# 🔄 Data Pipeline

## 🟤 Bronze

* Raw ingestion (S3, GSheets, PostgreSQL)
* Stored as Parquet
* Metadata added

## ⚪ Silver

* Cleaning & standardization
* Deduplication

## 🟡 Gold

* Star schema:

**Fact Tables**

* fact_sales
* fact_inventory_snapshots

**Dimension Tables**

* dim_products
* dim_stores
* dim_suppliers
* dim_warehouses

---

# 🐳 Running the Project (Docker Hub)

## ✅ 1. Pull Image from Docker Hub

```
docker pull faruksedik/supplychain360:latest
```

---

## ✅ 2. Run with Docker Compose

Inside the `airflow/` folder:

```
docker compose up airflow-init
docker compose up -d
```

---

## 🌐 Access Airflow

```
http://localhost:8080
```

---

# ⚠️ Environment Setup

---

### 🔑 dbt Profile (NOT in repo)

Create:

```
~/.dbt/profiles.yml
```

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
* Real-time streaming (Kafka)

---

# 👨‍💻 Author

Faruk Sedik
Data Engineer | Backend Developer

---
