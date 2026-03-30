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
├── airflow/                     # Orchestration logic
│   ├── dags/                    # Airflow DAGs (Ingestion, dbt, Quality)
│   └── docker-compose.yaml      # Local development environment
    └── docker-compose.prod.yaml # For running container in another pc
├── dbt/supplychain360/          # Transformation Layer
│   ├── models/                  # Bronze (Raw), Silver (Clean), Gold
│   ├── macros/                  # DRY utility functions
│   └── profiles.yml             # Warehouse connection config
├── src/                         # Custom Python Library
│   ├── extractors/              # S3, Postgres, and GSheets API logic
│   └── utils/                   # Logging and Metadata helpers
├── terraform/                   # IaC for S3, IAM, and Warehouse
├── Dockerfile                   # Multi-stage production build
├── requirements.txt             # Python dependencies
└── .dockerignore                # Build optimization
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

This guide explains how to run the pipeline on a new machine using the pre-built Docker image. In this mode, the DAGs, dbt models, and Python source code are already **"baked"** into the container for maximum reliability and consistent environments.

### 1. Prerequisites
* **Docker Desktop** installed and running.
* **Git** installed.
* A **Snowflake** account and **AWS** credentials.

### 2. Clone the Repository
Open your terminal and clone the project structure to your local machine:
```bash
git clone https://github.com/faruksedik/supplychain360_retail.git
cd supplychain360/airflow
```
## 3. Configure Secrets (Manual Setup)

Since security credentials are excluded from version control, you must
create these two files manually in the cloned directory:

### A. Create the `.env` file

Create a file named `.env` inside the `airflow/` folder:

``` bash
# Airflow UI Credentials
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_UID=50000

# Path to your AWS credentials folder on your PC
# Example: /home/user/.aws or C:/Users/Name/.aws
AWS_FOLDER=/path/to/your/.aws

# SMTP Settings for Email Alerts
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_gmail_app_password
```

### B. Create the `profiles.yml` file

Navigate to `dbt/supplychain360/` and create your Snowflake
profile:

``` yaml
# profiles.yml
supplychain360:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account_id
      user: your_username
      password: your_snowflake_password
      role: accountadmin
      warehouse: compute_wh
      database: supplychain_db
      schema: gold
      threads: 1
```

## 4. Pull and Launch

Run the following commands from the `airflow/` directory to download the
image and start the services:

``` bash
# Pull the image from Docker Hub
docker compose -f docker-compose.prod.yml pull

# Start the pipeline in the background
docker compose -f docker-compose.prod.yml up -d
```

## 5. Verify the Deployment

Once the containers are running, verify that the "baked" code is present
and the scheduler is active:

-   Access the UI: Go to http://localhost:8080

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

This project utilizes **GitHub Actions** to automate the full lifecycle of the data platform, from SQL validation to container deployment.

### 🧪 Stage 1: Snowflake & dbt Validation (CI)
* **Environment Setup:** dynamic creation of `profiles.yml` using GitHub Secrets for secure Snowflake connectivity.
* **Dependency Management:** Automated installation of `dbt-snowflake` and project dependencies via `dbt deps`.
* **Pre-Deployment Check:** Executes `dbt debug` and `dbt compile` to ensure all models, macros, and connection strings are syntactically correct before merging.

### 🐳 Stage 2: Docker Build & Push (CD)
* **Condition:** This stage only triggers if the Snowflake validation passes successfully.
* **Containerization:** Packages the application logic into a Docker image using the `v1` tag.
* **Registry Deployment:** Authenticates with **Docker Hub** and pushes the image to the central repository for deployment readiness.

### 📧 Stage 3: Automated Monitoring
* **Status Reporting:** A dedicated notification job that runs `if: always()`, ensuring the team is alerted regardless of success or failure.
* **SMTP Integration:** Sends a detailed email report including the commit SHA, repository link, and direct access to the GitHub Actions log for rapid debugging.

---

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
