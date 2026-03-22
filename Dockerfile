# 1. Base Image
FROM apache/airflow:3.1.3

USER root
# 2. System dependencies (needed for dbt and psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 3. Install Dependencies
# This requirements.txt should have: dbt-core, dbt-redshift/snowflake, pandas, etc.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 4. Bake the Code (This is what gets pushed to the Registry)
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/

# 5. Set the Path so 'import supplychain' works automatically
ENV PYTHONPATH="/opt/airflow:/opt/airflow/src"