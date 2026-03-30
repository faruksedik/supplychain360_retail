# 1. Base Image
FROM apache/airflow:3.1.3

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Bake application folders
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/
COPY --chown=airflow:root airflow/ /opt/airflow/airflow/

# Symlinks for Airflow standard paths
RUN ln -s /opt/airflow/airflow/dags /opt/airflow/dags && \
    ln -s /opt/airflow/airflow/config /opt/airflow/config

ENV PYTHONPATH="/opt/airflow:/opt/airflow/src"
ENV AIRFLOW__CORE__DAGS_FOLDER="/opt/airflow/dags"
ENV AIRFLOW_CONFIG="/opt/airflow/config/airflow.cfg"