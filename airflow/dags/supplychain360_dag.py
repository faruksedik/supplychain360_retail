
from airflow.sdk import DAG
from pendulum import datetime, duration
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


from supplychain.ingestion.google_sheets.google_sheet_ingestion import ingest_store_locations_sheet
from supplychain.ingestion.postgres.sales_data_ingestion import ingest_postgres_sales_data
from supplychain.ingestion.s3.s3_ingestion import ingest_s3_bucket_data
from supplychain.ingestion.snowflake.snowflake_load import create_snowflake_tables, load_all_data_to_snowflake

default_args = {
    "owner": "Faruk",
    "retries": 3,
    "email": ["faruksedik@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retry_delay": duration(seconds=30)
}

with DAG(
    dag_id="supplychain360_dag",
    start_date=datetime(2026, 3, 14),
    default_args=default_args,
    schedule=None,
    catchup=False
):
    

    s3_ingestion_task = PythonOperator(
        task_id="s3_ingestion",
        python_callable=ingest_s3_bucket_data
    )

    ingest_store_location_task = PythonOperator(
        task_id="ingest_store_location",
        python_callable=ingest_store_locations_sheet
    )

    ingest_postgres_sales_data_task = PythonOperator(
        task_id="ingest_postgres_sales_data",
        python_callable=ingest_postgres_sales_data
    )

    create_snowflake_tables_task = PythonOperator(
        task_id="create_snowflake_tables",
        python_callable=create_snowflake_tables
    )

    load_data_to_snowflake_task = PythonOperator(
        task_id="load_data_to_snowflake",
        python_callable=load_all_data_to_snowflake
    )

    # bronze layer
    bronze_run = BashOperator(
        task_id="bronze_run",
        bash_command="""cd /opt/airflow/dbt/supplychain360 && 
        dbt run --select bronze --profiles-dir ."""
    )

    bronze_test = BashOperator(
        task_id="bronze_test",
        bash_command="""
            cd /opt/airflow/dbt/supplychain360 && \
            dbt test --select bronze --profiles-dir . --warn-error || true
        """,
        do_xcom_push=False
    )

    # Silver layer
    silver_run = BashOperator(
        task_id="silver_run",
        bash_command="""cd /opt/airflow/dbt/supplychain360 && 
        dbt run --select silver --profiles-dir ."""
    )

    silver_test = BashOperator(
        task_id="silver_test",
        bash_command="""cd /opt/airflow/dbt/supplychain360 && 
        dbt test --select silver --profiles-dir ."""
    )

    # Gold layer
    gold_run = BashOperator(
        task_id="gold_run",
        bash_command="""cd /opt/airflow/dbt/supplychain360 && 
        dbt run --select gold --profiles-dir ."""
    )

    gold_test = BashOperator(
        task_id="gold_test",
        bash_command="""cd /opt/airflow/dbt/supplychain360 && 
        dbt test --select gold --profiles-dir ."""
    )
        
    


    
    [s3_ingestion_task, ingest_store_location_task, ingest_postgres_sales_data_task] >> create_snowflake_tables_task >> load_data_to_snowflake_task
    
    load_data_to_snowflake_task >> bronze_run >> bronze_test >> silver_run >> silver_test >> gold_run >> gold_test

