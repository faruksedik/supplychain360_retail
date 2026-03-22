from supplychain.utils import config
from supplychain.utils.postgres_utils import orchestrate_sales_ingestion


def ingest_postgres_sales_data() -> None:
    """
    Run the sales data ingestion pipeline.

    This function orchestrates the ingestion of daily sales transaction tables
    from the source PostgreSQL database into the raw layer of the destination
    S3 data lake.

    The pipeline performs the following steps:
        1. Retrieves PostgreSQL database credentials from AWS SSM Parameter Store.
        2. Connects to the source PostgreSQL database.
        3. Identifies all tables in the specified schema that match the
           naming pattern 'sales_%'.
        4. Checks the destination S3 bucket for already ingested sales tables.
        5. Determines which tables have not yet been ingested.
        6. Extracts the missing tables from PostgreSQL.
        7. Converts the data to Parquet format.
        8. Uploads the Parquet files to the destination S3 raw layer.

    The ingested files are stored in the following structure:

        raw/sales/<table_name>.parquet

    Example:
        raw/sales/sales_2026_03_13.parquet

    This function is designed to be triggered by workflow orchestration tools
    such as Apache Airflow.

    Raises
    ------
    Exception
        Propagates any exception raised during the ingestion process.
    """

    orchestrate_sales_ingestion(
        source_profile_name=config.SOURCE_ACCOUNT_PROFILE_NAME,
        profile_account_region=config.SOURCE_ACCOUNT_PROFILE_REGION,
        destination_profile_name=config.DESTINATION_ACCOUNT_PROFILE_NAME,
        destination_s3_bucket=config.DESTINATION_S3_BUCKET_NAME
    )