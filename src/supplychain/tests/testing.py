



from supplychain.ingestion.google_sheets.google_sheet_ingestion import ingest_store_locations_sheet
from supplychain.ingestion.postgres.sales_data_ingestion import ingest_postgres_sales_data
from supplychain.ingestion.s3.s3_ingestion import ingest_s3_bucket_data
from supplychain.ingestion.snowflake.snowflake_load import create_snowflake_tables, load_all_data_to_snowflake
from supplychain.utils.snowflake_utils import connect_to_snowflake, copy_parquet_folder, run_snowflake_load




# run_sales_ingestion()

# load_products()

# ingest_store_locations_sheet()

# ingest_products()

# ingest_s3_bucket_data()

# load_all_data_to_snowflake()

# create_snowflake_tables()

# ingest_store_locations_sheet()
