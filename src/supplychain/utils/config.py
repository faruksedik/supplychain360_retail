import os
from dotenv import load_dotenv

load_dotenv()

# AWS
SOURCE_ACCOUNT_PROFILE_NAME = "source-account"
SOURCE_ACCOUNT_PROFILE_REGION = "eu-west-2"
SOURCE_S3_BUCKET_NAME = "supplychain360-data"
DESTINATION_ACCOUNT_PROFILE_NAME = "destination-account"
DESTINATION_S3_BUCKET_NAME = "faruk-supplychain360-bucket"
# DESTINATION_S3_BUCKET_NAME = "faruk-data-lake-bucket"

SOURCE_BUCKET_FOLDERS = {
    "products": "raw/products",
    "warehouses": "raw/warehouses",
    "suppliers": "raw/suppliers",
    "shipments": "raw/shipments",
    "inventory": "raw/inventory"
}

# PostgreSQL SSM parameter paths
POSTGRES_CREDENTIALS = {
    "host": "/supplychain360/db/host",
    "port": "/supplychain360/db/port",
    "database": "/supplychain360/db/dbname",
    "user": "/supplychain360/db/user",
    "password": "/supplychain360/db/password"
}


# Google Sheets
GOOGLE_SHEET_NAME = "store_locations"
GOOGLE_SERVICE_ACCOUNT_PATH = "src/supplychain/credentials/google_service_account.json"
GOOGLE_API_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


# Snowflake
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")
WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
STAGE_NAME = os.getenv("STAGE_NAME")

target_tables = {
    "products": "products",
    "warehouses": "warehouses",
    "suppliers": "suppliers",
    "shipments": "shipments",
    "store_locations": "store_locations",
    "inventories": "inventories",
    "sales": "sales"
}

folders = {
    "products": "products",
    "warehouses": "warehouses",
    "suppliers": "suppliers",
    "shipments": "shipments",
    "inventory": "inventory",
    "sales": "sales",
    "store_locations": "store_locations"
}

