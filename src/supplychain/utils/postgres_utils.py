import io
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
from typing import Dict, List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from supplychain.utils.logger import get_logger
from supplychain.utils.config import POSTGRES_CREDENTIALS
from supplychain.utils.s3_utils import create_s3_client


logger = get_logger(__name__)


def create_ssm_client(profile_name: str, profile_account_region: str ) -> boto3.client:
    """
    Create an AWS Systems Manager (SSM) client using a specific AWS CLI profile.

    Parameters
    ----------
    profile_name : str
        The AWS CLI profile name (configured with `aws configure --profile`).
    
    profile_account_region : str
        The AWS CLI Source account region where the ssm credentials are created

    Returns
    -------
    boto3.client
        Configured SSM client.
    """
    try:
        logger.info("Creating SSM client for profile '%s'", profile_name)

        session = boto3.Session(profile_name=profile_name)
        ssm_client = session.client("ssm", profile_account_region)

        logger.info("SSM client created successfully for profile '%s'", profile_name)
        return ssm_client

    except BotoCoreError as e:
        logger.error(
            "Failed to create SSM client for profile '%s': %s",
            profile_name,
            str(e)
        )
        raise



def get_ssm_parameter(
    ssm_client,
    parameter_name: str,
    with_decryption: bool = True
) -> str:
    """
    Retrieve a parameter value from AWS Systems Manager Parameter Store.

    Parameters
    ----------
    ssm_client : boto3.client
        Configured SSM client.
    parameter_name : str
        Name of the parameter in AWS SSM Parameter Store.
    with_decryption : bool, default=True
        Whether to decrypt the parameter value. Set to True for SecureString parameters.

    Returns
    -------
    str
        The parameter value.

    Raises
    ------
    ClientError
        If the parameter does not exist or access is denied.
    BotoCoreError
        If boto3 encounters an AWS communication error.
    """
    try:
        logger.info("Fetching SSM parameter '%s'", parameter_name)

        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=with_decryption
        )

        parameter_value = response["Parameter"]["Value"]

        logger.info("Successfully fetched SSM parameter '%s'", parameter_name)
        return parameter_value

    except (ClientError, BotoCoreError) as e:
        logger.error(
            "Failed to fetch SSM parameter '%s': %s",
            parameter_name,
            str(e)
        )
        raise


def get_postgres_credentials(ssm_client) -> dict:
    """
    Retrieve PostgreSQL connection credentials from AWS SSM Parameter Store.

    This function reads the parameter paths defined in `config.POSTGRES_CREDENTIALS`
    and returns a dictionary containing the host, port, database, username, and password.

    Parameters
    ----------
    ssm_client : boto3.client
        A configured AWS SSM client.

    Returns
    -------
    dict
        PostgreSQL connection credentials with keys: 'host', 'port', 'database', 'user', 'password'.

    Raises
    ------
    Exception
        If any parameter cannot be retrieved from SSM.
    """
    try:
        logger.info("Fetching PostgreSQL credentials from SSM Parameter Store")

        credentials = {}
        for key, param_path in POSTGRES_CREDENTIALS.items():
            # All parameters are stored as plain String in SSM
            credentials[key] = get_ssm_parameter(ssm_client, param_path)

        logger.info("PostgreSQL credentials fetched successfully")
        return credentials

    except Exception as e:
        logger.error("Failed to fetch PostgreSQL credentials: %s", str(e))
        raise



def create_postgres_engine(credentials: Dict[str, str]) -> Engine:
    """Creates a SQLAlchemy engine for connecting to a PostgreSQL database.

    This function constructs a connection URI from a dictionary of credentials
    and initializes a SQLAlchemy Engine object used for executing SQL queries
    and managing database interactions.

    Args:
        credentials (Dict[str, str]): A dictionary containing connection details.
            Must include the keys: 'user', 'password', 'host', 'port', 'database'.

    Returns:
        Engine: A SQLAlchemy Engine instance configured for the specified database.

    Raises:
        KeyError: If any required key is missing from the credentials dictionary.
        Exception: If the engine initialization fails due to invalid parameters or 
            connectivity issues.
    """
    try:
        # Extract components to build the connection URI
        user = credentials["user"]
        password = credentials["password"]
        host = credentials["host"]
        port = credentials["port"]
        dbname = credentials["database"]

        logger.info(
            "Initializing SQLAlchemy engine for database '%s' at %s:%s", 
            dbname, host, port
        )

        # Connection URI format: postgresql+psycopg2://user:password@host:port/dbname
        connection_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        
        engine = create_engine(connection_uri)
        
        logger.info("SQLAlchemy engine for '%s' created successfully", dbname)
        return engine

    except KeyError as e:
        logger.error("Missing required connection credential: %s", str(e))
        raise
    except Exception as e:
        logger.error(
            "Failed to initialize SQLAlchemy engine for host '%s': %s", 
            credentials.get("host", "unknown"), 
            str(e)
        )
        raise



def get_sales_tables_from_db(engine: Engine, schema: str = "public") -> List[str]:
    """Queries the database information schema for tables starting with 'sales_'.

    Args:
        engine (Engine): An active SQLAlchemy engine.
        schema (str): The database schema to search. Defaults to 'public'.

    Returns:
        List[str]: A list of matching table names, sorted alphabetically.

    Raises:
        Exception: If the information schema query fails.
    """
    try:
        # Use a parameterized query for safety and wrap it in text()
        query = text("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = :schema 
              AND table_name LIKE 'sales_%'
            ORDER BY table_name;
        """)
        
        logger.info("Scanning schema '%s' for tables matching 'sales_%%'", schema)
        
        # Pass the parameters as a dictionary to pd.read_sql
        df = pd.read_sql(query, engine, params={"schema": schema})
        table_list = df["table_name"].tolist()
        
        logger.info("Successfully discovered %d sales tables", len(table_list))
        return table_list

    except Exception as e:
        logger.error("Failed to retrieve sales tables from schema '%s': %s", schema, str(e))
        raise


def get_ingested_tables_from_s3(
    s3_client: boto3.client, 
    bucket: str, 
    prefix: str = "raw/sales/"
) -> List[str]:
    """Retrieves a list of table names already present in the S3 bucket.

    This function uses an S3 paginator to handle buckets with a large number 
    of objects, extracting the base table name from the Parquet file keys.

    Args:
        s3_client (boto3.client): An initialized S3 client.
        bucket (str): The name of the S3 bucket to scan.
        prefix (str): The S3 prefix (folder path) where files are stored. 
            Defaults to 'raw/sales/'.

    Returns:
        List[str]: A list of table names (e.g., 'sales_2026_01_01') that have 
            already been ingested, excluding the file extension.

    Raises:
        ClientError: If there is an issue accessing the S3 bucket or prefix.
        Exception: For any other unexpected errors during pagination.
    """
    try:
        logger.info("Scanning s3://%s/%s for already ingested tables", bucket, prefix)
        
        paginator = s3_client.get_paginator("list_objects_v2")
        ingested_tables = []

        # Paginate through S3 objects to handle more than 1,000 files
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            
            if not contents:
                logger.info("No existing objects found in s3://%s/%s", bucket, prefix)
                continue

            for obj in contents:
                key = obj["Key"]
                # Skip the prefix itself if it appears as an object
                if key == prefix:
                    continue
                
                # Extract filename from path and remove .parquet extension
                # Example: 'raw/sales/sales_2026.parquet' -> 'sales_2026'
                file_name = key.split("/")[-1]
                if file_name.endswith(".parquet"):
                    table_name = file_name.replace(".parquet", "")
                    ingested_tables.append(table_name)

        logger.info("Found %d already ingested tables in S3", len(ingested_tables))
        return ingested_tables

    except (ClientError, BotoCoreError) as e:
        logger.error("AWS error while listing S3 objects in bucket '%s': %s", bucket, str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error listing S3 objects: %s", str(e))
        raise



def fetch_table_as_dataframe(engine: Engine, table_name: str, schema: str = "public") -> pd.DataFrame:
    """Reads an entire database table into a pandas DataFrame.

    Args:
        engine (Engine): An active SQLAlchemy engine used to connect to the database.
        table_name (str): The name of the table to fetch.
        schema (str): The schema containing the table. Defaults to 'public'.

    Returns:
        pd.DataFrame: A DataFrame containing all records from the specified table.

    Raises:
        Exception: If the query execution fails or the table cannot be reached.
    """
    try:
        query = text(f'SELECT * FROM "{schema}"."{table_name}"')
        logger.info("Fetching all records from '%s.%s'", schema, table_name)
        
        df = pd.read_sql(query, engine)

        df = df.astype(str) 

        logger.info("Successfully fetched %d rows from '%s.%s'", len(df), schema, table_name)
        return df

    except Exception as e:
        logger.error(
            "Failed to fetch table '%s.%s' from database: %s", 
            schema, table_name, str(e)
        )
        raise



def upload_dataframe_to_s3(df: pd.DataFrame, bucket: str, key: str, s3_client: boto3.client) -> str:
    """Converts a DataFrame to Parquet and uploads it to an S3 bucket.

    Args:
        df (pd.DataFrame): The DataFrame to be uploaded.
        bucket (str): The destination S3 bucket name.
        key (str): The S3 object key (path).
        s3_client (boto3.client): An initialized S3 client.

    Returns:
        str: The S3 key of the uploaded object.
    """
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket, key)
        logger.info("Successfully uploaded parquet to s3://%s/%s", bucket, key)
        return key
    except Exception as e:
        logger.error("Failed to upload parquet to S3: %s", str(e))
        raise



def orchestrate_sales_ingestion(
    source_profile_name: str,
    profile_account_region: str,
    destination_profile_name: str,
    destination_s3_bucket: str,
    destination_s3_prefix: str = "raw/sales/"
):
    """Orchestrates the transfer of new sales tables from PostgreSQL to S3.

    This function identifies tables in the source database that do not yet 
    exist in the destination S3 bucket, fetches them as DataFrames, 
    converts them to Parquet, and uploads them.

    Args:
        source_profile (str): AWS CLI profile name for SSM and DB credentials.
        region (str): AWS region where SSM parameters are located.
        dest_profile (str): AWS CLI profile name for the destination S3 bucket.
        bucket (str): Name of the destination S3 bucket.
        prefix (str): S3 folder path for the Parquet files. Defaults to 'raw/sales/'.

    Raises:
        Exception: If any step in the ingestion pipeline fails.
    """
    try:
        # 1. Initialize AWS Clients
        ssm = create_ssm_client(source_profile_name, profile_account_region)
        s3 = create_s3_client(destination_profile_name)

        # 2. Setup Database Engine
        db_creds = get_postgres_credentials(ssm)
        engine = create_postgres_engine(db_creds)

        # 3. Discovery: Get tables from DB and S3
        db_tables = get_sales_tables_from_db(engine)
        
        # This returns a list of names like ['sales_2026_01_01', 'sales_2026_01_02']
        ingested_table_names = get_ingested_tables_from_s3(s3, destination_s3_bucket, destination_s3_prefix)

        # 4. Filter for New Tables
        # We compare the raw table name from DB against the cleaned list from S3
        new_tables = [t for t in db_tables if t not in ingested_table_names]

        if not new_tables:
            logger.info("No new tables found in database. Ingestion is up to date.")
            return

        logger.info("Found %d new tables to ingest.", len(new_tables))

        # 5. Execution Loop
        for table_name in new_tables:
            try:
                logger.info("Starting ingestion for table: %s", table_name)
                
                # Fetch data
                df = fetch_table_as_dataframe(engine, table_name)
                
                # Construct destination key (adding the extension here for the upload)
                dest_key = f"{destination_s3_prefix}{table_name}.parquet"
                
                # Upload to S3
                upload_dataframe_to_s3(df, destination_s3_bucket, dest_key, s3)
                
                logger.info("Successfully completed ingestion for %s", table_name)
                
            except Exception as table_error:
                # Log the specific table error but continue with other tables
                logger.error("Failed to process table '%s': %s", table_name, str(table_error))
                continue

        logger.info("Sales ingestion orchestration completed successfully.")

    except Exception as e:
        logger.error("Critical failure in sales ingestion orchestration: %s", str(e))
        raise