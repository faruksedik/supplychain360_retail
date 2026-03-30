
import boto3
import pandas as pd
import io

from datetime import datetime, timezone
from typing import List
from supplychain.utils import config
from botocore.exceptions import BotoCoreError
from supplychain.utils.logger import get_logger
from supplychain.utils import config


logger = get_logger(__name__)


def create_s3_client(profile_name: str) -> boto3.client:
    """
    Create an S3 client using a specific AWS CLI profile.

    Parameters
    ----------
    profile_name : str
        The AWS CLI profile name (configured with `aws configure --profile`).

    Returns
    -------
    boto3.client
        Configured S3 client.
    """
    try:
        logger.info("Creating S3 client for profile '%s'", profile_name)

        session = boto3.Session(profile_name=profile_name)
        s3_client = session.client("s3")

        logger.info("S3 client created successfully for profile '%s'", profile_name)
        return s3_client

    except BotoCoreError as e:
        logger.error("Failed to create S3 client for profile '%s': %s", profile_name, str(e))
        raise


def ingest_to_s3(
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    preserve_empty: bool = True
) -> List[str]:
    """
    Ingest all files from a folder (prefix) in a source S3 bucket into a 
    structured raw layer in a destination S3 bucket as Parquet files.

    The function automatically detects all files in the specified source folder, 
    converts them to Parquet format, and uploads them to the destination S3 bucket 
    while preserving the original filenames. 
    Files that have already been ingested (i.e., already exist in 
    the destination) are skipped.

    Destination structure:
        raw/<source_folder_name>/<original_file_name>.parquet
    Example:
        Source: landing/products/products_1.csv
        Destination: raw/products/products_1.parquet

    Parameters
    ----------
    source_bucket : str
        Name of the source S3 bucket containing the files to ingest.

    source_prefix : str
        Prefix (folder path) in the source bucket where the files are located.
        Example: 'landing/products/'.

    dest_bucket : str
        Name of the destination S3 bucket where the Parquet files will be stored.

    preserve_empty : bool, default True
        Whether to preserve empty rows or columns when reading CSV files.
        If False, empty values may be converted to NaN.

    Returns
    -------
    List[str]
        A list of S3 keys for the files that were successfully uploaded 
        to the destination bucket.

    Raises
    ------
    Exception
        Any exceptions during S3 connection, file download, file parsing, or upload 
        are logged and raised.
    """
    uploaded_files = []

    try:
        source_s3 = create_s3_client(profile_name=config.SOURCE_ACCOUNT_PROFILE_NAME)
        dest_s3 = create_s3_client(profile_name=config.DESTINATION_ACCOUNT_PROFILE_NAME)

        logger.info("Scanning folder s3://%s/%s", source_bucket, source_prefix)

        response = source_s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        objects = response.get("Contents", [])


        if not objects:
            logger.warning("No files found in %s", source_prefix)
            return uploaded_files

        # Extract folder name from the source prefix
        folder_name = source_prefix.rstrip("/").split("/")[-1]

        for obj in objects:
            source_key = obj["Key"]
            if source_key.endswith("/"):
                continue  # skip folder placeholders

            file_name = source_key.split("/")[-1]
            base_name = file_name.split(".")[0]

            # Destination key: raw/<folder_name>/<file_name>.parquet
            dest_key = f"raw/{folder_name}/{base_name}.parquet"

            # Skip already ingested files
            try:
                dest_s3.head_object(Bucket=dest_bucket, Key=dest_key)
                logger.info("Skipping already ingested file: %s", file_name)
                continue
            except:
                pass

            logger.info("Processing file: %s", source_key)

            # Download the source file
            buffer = io.BytesIO()
            source_s3.download_fileobj(source_bucket, source_key, buffer)
            buffer.seek(0)

            # Detect file type automatically based on extension
            if file_name.endswith(".csv"):
                df = pd.read_csv(
                    buffer,
                    keep_default_na=not preserve_empty,
                    parse_dates=False,
                    low_memory=False    # better type inference
                )
            
            elif file_name.endswith(".json"):
                df = pd.read_json(buffer, dtype=str)

            elif file_name.endswith(".parquet"):
                df = pd.read_parquet(buffer)
                
            else:
                logger.warning("Unsupported file type: %s", file_name)
                continue

            # Add ingestion timestamp
            df['ingestion_timestamp'] = datetime.now(timezone.utc)

            # Convert DataFrame to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Upload to destination S3
            dest_s3.upload_fileobj(parquet_buffer, dest_bucket, dest_key)
            logger.info("Uploaded %s to s3://%s/%s", file_name, dest_bucket, dest_key)

            uploaded_files.append(dest_key)

        return uploaded_files

    except Exception as e:
        logger.error("Failed to ingest data to S3: %s", str(e))
        raise