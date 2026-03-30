import datetime
import io
from pathlib import Path
import gspread
import pandas as pd

from datetime import datetime, timezone
from supplychain.utils import config
from supplychain.utils.logger import get_logger
from google.oauth2.service_account import Credentials
from botocore.exceptions import ClientError
from google.oauth2.service_account import Credentials
from supplychain.utils.s3_utils import create_s3_client

logger = get_logger(__name__)

def get_google_credentials(credentials_path: str) -> Credentials:
    """
    Load Google service account credentials.

    Parameters
    ----------
    credentials_path : str
        Absolute or relative path to the Google service account JSON file.

    Returns
    -------
    Credentials
        Authenticated Google API credentials.

    Raises
    ------
    FileNotFoundError
        If the credentials file does not exist.
    ValueError
        
    """

    # Resolve project root
    project_root = Path(__file__).resolve().parents[3]

    # Build absolute path
    full_path = project_root / credentials_path

    try:
        if not full_path.exists():
            raise FileNotFoundError(
                f"Google credentials file not found: {full_path}"
            )

        logger.info("Loading Google credentials from %s", full_path)

        
        credentials = Credentials.from_service_account_file(
            str(full_path),
            scopes=config.GOOGLE_API_SCOPES,
        )

        logger.info("Google credentials loaded successfully")

        return credentials

    except Exception as exc:
        logger.error("Failed to load Google credentials: %s", exc)
        raise



def ingest_google_sheet_to_s3(
    sheet_name: str,
    credentials: Credentials,
    dest_bucket: str,
    worksheet_name: str | None = None,
) -> str:
    """
    Retrieve data from a Google Sheet,
    convert the data to Parquet, and upload it to the Raw layer in S3.

    The destination path is automatically generated using the sheet name.

    Example
    -------
    If sheet_name = "store_locations"

    The uploaded file will be stored as:
        raw/store_locations/store_locations.parquet

    If the file already exists in the destination bucket, the ingestion
    is skipped to prevent duplicate uploads.

    Parameters
    ----------
    sheet_name : str
        Name of the Google Spreadsheet.

    credentials : Credentials
        Authenticated Google API credentials.

    dest_bucket : str
        Destination S3 bucket where the Parquet file will be stored.

    worksheet_name : str, optional
        Specific worksheet name inside the spreadsheet.
        If None, the first worksheet is used.

    Returns
    -------
    str
        The S3 key where the file exists or was uploaded.
    """

    try:
        logger.info("Connecting to Google Sheets")

        # Connect to Google Sheets
        client = gspread.authorize(credentials)
        spreadsheet = client.open(sheet_name)

        worksheet = (
            spreadsheet.worksheet(worksheet_name)
            if worksheet_name
            else spreadsheet.sheet1
        )

        logger.info("Fetching data from worksheet '%s'", worksheet.title)

        # Fetch records
        records = worksheet.get_all_records()

        if not records:
            raise ValueError(f"No data found in sheet '{sheet_name}'")

        # Create DataFrame
        df = pd.DataFrame(records)

        # Add Ingestion Timestamp
        df['ingestion_timestamp'] = datetime.now(timezone.utc)

        # Identify date columns and convert them to actual datetime objects
        for col in df.columns:
            if 'date' in col.lower() and col != 'ingestion_timestamp':
                logger.info("Normalizing date format for column: %s", col)
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                df[col] = df[col].dt.date

        # Convert to best possible dtypes (Pandas nullable types)
        df = df.convert_dtypes()

        # Convert dataframe to parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Clean sheet name for S3 path
        clean_name = sheet_name.replace(" ", "_").lower()

        # Destination key
        dest_key = f"raw/{clean_name}/{clean_name}.parquet"

        # Create S3 client
        s3_client = create_s3_client(
            profile_name=config.DESTINATION_ACCOUNT_PROFILE_NAME
        )

        logger.info("Checking if file already exists in s3://%s/%s", dest_bucket, dest_key)

        # Check if file already exists
        try:
            s3_client.head_object(Bucket=dest_bucket, Key=dest_key)

            logger.info(
                "File already exists. Skipping ingestion: s3://%s/%s",
                dest_bucket,
                dest_key,
            )

            return dest_key

        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        # Upload file if it does not exist
        logger.info("Uploading file to s3://%s/%s", dest_bucket, dest_key)

        s3_client.upload_fileobj(parquet_buffer, dest_bucket, dest_key)

        logger.info(
            "Successfully uploaded Google Sheet '%s' to s3://%s/%s",
            sheet_name,
            dest_bucket,
            dest_key,
        )

        return dest_key

    except Exception as exc:
        logger.error("Failed to ingest Google Sheet '%s': %s", sheet_name, exc)
        raise
