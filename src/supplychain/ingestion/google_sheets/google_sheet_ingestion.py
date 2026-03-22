

from supplychain.utils import config
from supplychain.utils.google_sheet_utils import get_google_credentials, ingest_google_sheet_to_s3


def ingest_store_locations_sheet() -> str:
    """
    Load Google service account credentials and ingest the configured
    Google Sheet into the S3 raw layer.

    The sheet is retrieved using the configured sheet name and uploaded
    to the destination S3 bucket as a Parquet file under:

        raw/<sheet_name>/<sheet_name>.parquet

    Returns
    -------
    str
        The S3 key of the uploaded Parquet file.
    """

    # Load Google credentials
    creds_path = config.GOOGLE_SERVICE_ACCOUNT_PATH
    google_creds = get_google_credentials(creds_path)

    # Ingest sheet to S3
    uploaded_key = ingest_google_sheet_to_s3(
        sheet_name=config.GOOGLE_SHEET_NAME,
        credentials=google_creds,
        dest_bucket=config.DESTINATION_S3_BUCKET_NAME,
    )

    return uploaded_key