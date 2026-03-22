from supplychain.utils import config
from supplychain.utils.logger import get_logger
from supplychain.utils.s3_utils import ingest_to_s3

logger = get_logger(__name__)

def ingest_s3_bucket_data():
    """
    Automatically loops through all configured source folders and 
    ingests them into the destination S3 bucket.
    
    Returns:
        dict: A mapping of domain names to the list of successfully uploaded keys.
    """
    all_uploaded_keys = {}
    
    # Iterate through every folder s3_bucket defined in config
    # e.g., 'products', 'inventory', 'warehouses', etc.
    for domain, prefix in config.SOURCE_BUCKET_FOLDERS.items():
        try:
            logger.info(">>> Starting batch ingestion for domain: %s", domain)
            
            
            uploaded_keys = ingest_to_s3(
                config.SOURCE_S3_BUCKET_NAME,
                prefix,
                config.DESTINATION_S3_BUCKET_NAME
            )
            
            all_uploaded_keys[domain] = uploaded_keys
            logger.info("<<< Completed %s: %d files moved.", domain, len(uploaded_keys))
            
        except Exception as e:
            logger.error("Failed to ingest domain '%s': %s", domain, str(e))
            continue

    return all_uploaded_keys