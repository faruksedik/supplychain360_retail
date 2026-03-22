
import logging
from supplychain.utils import config
from supplychain.utils.snowflake_utils import create_tables_in_snowflake, run_snowflake_load

logger = logging.getLogger(__name__)


def create_snowflake_tables() -> None:
    """
    Airflow wrapper task for creating Snowflake tables.

    This function retrieves Snowflake credentials from config and
    calls the table creation function.

    Designed to be used directly in an Airflow PythonOperator.

    Raises
    ------
    Exception
        Propagates any exception from table creation
    """
    logger.info("Starting Snowflake table creation task...")

    create_tables_in_snowflake(
        user=config.USER,
        password=config.PASSWORD,
        account=config.ACCOUNT,
        warehouse=config.WAREHOUSE,
        database=config.DATABASE,
        schema=config.SCHEMA
    )

    logger.info("Snowflake table creation task completed successfully.")

def load_all_data_to_snowflake() -> None:
    """
    Load all datasets from S3 into Snowflake.

    This function iterates through all dataset configurations defined
    in `config.target_tables` and executes the Snowflake load process
    for each dataset.

    For each dataset:
        - Logs the start of the load process
        - Executes the load using `run_snowflake_load`
        - Logs successful completion
        - Logs and raises any errors encountered

    Raises:
        Exception: Re-raises any exception encountered during loading
        to ensure upstream systems (e.g., Airflow) can handle failures.
    """
    SPECIAL_CASES = {
    "inventories": "inventory"
    }

    for entity in config.target_tables:
        logger.info(f"Loading dataset: {entity}")

        folder = SPECIAL_CASES.get(entity) or config.folders.get(entity)

        try:
            run_snowflake_load(
                target_table=config.target_tables[entity],
                folder=folder
            )
            logger.info(f"Completed dataset: {entity}")
        except Exception:
            logger.exception(f"Failed dataset: {entity}")
            raise

    logger.info("Data load pipeline completed successfully.")