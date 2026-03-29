
from supplychain.utils import config
from supplychain.utils.logger import get_logger
import snowflake.connector
from typing import Tuple
from supplychain.utils.logger import get_logger

logger = get_logger(__name__)


def connect_to_snowflake(
    user: str,
    password: str,
    account: str,
    warehouse: str,
    database: str,
    schema: str
) -> Tuple:
    """
    Create and return a Snowflake connection and cursor.

    Parameters
    ----------
    user : str
    password : str
    account : str
    warehouse : str
    database : str
    schema : str

    Returns
    -------
    tuple
        (connection, cursor)

    Raises
    ------
    Exception
        If connection fails
    """
    try:
        logger.info("Connecting to Snowflake (account=%s, db=%s, schema=%s)",
                    account, database, schema)

        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        cursor = conn.cursor()

        logger.info("Snowflake connection established successfully")
        return conn, cursor

    except Exception as e:
        logger.error("Snowflake connection failed: %s", str(e))
        raise


def copy_parquet_folder(
    conn,
    target_table: str,
    stage_name: str,
    folder: str = None,
    file_format: str = "PARQUET",
    on_error: str = "CONTINUE"
) -> None:
    """
    Load Parquet files from a Snowflake stage into a target table.

    Parameters
    ----------
    conn : Snowflake connection
    target_table : str
        Destination table in Snowflake
    stage_name : str
        Snowflake stage name
    folder : str, optional
        Folder path inside stage (e.g., 'raw/sales/')
    file_format : str
        File format type (default: PARQUET)
    on_error : str
        Error handling strategy (default: CONTINUE)

    Returns
    -------
    None

    Raises
    ------
    Exception
        If COPY INTO fails
    """
    cursor = conn.cursor()

    try:
        if not target_table:
            raise ValueError("target_table cannot be empty")

        if not stage_name:
            raise ValueError("stage_name cannot be empty")

        # Build stage path
        stage_path = f"@{stage_name}"
        if folder:
            stage_path += f"/{folder.strip('/')}"

        logger.info(
            "Starting COPY INTO for table '%s' from '%s'",
            target_table, stage_path
        )

        copy_sql = f"""
        COPY INTO {target_table}
        FROM {stage_path}
        FILE_FORMAT = (TYPE = {file_format})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = '{on_error}'
        FORCE = FALSE;
        """

        cursor.execute(copy_sql)

        logger.info("COPY INTO completed successfully for '%s'", target_table)

    except Exception as e:
        logger.error(
            "COPY INTO failed for table '%s': %s",
            target_table, str(e)
        )
        raise

    finally:
        cursor.close()
        logger.info("Snowflake cursor closed after COPY")



# Table creation SQLs
TABLE_CREATION_SQL = {
    "suppliers": """
        CREATE OR REPLACE TABLE suppliers (
            supplier_id   VARCHAR PRIMARY KEY,
            supplier_name VARCHAR,
            category      VARCHAR,
            country       VARCHAR
        );
    """,
    "warehouses": """
        CREATE OR REPLACE TABLE warehouses (
            warehouse_id VARCHAR PRIMARY KEY,
            city         VARCHAR,
            state        VARCHAR
        );
    """,
    "store_locations": """
        CREATE OR REPLACE TABLE store_locations (
            store_id        VARCHAR PRIMARY KEY,
            store_name      VARCHAR,
            city            VARCHAR,
            state           VARCHAR,
            region          VARCHAR,
            store_open_date DATE
        );
    """,
    "products": """
        CREATE OR REPLACE TABLE products (
            product_id   VARCHAR PRIMARY KEY,
            product_name VARCHAR,
            category     VARCHAR,
            brand        VARCHAR,
            supplier_id  VARCHAR,
            unit_price   NUMERIC(12, 2)
        );
    """,
    "inventories": """
        CREATE OR REPLACE TABLE inventories (
            warehouse_id       VARCHAR,
            product_id         VARCHAR,
            quantity_available INTEGER,
            reorder_threshold  INTEGER,
            snapshot_date      DATE
        );
    """,
    "sales": """
        CREATE OR REPLACE TABLE sales (
            transaction_id        VARCHAR PRIMARY KEY,
            store_id              VARCHAR,
            product_id            VARCHAR,
            quantity_sold         INTEGER,
            unit_price            NUMERIC(12, 2),
            discount_pct          NUMERIC(5, 2),
            sale_amount           NUMERIC(15, 2),
            transaction_timestamp TIMESTAMP_NTZ
        );
    """,
    "shipments": """
        CREATE OR REPLACE TABLE shipments (
            shipment_id            VARCHAR PRIMARY KEY,
            warehouse_id           VARCHAR,
            store_id               VARCHAR,
            product_id             VARCHAR,
            quantity_shipped       INTEGER,
            shipment_date          DATE,
            expected_delivery_date DATE,
            actual_delivery_date   DATE,
            carrier                VARCHAR
        );
    """
}

def create_tables_in_snowflake(
    user: str,
    password: str,
    account: str,
    warehouse: str,
    database: str,
    schema: str
) -> None:
    """
    Create all tables in Snowflake using the provided connection parameters.

    Parameters
    ----------
    user : str
    password : str
    account : str
    warehouse : str
    database : str
    schema : str

    Raises
    ------
    Exception
        If any table creation fails
    """
    try:
        conn, cursor = connect_to_snowflake(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        for table_name, sql in TABLE_CREATION_SQL.items():
            logger.info("Creating table: %s", table_name)
            cursor.execute(sql)
            logger.info("Table created successfully: %s", table_name)

        cursor.close()
        conn.close()
        logger.info("All tables created successfully in Snowflake!")

    except Exception as e:
        logger.exception("Error creating tables in Snowflake: %s", str(e))
        raise



def run_snowflake_load(
    target_table: str,
    folder: str,
    user: str = config.USER,
    password: str = config.PASSWORD,
    account: str = config.ACCOUNT,
    warehouse: str = config.WAREHOUSE,
    database: str = config.DATABASE,
    schema: str = config.SCHEMA,
    stage_name: str = config.STAGE_NAME,
) -> None:
    """
    Orchestrate loading of Parquet files from S3 into Snowflake tables.

    This function:
        1. Establishes a connection to Snowflake
        2. Loads data from corresponding S3 folders into each table
        3. Ensures proper cleanup of resources

    Parameters
    ----------
    user : str
        Snowflake username
    password : str
        Snowflake password
    account : str
        Snowflake account identifier
    warehouse : str
        Snowflake warehouse name
    database : str
        Target database
    schema : str
        Target schema
    stage_name : str
        Snowflake stage name pointing to S3
    target_table : str
        table to load in snowflake (e.g., "sales", "products")
    folder : str, optional
        Folder path inside stage (e.g., 'raw/sales/')

    Returns
    -------
    None

    Raises
    ------
    Exception
        If any step in the pipeline fails
    """

    conn = None

    try:
        logger.info("Starting Snowflake load orchestration")

        # Step 1: Connect (same structure you like)
        conn, _ = connect_to_snowflake(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        # Step 2: Load the target table table
        copy_parquet_folder(
            conn,
            target_table=target_table,
            stage_name=stage_name,
            folder=folder,
            file_format="PARQUET",
            on_error="CONTINUE"
        )

        logger.info("Snowflake load orchestration completed successfully")

    except Exception as e:
        logger.error("Snowflake load orchestration failed: %s", str(e))
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")