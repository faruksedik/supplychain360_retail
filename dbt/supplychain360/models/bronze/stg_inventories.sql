select
    warehouse_id,
    product_id,
    quantity_available,
    reorder_threshold,
    snapshot_date,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'inventories') }}