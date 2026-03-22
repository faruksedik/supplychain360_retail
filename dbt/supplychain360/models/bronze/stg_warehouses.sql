select
    warehouse_id,
    city,
    state,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'warehouses') }}