select
    store_id,
    store_name,
    city,
    state,
    region,
    store_open_date,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'store_locations') }}