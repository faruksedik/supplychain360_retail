select
    supplier_id,
    supplier_name,
    category,
    country,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'suppliers') }}