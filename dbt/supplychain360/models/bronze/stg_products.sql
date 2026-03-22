select
    product_id,
    product_name,
    category,
    brand,
    supplier_id,
    unit_price,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'products') }}