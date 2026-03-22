select
    transaction_id,
    store_id,
    product_id,
    quantity_sold,
    unit_price,
    discount_pct,
    sale_amount,
    transaction_timestamp,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'sales') }}