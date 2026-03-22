select
    shipment_id,
    warehouse_id,
    store_id,
    product_id,
    quantity_shipped,
    shipment_date,
    expected_delivery_date,
    actual_delivery_date,
    carrier,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'shipments') }}