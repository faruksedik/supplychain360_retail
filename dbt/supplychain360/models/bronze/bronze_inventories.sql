with source as (
    SELECT * FROM {{ source('raw', 'inventories') }}
),

renamed as (
    SELECT
        warehouse_id,
        product_id,
        quantity_available,
        reorder_threshold,
        snapshot_date,
        current_timestamp as _ingested_at,
        'raw.inventories' as _source_file_path
    FROM source
)

SELECT * FROM renamed