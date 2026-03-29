{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

WITH bronze_products AS (
    SELECT * FROM {{ ref('bronze_products') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM bronze_products
    
    {% if is_incremental() %}
    -- Only pull records ingested since the last run
    WHERE _ingested_at > (SELECT MAX(bronze_ingested_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        UPPER(TRIM(product_id)) AS product_id,
        UPPER(TRIM(supplier_id)) AS supplier_id,

        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(brand) AS brand,
        
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,

        CASE 
            WHEN CAST(unit_price AS NUMERIC(12, 2)) > 500 THEN 'HIGH_VALUE'
            WHEN CAST(unit_price AS NUMERIC(12, 2)) > 100 THEN 'MEDIUM_VALUE'
            ELSE 'STANDARD_VALUE'
        END AS price_segment,

        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed