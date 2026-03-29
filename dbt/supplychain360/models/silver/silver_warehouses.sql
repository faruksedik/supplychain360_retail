{{
    config(
        materialized='incremental',
        unique_key='warehouse_id'
    )
}}

WITH bronze_warehouses AS (
    SELECT * FROM {{ ref('bronze_warehouses') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id 
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM bronze_warehouses

    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(bronze_ingested_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        UPPER(TRIM(warehouse_id)) AS warehouse_id,

        TRIM(city) AS city,
        TRIM(state) AS state,

        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed