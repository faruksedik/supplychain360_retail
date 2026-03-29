{{
    config(
        materialized='incremental',
        unique_key='store_id'
    )
}}

WITH bronze_store_locations AS (
    SELECT * FROM {{ ref('bronze_store_locations') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY store_id 
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM bronze_store_locations

    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(bronze_ingested_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        UPPER(TRIM(store_id)) AS store_id,

        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(region) AS region,
        
        CAST(store_open_date AS DATE) AS store_open_date,

        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed