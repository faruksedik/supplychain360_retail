{{
    config(
        materialized='incremental',
        unique_key='supplier_id'
    )
}}

WITH bronze_suppliers AS (
    SELECT * FROM {{ ref('bronze_suppliers') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_id 
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM bronze_suppliers

    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(bronze_ingested_at) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        UPPER(TRIM(supplier_id)) AS supplier_id,

        TRIM(supplier_name) AS supplier_name,
        TRIM(category) AS category,
        TRIM(country) AS country,

        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed