with source as (
    select * from {{ ref('stg_products') }}
),

deduplicated as (
    select
        product_id,
        trim(product_name) as product_name,
        upper(category) as category,
        trim(brand) as brand,
        supplier_id,
        cast(unit_price as decimal(18, 2)) as unit_price_usd,
        ingested_at as bronze_ingested_at,
        row_number() over (partition by product_id order by ingested_at desc) as rn
    from source
)

select 
    product_id,
    product_name,
    category,
    brand,
    supplier_id,
    unit_price_usd,
    bronze_ingested_at
from deduplicated
where rn = 1