with source as (
    select * from {{ ref('stg_suppliers') }}
),

standardized as (
    select
        supplier_id,
        trim(supplier_name) as supplier_name,
        upper(trim(category)) as supplier_category,
        upper(trim(country)) as country_code,
        ingested_at as bronze_ingested_at,
        row_number() over (partition by supplier_id order by ingested_at desc) as rn
    from source
)

select 
    supplier_id,
    supplier_name,
    supplier_category,
    country_code,
    bronze_ingested_at
from standardized
where rn = 1