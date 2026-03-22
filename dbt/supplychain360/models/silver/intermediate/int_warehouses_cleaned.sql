with source as (
    select * from {{ ref('stg_warehouses') }}
),

standardized as (
    select
        warehouse_id,
        initcap(trim(city)) as city_name,
        upper(trim(state)) as state_code,
        ingested_at as bronze_ingested_at,
        row_number() over (partition by warehouse_id order by ingested_at desc) as rn
    from source
)

select 
    warehouse_id,
    city_name,
    state_code,
    bronze_ingested_at
from standardized
where rn = 1