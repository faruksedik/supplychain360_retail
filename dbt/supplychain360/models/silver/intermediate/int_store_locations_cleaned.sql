with source as (
    select * from {{ ref('stg_store_locations') }}
),

standardized as (
    select
        store_id,
        trim(store_name) as store_name,
        initcap(trim(city)) as city_name,
        upper(trim(state)) as state_code,
        upper(trim(region)) as region_name,
        store_open_date::date as store_open_date,
        ingested_at as bronze_ingested_at,
        row_number() over (partition by store_id order by ingested_at desc) as rn
    from source
)

select 
    store_id,
    store_name,
    city_name,
    state_code,
    region_name,
    store_open_date,
    bronze_ingested_at
from standardized
where rn = 1