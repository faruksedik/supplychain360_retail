
{{ config(
    materialized='incremental',
    unique_key='inventory_pk',
    schema='silver'
) }}

with source as (
    select * from {{ ref('stg_inventories') }}
    {% if is_incremental() %}
      where ingested_at > (select max(bronze_ingested_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Creating a unique key for incremental processing in Snowflake
        {{ generate_surrogate_key(['warehouse_id', 'product_id', 'snapshot_date']) }} as inventory_pk,
        warehouse_id,
        product_id,
        cast(quantity_available as integer) as quantity_available,
        cast(reorder_threshold as integer) as reorder_threshold,
        snapshot_date::date as snapshot_date,
        case 
            when quantity_available <= reorder_threshold then 1 
            else 0 
        end as is_out_of_stock_risk,
        ingested_at as bronze_ingested_at,
        row_number() over (
            partition by warehouse_id, product_id, snapshot_date 
            order by ingested_at desc
        ) as rn
    from source
)

select 
    inventory_pk,
    warehouse_id,
    product_id,
    quantity_available,
    reorder_threshold,
    snapshot_date,
    is_out_of_stock_risk,
    bronze_ingested_at
from renamed
where rn = 1