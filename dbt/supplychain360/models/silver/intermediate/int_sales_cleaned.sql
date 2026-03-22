
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    schema='silver'
) }}

with source as (
    select * from {{ ref('stg_sales') }}
    {% if is_incremental() %}
      -- Only look at records newer than the max date currently in this table
      where ingested_at > (select max(bronze_ingested_at) from {{ this }})
    {% endif %}
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by transaction_id 
            order by ingested_at desc
        ) as row_num
    from source
),

standardized as (
    select
        transaction_id,
        store_id,
        product_id,
        cast(quantity_sold as integer) as quantity_sold,
        cast(unit_price as decimal(18, 2)) as unit_price_usd,
        cast(coalesce(discount_pct, 0) as decimal(5, 2)) as discount_pct,
        cast(sale_amount as decimal(18, 2)) as sale_amount_usd,
        transaction_timestamp as sold_at_xts,
        ingested_at as bronze_ingested_at
    from deduplicated
    where row_num = 1
)

select * from standardized