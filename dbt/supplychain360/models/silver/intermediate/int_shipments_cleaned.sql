
{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    schema='silver'
) }}

with source as (
    select * from {{ ref('stg_shipments') }}
    {% if is_incremental() %}
      where ingested_at > (select max(bronze_ingested_at) from {{ this }})
    {% endif %}
),

logic as (
    select
        shipment_id,
        warehouse_id,
        store_id,
        product_id,
        cast(quantity_shipped as integer) as quantity_shipped,
        shipment_date::date as shipment_date,
        expected_delivery_date::date as expected_delivery_date,
        actual_delivery_date::date as actual_delivery_date,
        case 
            when actual_delivery_date is not null 
            then datediff('day', expected_delivery_date::date, actual_delivery_date::date)
            else null 
        end as delivery_delay_days,
        carrier,
        ingested_at as bronze_ingested_at,
        row_number() over (
            partition by shipment_id 
            order by ingested_at desc
        ) as rn
    from source
)

select 
    shipment_id,
    warehouse_id,
    store_id,
    product_id,
    quantity_shipped,
    shipment_date,
    expected_delivery_date,
    actual_delivery_date,
    delivery_delay_days,
    carrier,
    bronze_ingested_at
from logic
where rn = 1