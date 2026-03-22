{{ config(
    materialized='table',
    schema='silver'
) }}

with warehouses as (
    select * from {{ ref('int_warehouses_cleaned') }}
),

shipment_stats as (
    -- Aggregating shipment volume per warehouse to measure throughput
    select
        warehouse_id,
        count(shipment_id) as total_shipments_processed,
        sum(quantity_shipped) as total_units_shipped,
        avg(delivery_delay_days) as avg_warehouse_delay_days
    from {{ ref('int_shipments_cleaned') }}
    group by 1
),

enriched as (
    select
        -- Using custom macro for the primary key
        {{ generate_surrogate_key(['w.warehouse_id']) }} as warehouse_key,
        
        w.warehouse_id,
        w.city_name,
        w.state_code,
        
        -- Efficiency Metrics
        coalesce(s.total_shipments_processed, 0) as total_shipments_processed,
        coalesce(s.total_units_shipped, 0) as total_units_shipped,
        coalesce(s.avg_warehouse_delay_days, 0) as avg_delay_days,
        
        -- Metadata
        w.bronze_ingested_at as processed_at

    from warehouses w
    left join shipment_stats s on w.warehouse_id = s.warehouse_id
)

select * from enriched