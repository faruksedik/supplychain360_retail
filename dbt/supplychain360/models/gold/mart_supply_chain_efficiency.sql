{{ config(
    materialized='view',
    schema='gold'
) }}

with supplier_performance as (
    select * from {{ ref('fact_supplier_performance') }}
),

warehouse_dim as (
    select * from {{ ref('dim_warehouses_enriched') }}
),

efficiency_metrics as (
    select
        -- Geographic & Entity Grain
        s.supplier_name,
        s.supplier_category,
        w.city_name as warehouse_city,
        w.state_code as warehouse_state,
        s.carrier,
        
        -- Temporal Grain: Monthly
        date_trunc('month', s.shipment_date)::date as shipment_month,
        
        -- Performance Aggregations
        count(s.shipment_id) as total_shipments,
        sum(s.quantity_shipped) as total_units_moved,
        avg(s.delivery_delay_days) as avg_days_delayed,
        
        -- Reliability Logic
        count(case when s.delivery_status = 'LATE' then 1 end) as late_shipment_count,
        count(case when s.delivery_status = 'ON_TIME' then 1 end) as on_time_shipment_count

    from supplier_performance s
    inner join warehouse_dim w on s.warehouse_id = w.warehouse_id
    group by 1, 2, 3, 4, 5, 6
),

final_mart as (
    select
        *,
        -- Reliability Score (On-Time %)
        case 
            when total_shipments > 0 
            then (cast(on_time_shipment_count as decimal) / total_shipments) * 100 
            else 0 
        end as on_time_delivery_rate_pct
    from efficiency_metrics
)

select * from final_mart