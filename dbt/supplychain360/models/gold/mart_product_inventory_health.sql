{{ config(
    materialized='view',
    schema='gold'
) }}

with inventory_status as (
    select * from {{ ref('fact_inventory_daily_status') }}
),

inventory_metrics as (
    select
        -- Grain: Product per Brand per Month
        date_trunc('month', snapshot_date)::date as inventory_month,
        product_name,
        product_brand,
        product_category,
        
        -- Aggregated Health Metrics
        avg(quantity_available) as avg_daily_stock,
        avg(reorder_threshold) as avg_reorder_level,
        avg(stock_coverage_ratio) as avg_stock_coverage,
        
        -- Stockout Frequency (How many days in the month was it at risk?)
        count(case when stock_status = 'OUT_OF_STOCK' then 1 end) as days_out_of_stock,
        count(case when stock_status = 'LOW_STOCK' then 1 end) as days_low_stock,
        count(snapshot_date) as total_days_in_month

    from inventory_status
    group by 1, 2, 3, 4
),

final_mart as (
    select
        *,
        -- Stockout Rate: % of the month the product was unavailable
        (cast(days_out_of_stock as decimal) / total_days_in_month) * 100 as stockout_rate_pct,
        
        -- Health Categorization
        case 
            when (cast(days_out_of_stock as decimal) / total_days_in_month) > 0.20 then 'CRITICAL_UNRELIABLE'
            when avg_stock_coverage < 1.2 then 'UNDER_STOCKED'
            when avg_stock_coverage > 5.0 then 'OVER_STOCKED'
            else 'OPTIMAL'
        end as inventory_efficiency_rating
    from inventory_metrics
)

select * from final_mart