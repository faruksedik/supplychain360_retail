{{ config(
    materialized='view',
    schema='gold'
) }}

with fact_sales as (
    select * from {{ ref('fact_sales_enriched') }}
),

monthly_aggregation as (
    select
        -- Temporal Grain: Monthly
        date_trunc('month', sold_at_xts)::date as sales_month,
        
        -- Geographic & Category Grain
        store_region,
        store_state,
        product_category,
        product_brand,
        
        -- Aggregated Metrics
        count(transaction_id) as total_orders,
        sum(quantity_sold) as total_units_sold,
        sum(sale_amount_usd) as gross_revenue_usd,
        sum(net_revenue_usd) as net_revenue_usd,
        
        -- Performance Calculation
        avg(discount_pct) as avg_discount_applied

    from fact_sales
    group by 1, 2, 3, 4, 5
),

final_metrics as (
    select
        *,
        -- Average Order Value (AOV)
        case 
            when total_orders > 0 then net_revenue_usd / total_orders 
            else 0 
        end as avg_order_value_usd
    from monthly_aggregation
)

select * from final_metrics