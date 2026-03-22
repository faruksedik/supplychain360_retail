{{ config(
    materialized='table',
    schema='silver'
) }}

with sales as (
    select * from {{ ref('int_sales_cleaned') }}
),

products as (
    select * from {{ ref('int_products_cleaned') }}
),

stores as (
    select * from {{ ref('int_store_locations_cleaned') }}
),

enriched as (
    select
        -- Primary Key
        s.transaction_id,
        
        -- Foreign Keys & Natural Keys
        s.store_id,
        s.product_id,
        p.supplier_id,
        
        -- Sales Metrics (USD)
        s.quantity_sold,
        s.unit_price_usd,
        s.discount_pct,
        s.sale_amount_usd,
        
        -- Calculated Metrics
        (s.sale_amount_usd * s.discount_pct) as discount_amount_usd,
        (s.sale_amount_usd - (s.sale_amount_usd * s.discount_pct)) as net_revenue_usd,
        
        -- Product Context
        p.product_name,
        p.category as product_category,
        p.brand as product_brand,
        
        -- Store/Regional Context
        st.store_name,
        st.city_name as store_city,
        st.state_code as store_state,
        st.region_name as store_region,
        
        -- Timestamps
        s.sold_at_xts,
        s.bronze_ingested_at as processed_at
        
    from sales s
    inner join products p on s.product_id = p.product_id
    inner join stores st on s.store_id = st.store_id
)

select * from enriched