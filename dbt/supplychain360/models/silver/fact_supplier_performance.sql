{{ config(
    materialized='table',
    schema='silver'
) }}

with shipments as (
    select * from {{ ref('int_shipments_cleaned') }}
),

products as (
    select * from {{ ref('int_products_cleaned') }}
),

suppliers as (
    select * from {{ ref('int_suppliers_cleaned') }}
),

enriched as (
    select
        -- Primary Key
        s.shipment_id,
        
        -- Foreign Keys
        s.product_id,
        s.warehouse_id,
        p.supplier_id,
        
        -- Header Info
        sup.supplier_name,
        sup.supplier_category,
        p.product_name,
        p.brand as product_brand,
        
        -- Logistics Performance Metrics
        s.quantity_shipped,
        s.shipment_date,
        s.expected_delivery_date,
        s.actual_delivery_date,
        s.delivery_delay_days,
        
        -- Status Logic
        case 
            when s.actual_delivery_date is null then 'IN_TRANSIT'
            when s.delivery_delay_days > 0 then 'LATE'
            when s.delivery_delay_days = 0 then 'ON_TIME'
            else 'EARLY'
        end as delivery_status,
        
        s.carrier,
        s.bronze_ingested_at as processed_at

    from shipments s
    -- First: Link shipment to product
    inner join products p 
        on s.product_id = p.product_id
    -- Second: Link product to its supplier
    inner join suppliers sup 
        on p.supplier_id = sup.supplier_id
)

select * from enriched