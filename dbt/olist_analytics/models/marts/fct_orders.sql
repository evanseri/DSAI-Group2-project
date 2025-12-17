with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    
    -- Timestamps
    purchase_at,
    approved_at,
    delivered_carrier_at,
    delivered_customer_at,
    estimated_delivery_at,

    -- Calculated Metrics (BigQuery Syntax)
    -- 1. How long did it take to deliver?
    timestamp_diff(delivered_customer_at, purchase_at, hour) as time_to_delivery_hours,
    
    -- 2. Was it late? (Difference between estimated and actual)
    -- Positive numbers mean it arrived early, Negative means it was late
    timestamp_diff(estimated_delivery_at, delivered_customer_at, day) as days_early_or_late

from orders