with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    -- Surrogate Key (Good practice to have a unique string for every row)
    concat(items.order_id, '-', cast(items.order_item_id as string)) as order_item_sk,
    
    items.order_id,
    items.order_item_id,
    items.product_id,
    items.seller_id,
    
    -- We bring in Customer ID from the orders table so we can slice sales by customer location later
    orders.customer_id,
    
    items.price,
    items.freight_value,
    
    -- Calculated Total
    (items.price + items.freight_value) as total_order_value

from items
left join orders 
    on items.order_id = orders.order_id