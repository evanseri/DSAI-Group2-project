with customers as (
    select * from {{ ref('stg_customers') }}
),

unique_customers as (
    -- Deduplicate to get one row per unique person
    select
        customer_unique_id,
        -- We use MAX/ANY_VALUE to just grab one valid location per person
        -- In a real scenario, you might want their "current" address logic
        max(zip_code) as zip_code,
        max(city) as city,
        max(state) as state
    from customers
    group by customer_unique_id
)

select
    customer_unique_id,
    zip_code,
    city,
    state
from unique_customers