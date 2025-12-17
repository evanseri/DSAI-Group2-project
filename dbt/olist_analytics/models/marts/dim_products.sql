with products as (
    select * from {{ ref('stg_products') }}
),

translations as (
    select * from {{ ref('stg_product_category_name_translation') }}
)

select
    p.product_id,
    -- If english name is null, fall back to original name, then to 'Unknown'
    coalesce(t.product_category_name_english, p.product_category_name, 'Unknown') as category_name,
    p.photos_quantity,
    p.weight_g,
    p.length_cm,
    p.height_cm,
    p.width_cm
from products p
left join translations t 
    on p.product_category_name = t.product_category_name