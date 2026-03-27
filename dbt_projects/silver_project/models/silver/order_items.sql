-- Silver layer: enriched order items with product details
select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    p.unit_price,
    oi.quantity * p.unit_price as line_total
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_products') }} p on oi.product_id = p.product_id
