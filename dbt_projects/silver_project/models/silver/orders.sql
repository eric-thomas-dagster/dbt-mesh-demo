-- Silver layer: enriched order data with customer and line item details
select
    o.order_id,
    o.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    o.order_date,
    o.status,
    o.amount as order_total,
    count(oi.order_item_id) as item_count
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
group by o.order_id, o.customer_id, c.first_name, c.last_name, o.order_date, o.status, o.amount
