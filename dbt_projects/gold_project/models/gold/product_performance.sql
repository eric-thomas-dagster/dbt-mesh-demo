-- Gold layer: product performance analytics
select
    oi.product_id,
    oi.product_name,
    oi.category,
    count(oi.order_item_id) as times_ordered,
    sum(oi.quantity) as total_units_sold,
    sum(oi.line_total) as total_revenue,
    avg(oi.line_total) as avg_line_total
from {{ ref('silver_project', 'order_items') }} oi
group by oi.product_id, oi.product_name, oi.category
