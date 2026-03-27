-- Gold layer: monthly revenue summary
select
    date_trunc('month', o.order_date) as revenue_month,
    count(distinct o.order_id) as total_orders,
    count(distinct o.customer_id) as unique_customers,
    sum(o.order_total) as total_revenue,
    avg(o.order_total) as avg_order_value
from {{ ref('silver_project', 'orders') }} o
where o.status = 'completed'
group by date_trunc('month', o.order_date)
