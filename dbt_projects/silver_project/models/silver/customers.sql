-- Silver layer: enriched customer data with order summary
select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.created_at,
    count(o.order_id) as total_orders,
    coalesce(sum(o.amount), 0) as lifetime_spend
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
group by c.customer_id, c.first_name, c.last_name, c.email, c.created_at
