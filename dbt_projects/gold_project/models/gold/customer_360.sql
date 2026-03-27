-- Gold layer: customer 360 view with segmentation
select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.created_at,
    c.total_orders,
    c.lifetime_spend,
    case
        when c.lifetime_spend >= 500 then 'platinum'
        when c.lifetime_spend >= 200 then 'gold'
        when c.lifetime_spend >= 50 then 'silver'
        else 'bronze'
    end as customer_tier,
    case
        when c.total_orders >= 3 then true
        else false
    end as is_repeat_customer
from {{ ref('silver_project', 'customers') }} c
