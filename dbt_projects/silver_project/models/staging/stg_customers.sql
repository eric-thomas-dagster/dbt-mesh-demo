-- Bronze layer: raw customer data with basic type casting
select
    customer_id,
    first_name,
    last_name,
    email,
    cast(created_at as date) as created_at
from {{ ref('raw_customers') }}
