-- Bronze layer: raw product data with basic type casting
select
    product_id,
    product_name,
    category,
    cast(unit_price as decimal(10,2)) as unit_price
from {{ ref('raw_products') }}
