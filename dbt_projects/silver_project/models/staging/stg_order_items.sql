-- Bronze layer: raw order items with basic type casting
select
    order_item_id,
    order_id,
    product_id,
    quantity
from {{ source('raw_data', 'raw_order_items') }}
