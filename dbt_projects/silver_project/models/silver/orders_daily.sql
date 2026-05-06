{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_date',
    begin='2024-06-01',
    batch_size='day'
) }}

select
    order_id,
    customer_id,
    order_date,
    status,
    amount
from {{ ref('stg_orders') }}
