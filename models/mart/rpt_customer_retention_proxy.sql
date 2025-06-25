{{ config(materialized='view') }}

with customer_orders as (
    select customer_id, count(distinct order_id) as total_orders
    from {{ ref('fct_orders') }}
    group by customer_id
)

select
    count(*) as total_customers,
    count_if(total_orders > 1) as returning_customers,
    round(100.0 * count_if(total_orders > 1) / count(*), 2) as return_rate_percent
from customer_orders
