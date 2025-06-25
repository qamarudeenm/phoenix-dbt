{{ config(materialized='view') }}

select
    order_month_str,
    count(distinct order_id) as total_orders,
    sum(net_revenue) as total_revenue
from {{ ref('fct_orders') }}
group by order_month_str
