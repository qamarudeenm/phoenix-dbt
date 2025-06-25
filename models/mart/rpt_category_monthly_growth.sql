{{ config(materialized='view') }}

select
    p.product_category,
    order_month_str,
    sum(net_revenue) as monthly_revenue
from {{ ref('fct_orders') }} o
join {{ ref('dim_product') }} p
    on o.product_id = p.product_id
group by p.product_category, order_month_str
