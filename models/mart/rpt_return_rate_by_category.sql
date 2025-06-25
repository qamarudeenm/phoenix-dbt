{{ config(materialized='view') }}

select
    p.product_category,
    count(*) as total_orders,
    sum(case when is_returned then 1 else 0 end) as returned_orders,
    round(100.0 * sum(case when is_returned then 1 else 0 end) / count(*), 2) as return_rate_percent
from {{ ref('fct_orders') }} o
join {{ ref('dim_product') }} p
    on o.product_id = p.product_id
group by p.product_category
