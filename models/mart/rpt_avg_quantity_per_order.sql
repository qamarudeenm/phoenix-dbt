{{ config(materialized='view') }}

select
    order_id,
    avg(quantity) as avg_quantity_per_order
from {{ ref('fct_orders') }}
group by order_id
