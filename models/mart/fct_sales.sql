{{ config(materialized='table') }}

with final as (

    select
        order_line_id,
        order_id,
        customer_id,
        product_id,
        date_id,

        quantity,
        gross_revenue,
        discount_value,
        net_revenue,

        is_discounted,
        is_delivered,
        is_cancelled,
        is_returned,

        order_date,
        order_month_str,
        order_year,

        record_loaded_at

    from {{ ref('int_customer_orders') }}

)

select *
from final
