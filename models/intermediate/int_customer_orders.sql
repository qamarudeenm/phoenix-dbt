{{ config(materialized='view') }}

with base as (

    select
        -- Primary identifiers
        order_id,
        customer_id,
        product_id,
        order_date,

        -- Product information
        product_category,
        product_name,

        -- Order details
        quantity,
        price_per_unit,
        discount_applied,
        delivery_status,

        -- Revenue metrics
        quantity * price_per_unit as gross_revenue,
        quantity * price_per_unit * discount_applied as discount_value,
        quantity * price_per_unit * (1 - discount_applied) as net_revenue,

        -- Flags
        discount_applied > 0 as is_discounted,
        delivery_status = 'Delivered' as is_delivered,
        delivery_status = 'Cancelled' as is_cancelled,
        delivery_status = 'Returned' as is_returned,

        -- Date breakdowns
        extract(day from order_date) as order_day,
        extract(week from order_date) as order_week,
        extract(month from order_date) as order_month,
        extract(quarter from order_date) as order_quarter,
        extract(year from order_date) as order_year,
        to_char(order_date, 'YYYY-MM') as order_month_str,
        to_char(order_date, 'YYYY-MM-DD') as order_date_str,
        cast(to_char(order_date, 'YYYYMMDD') as integer) as date_id,

        -- Surrogate keys
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_line_id,
        {{ dbt_utils.generate_surrogate_key([
            'order_id', 'customer_id', 'product_id', 'order_date'
        ]) }} as row_key,

        -- Metadata
        current_timestamp() as record_loaded_at

    from {{ ref('stg_customer_orders') }}

)

select *
from base
