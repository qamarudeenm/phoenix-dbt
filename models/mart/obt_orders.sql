{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('int_customer_orders') }}
),

joined as (
    select
        -- Primary keys
        b.order_line_id,
        b.order_id,

        -- Customer
        c.customer_key,
        b.customer_id,

        -- Product
        p.product_id,
        p.product_name,
        p.product_category,

        -- Date
        d.date_id,
        d.date as order_date,
        d.day,
        d.week,
        d.month,
        d.quarter,
        d.year,
        d.month_str as order_month_str,

        -- Order metrics
        b.quantity,
        b.price_per_unit,
        b.gross_revenue,
        b.discount_value,
        b.net_revenue,

        -- Flags
        b.is_discounted,
        b.is_delivered,
        b.is_cancelled,
        b.is_returned,

        -- Metadata
        b.delivery_status,
        b.record_loaded_at

    from base b
    left join {{ ref('dim_product') }} p on b.product_id = p.product_id
    left join {{ ref('dim_customer') }} c on b.customer_id = c.customer_id
    left join {{ ref('dim_date') }} d on b.date_id = d.date_id
)

select * from joined
