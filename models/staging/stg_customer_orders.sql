{{ config(materialized='view') }}

with source as (

    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        {{ dbt_utils.generate_surrogate_key(['product_category', 'product_name']) }} as product_id,
        product_category,
        product_name,
        quantity,
        price_per_unit,
        discount_applied,
        delivery_status
        
    from {{ source('order_data', 'customer_orders') }}

)

select *
from source
