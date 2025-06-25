{{ config(materialized='view') }}

with customers as (

    select distinct
        customer_id
    from {{ ref('stg_customer_orders') }}

)

select
    customer_id,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key
from customers
