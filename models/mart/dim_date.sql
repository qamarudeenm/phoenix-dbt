{{ config(materialized='table') }}

with date_spine as (

    {{ dbt_utils.date_spine(
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2030-12-31' as date)",
        datepart="day"
    ) }}

)

select
    cast(date_day as date) as date,
    extract(day from date_day) as day,
    extract(week from date_day) as week,
    extract(month from date_day) as month,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    to_char(date_day, 'YYYY-MM') as month_str,
    to_char(date_day, 'YYYY-MM-DD') as date_str,
    cast(to_char(date_day, 'YYYYMMDD') as integer) as date_id
from date_spine
