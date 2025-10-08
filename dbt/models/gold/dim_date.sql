{{ config(materialized='table') }}

with dates as (
    select 
        date_add(date '2020-01-01', interval n day) as date_day
    from range(0, 365 * 5)
)
select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    week(date_day) as week_of_year,
    case when extract(dow from date_day) in (6,0) then true else false end as is_weekend
from dates
