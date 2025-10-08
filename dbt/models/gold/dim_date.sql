{{ config(materialized='table') }}

with dates as (
    select 
        date '2020-01-01' + range * interval '1 day' as date_day
    from range(0, 365 * 5)  -- 5 years of dates
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    strftime(date_day, '%A') as day_name,   -- full day name
    strftime(date_day, '%B') as month_name, -- full month name
    strftime(date_day, '%W')::int + 1 as week_of_year,  -- week number
    case when extract(dow from date_day) in (6,0) then true else false end as is_weekend
from dates
