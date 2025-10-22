-- Run dbt run --select fct_sales --profiles-dir ./profiles
{{ config(materialized='incremental') }}

with base as (
    select
        c.customer_id,
        c.masked_customer_key,
        d.date_day as order_date,
        -- dummy amount for testing
        100 as amount,
        100 as price
    from {{ ref('dim_customers') }} c
    cross join {{ ref('dim_date') }} d
    -- optionally limit rows for faster testing
    where d.date_day <= current_date
)
select
    customer_id,
    masked_customer_key,
    order_date,
    amount,
    amount * price as total_revenue,
    case 
        when masked_customer_key like 'VIP%' then 0.9 * (amount * price) 
        else amount * price 
    end as discounted_revenue
from base
