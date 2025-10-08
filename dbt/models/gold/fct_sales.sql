{{ config(materialized='view') }}

select
    o.order_id,
    c.masked_customer_key,
    p.product_id,
    d.date_day as order_date,
    o.amount,
    o.amount * p.price as total_revenue,
    case when c.customer_type = 'VIP' then 0.9 * (o.amount * p.price) else o.amount * p.price end as discounted_revenue
from {{ ref('fct_customers') }} o
join {{ ref('dim_customers') }} c on o.customer_id = c.customer_id
join {{ ref('dim_products') }} p on o.product_id = p.product_id
join {{ ref('dim_date') }} d on cast(o.order_date as date) = d.date_day
