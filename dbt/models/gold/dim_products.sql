{{ config(materialized='view') }}

select
    product_id,
    product_name,
    category,
    price,
    effective_date,
    end_date,
    is_current
from {{ ref('products_snapshot') }}
where is_current = true
