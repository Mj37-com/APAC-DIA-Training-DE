{{ config(materialized='incremental') }}

select
    product_id,
    sha256(concat(name, sku)) as masked_product_key,
    category,
    subcategory,
    case 
        when is_discontinued = true then 'Discontinued'
        else 'Active'
    end as product_status
from {{ ref('fct_products') }}
