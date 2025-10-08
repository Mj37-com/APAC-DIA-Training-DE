{{ config(materialized='view') }}

select
    customer_id,
    sha256(concat(first_name, last_name)) as masked_customer_key,
    case when is_vip = 1 then 'VIP' else 'Regular' end as customer_type
from {{ ref('fct_customers') }}
