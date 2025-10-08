{{ config(
    materialized = 'incremental',
    unique_key = 'product_id'
) }}

with src as (
    select *
    from {{ ref('stg_products') }}
)
select *
from src
{% if is_incremental() %}
where product_id not in (select product_id from {{ this }})
{% endif %}
