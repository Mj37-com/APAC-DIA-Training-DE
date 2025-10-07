{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with src as (
    select *
    from {{ ref('stg_customers') }}
)
select *
from src
{% if is_incremental() %}
where customer_id not in (select customer_id from {{ this }})
{% endif %}
