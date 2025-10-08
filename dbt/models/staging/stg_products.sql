{% set lake_root = '../data_raw' %}
{% set source_table = var('source_table', 'products') %}

{{ config(
    materialized = 'table',
    contract = { 'enforced': true }
) }}

select
    product_id,
    sku,
    name,
    category,
    subcategory,
    current_price,
    currency,
    is_discontinued,
    introduced_dt,
    discontinued_dt
from read_parquet('{{ lake_root }}/{{ source_table }}.parquet')