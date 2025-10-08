{% set lake_root = '../lake/bronze/parquet' %}
{% set table_name = var('source_table', 'customers') %}  -- 'customers' is the default

{{ config(materialized='view') }}

select *
from read_parquet('{{ lake_root }}/{{ table_name }}/*.parquet')