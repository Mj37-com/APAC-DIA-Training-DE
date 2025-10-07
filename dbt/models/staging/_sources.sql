{% set lake_root = '../lake/bronze' %}

{{ config(materialized='view') }}

select *
from read_parquet('{{ lake_root }}/parquet/customers/*.parquet')
