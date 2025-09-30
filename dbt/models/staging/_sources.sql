-- External views pointing at Bronze Parquet/Delta. Adjust path if needed.
{% set lake_root = '../lake/bronze' %}

select * from read_parquet('../lake/bronze/parquet/customers/*.parquet')

-- create or replace view bronze_customers_parquet as
-- select * from read_parquet('{{ lake_root }}/parquet/customers/*.parquet');

-- create or replace view bronze_customers_delta as
-- select * from delta_scan('{{ lake_root }}/delta/customers');
