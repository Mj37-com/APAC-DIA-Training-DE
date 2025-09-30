{{ config(
    materialized='table') }}


with src as (
  select *
    from {{ ref('_sources') }}
),
typed as (
  select
    cast(customer_id as bigint) as customer_id,
    natural_key,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    email,
    phone,
    address_line1, address_line2, city, state_region, postcode, country_code,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(birth_date as date) as birth_date,
    cast(join_ts as timestamp) as join_ts_utc,
    cast(is_vip as boolean) as is_vip,
    cast(gdpr_consent as boolean) as gdpr_consent
  from src
)
select * from typed
