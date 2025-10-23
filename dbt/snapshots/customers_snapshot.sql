{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['address_line1','address_line2','city','state_region','postcode','country_code']
) }}

SELECT *
FROM {{ ref('stg_customers') }}

{% endsnapshot %}