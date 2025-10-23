{% snapshot products_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='check',
    check_cols=['price', 'name', 'category']
) }}

SELECT *
FROM {{ source('bronze', 'bronze_products') }}

{% endsnapshot %}
