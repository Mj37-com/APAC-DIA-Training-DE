{{ config(
    materialized='view',
    description='Cleaned products data'
) }}

SELECT
    product_id,
    TRIM(name) AS name,
    TRIM(category) AS category,
    CAST(price AS DOUBLE) AS price,
    CAST(cost AS DOUBLE) AS cost,
    updated_at
FROM {{ source('bronze', 'bronze_products') }}
