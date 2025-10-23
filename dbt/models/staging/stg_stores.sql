{{ config(
    materialized='view',
    description='Cleaned stores data'
) }}

SELECT
    store_id,
    TRIM(store_name) AS store_name,
    city,
    state_region,
    country_code,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude
FROM {{ source('bronze', 'bronze_stores') }}
