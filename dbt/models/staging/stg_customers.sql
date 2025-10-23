{{ config(
    materialized='table'
) }}

WITH raw_customers AS (
    SELECT
        customer_id,
        natural_key,
        TRIM(CAST(first_name AS VARCHAR)) AS first_name,
        TRIM(CAST(last_name AS VARCHAR)) AS last_name,
        TRIM(CAST(email AS VARCHAR)) AS email,
        TRIM(CAST(phone AS VARCHAR)) AS phone,
        CAST(birth_date AS DATE) AS birth_date,
        TRIM(CAST(address_line1 AS VARCHAR)) AS address_line1,
        TRIM(CAST(address_line2 AS VARCHAR)) AS address_line2,
        TRIM(CAST(city AS VARCHAR)) AS city,
        TRIM(CAST(state_region AS VARCHAR)) AS state_region,
        TRIM(CAST(postcode AS VARCHAR)) AS postcode,
        TRIM(CAST(country_code AS VARCHAR)) AS country_code,
        CAST(latitude AS DOUBLE) AS latitude,
        CAST(longitude AS DOUBLE) AS longitude
    FROM {{ source('bronze', 'bronze_customers') }}
)

SELECT *
FROM raw_customers
