{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        birth_date,
        address_line1,
        address_line2,
        city,
        state_region,
        postcode,
        country_code,
        latitude,
        longitude
    FROM {{ source('bronze', 'bronze_customers') }}
),

dim_customer AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'email']) }} AS customer_sk,
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        birth_date,
        address_line1,
        address_line2,
        city,
        state_region,
        postcode,
        country_code,
        latitude,
        longitude
    FROM source_data
)

SELECT * FROM dim_customer
