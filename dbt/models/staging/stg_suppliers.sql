{{ config(
    materialized='view',
    description='Cleaned suppliers data'
) }}

SELECT
    supplier_id,
    TRIM(name) AS name,
    TRIM(contact_name) AS contact_name,
    email,
    phone,
    country_code
FROM {{ source('bronze', 'bronze_suppliers') }}
