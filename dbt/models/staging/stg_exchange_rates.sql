{{ config(
    materialized='view',
    description='Cleaned exchange rates'
) }}

SELECT
    currency,
    CAST(rate AS DOUBLE) AS rate,
    CAST(rate_date AS DATE) AS rate_date
FROM {{ source('bronze', 'bronze_exchange_rates') }}
