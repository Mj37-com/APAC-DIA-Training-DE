{{ config(materialized='table') }}

SELECT
    *
FROM {{ source('bronze', 'bronze_exchange_rates') }}