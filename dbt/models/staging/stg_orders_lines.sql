{{ config(materialized='table') }}

SELECT
    *
FROM {{ source('bronze', 'bronze_orders_lines') }}