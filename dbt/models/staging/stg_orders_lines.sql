{{ config(materialized='table') }}

SELECT
    *,
    product_id 
FROM {{ source('bronze', 'bronze_orders_lines') }}