{{ config(
    materialized='view',
    description='Cleaned orders lines data'
) }}

SELECT
    order_id,
    line_id,
    product_id,
    quantity,
    CAST(price AS DOUBLE) AS price,
    CAST(discount AS DOUBLE) AS discount,
    (quantity * price * (1 - discount/100)) AS line_total
FROM {{ source('bronze', 'bronze_orders_lines') }}
