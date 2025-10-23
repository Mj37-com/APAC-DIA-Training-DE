{{ config(
    materialized='view',
    description='Staging orders header and lines'
) }}

WITH header AS (
    SELECT *,
           CAST(order_dt AS TIMESTAMP) AT TIME ZONE 'UTC' AS order_ts
    FROM {{ source('bronze', 'bronze_orders_header') }}
),
lines AS (
    SELECT *,
           CAST(shipment_dt AS TIMESTAMP) AT TIME ZONE 'UTC' AS shipment_ts
    FROM {{ source('bronze', 'bronze_orders_lines') }}
),
joined AS (
    SELECT
        h.order_id,
        h.customer_id,
        h.order_ts,
        h.status,
        l.product_id,
        l.quantity,
        l.price,
        l.discount,
        (l.price * l.quantity * (1 - l.discount/100)) AS order_total
    FROM header h
    LEFT JOIN lines l USING(order_id)
)
SELECT *
FROM joined
