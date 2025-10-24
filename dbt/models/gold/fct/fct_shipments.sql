{{ config(materialized='table') }}

SELECT
    shipment_id,
    order_id,
    carrier,
    shipped_at,
    delivered_at,
    DATEDIFF('day', shipped_at, delivered_at) AS delivery_days,
    CASE 
        WHEN delivered_at <= shipped_at + INTERVAL 5 DAY THEN 1 
        ELSE 0 
    END AS on_time_flag,
    ship_cost
FROM {{ ref('stg_shipments') }}
WHERE delivered_at IS NOT NULL