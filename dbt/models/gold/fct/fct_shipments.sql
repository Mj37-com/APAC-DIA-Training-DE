{{ config(
    materialized='table',
    schema='gold'
) }}

WITH base AS (
    SELECT
        shipment_id,
        order_id,
        carrier,
        shipped_at,
        delivered_at,
        ship_cost AS shipping_cost
    FROM {{ ref('stg_shipments') }}
),

metrics AS (
    SELECT
        shipment_id,
        order_id,
        carrier,
        shipping_cost,
        shipped_at,
        delivered_at,

        -- Delivery days between ship and delivery (NULL if not yet delivered)
        CASE 
            WHEN delivered_at IS NOT NULL THEN datediff('day', shipped_at, delivered_at)
            ELSE NULL
        END AS delivery_days,

        -- On-time flag (SLA: delivered within 5 days)
        CASE 
            WHEN delivered_at IS NOT NULL AND datediff('day', shipped_at, delivered_at) <= 5 THEN TRUE
            ELSE FALSE
        END AS on_time_flag,

        -- SLA compliance (percentage form)
        CASE 
            WHEN delivered_at IS NOT NULL THEN 
                CASE WHEN datediff('day', shipped_at, delivered_at) <= 5 THEN 1 ELSE 0 END
            ELSE 0
        END AS sla_compliant_flag
    FROM base
)

SELECT
    shipment_id,
    order_id,
    carrier,
    shipping_cost,
    delivery_days,
    on_time_flag,
    sla_compliant_flag
FROM metrics