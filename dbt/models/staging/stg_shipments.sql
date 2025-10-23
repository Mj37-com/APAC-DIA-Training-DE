{{ config(
    materialized='view',
    description='Cleaned shipments data'
) }}

SELECT
    shipment_id,
    order_id,
    CAST(ship_date AS TIMESTAMP) AT TIME ZONE 'UTC' AS ship_ts,
    carrier,
    tracking_number,
    status
FROM {{ source('bronze', 'bronze_shipments') }}
