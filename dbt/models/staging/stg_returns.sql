{{ config(
    materialized='view',
    description='Cleaned returns data'
) }}

SELECT
    return_id,
    order_id,
    product_id,
    CAST(return_ts AS TIMESTAMP) AT TIME ZONE 'UTC' AS return_ts,
    reason,
    quantity,
    CAST(refund_amount AS DOUBLE) AS refund_amount
FROM {{ source('bronze', 'bronze_returns') }}
