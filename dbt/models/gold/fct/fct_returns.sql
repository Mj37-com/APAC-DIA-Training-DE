{{ config(materialized='table') }}

SELECT
    r.return_id,
    r.order_id,
    r.product_id,
    r.qty AS return_qty,
    r.return_ts,
    r.reason,
    r.return_reason_code
FROM {{ ref('stg_returns') }} r