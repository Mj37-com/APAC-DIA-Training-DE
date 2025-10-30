{{ config(materialized='incremental') }}

SELECT
    order_id,
    order_ts,
    order_dt,
    order_dt_local,
    customer_id,
    store_id,
    shipping_fee,
    payment_method
FROM {{ ref('stg_orders_lines') }}
