{{ config(
    materialized = 'incremental',
    unique_key = 'order_line_sk'
) }}

WITH base AS (
    SELECT
        ol.order_id,
        ol.product_id,             -- must exist here
        ol.order_ts,
        ol.order_dt,
        ol.customer_id,
        ol.store_id,
        ol.channel,
        ol.payment_method,
        ol.coupon_code,
        ol.shipping_fee,
        ol.currency
    FROM {{ ref('stg_orders_lines') }} AS ol
),

products AS (
    SELECT
        product_id,
        current_price AS unit_price,
        currency AS product_currency
    FROM {{ ref('stg_products') }}
),

customers AS (
    SELECT customer_id, natural_key AS customer_key
    FROM {{ ref('stg_customers') }}
),

stores AS (
    SELECT store_id, store_code, region, state
    FROM {{ ref('stg_stores') }}
),

exchange_rates AS (
    SELECT date, currency, rate_to_aud
    FROM {{ ref('stg_exchange_rates') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['b.order_id','b.product_id']) }} AS order_line_sk,
    b.order_id AS order_number,
    b.customer_id,
    c.customer_key,
    b.store_id,
    s.store_code,
    s.region,
    s.state,
    b.product_id,
    p.unit_price,
    b.channel,
    b.payment_method,
    b.currency,
    b.order_dt AS date,
    b.order_ts,
    1 AS quantity,
    1 * p.unit_price AS line_total,
    CASE WHEN b.coupon_code IS NOT NULL THEN 0.05 * p.unit_price ELSE 0 END AS discount_amount,
    0.10 * p.unit_price AS tax_amount,
    (1 * p.unit_price) - (CASE WHEN b.coupon_code IS NOT NULL THEN 0.05 * p.unit_price ELSE 0 END) + (0.10 * p.unit_price) AS net_amount,
    b.shipping_fee
FROM base b
LEFT JOIN products p USING (product_id)
LEFT JOIN customers c USING (customer_id)
LEFT JOIN stores s USING (store_id)
LEFT JOIN exchange_rates er
  ON b.order_dt = er.date
  AND b.currency = er.currency
