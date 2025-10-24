{{ config(materialized='table') }}

WITH orders AS (
    SELECT 
        order_id,
        line_number,
        product_id,
        qty,
        unit_price,
        line_discount_pct,
        tax_pct
    FROM {{ ref('stg_orders_lines') }}
),
products AS (
    SELECT 
        product_id,
        name AS product_name,
        sku,
        category,
        subcategory,
        current_price,
        currency AS product_currency,
        is_discontinued
    FROM {{ ref('stg_products') }}
)
SELECT
    o.order_id,
    o.line_number,
    o.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    o.qty,
    o.unit_price,
    o.line_discount_pct,
    o.tax_pct,
    o.qty * o.unit_price * (1 - o.line_discount_pct/100) AS net_amount
FROM orders o
JOIN products p ON o.product_id = p.product_id