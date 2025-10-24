{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='merge',
        schema='gold'
    )
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
scd_products AS (
    SELECT
        product_id,
        sku,
        name,
        category,
        subcategory,
        current_price,
        currency,
        is_discontinued,
        introduced_dt,
        discontinued_dt,
        CASE
            WHEN LAG(current_price) OVER (PARTITION BY product_id ORDER BY introduced_dt) IS DISTINCT FROM current_price
            THEN 1 ELSE 0
        END AS price_changed_flag,
        CASE
            WHEN discontinued_dt IS NULL OR discontinued_dt > CURRENT_DATE THEN true ELSE false
        END AS is_current
    FROM products
)

SELECT *
FROM scd_products