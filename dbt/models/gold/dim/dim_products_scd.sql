WITH source AS (
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
        dbt_valid_from AS effective_start_date,
        dbt_valid_to   AS effective_end_date
    FROM {{ ref('stg_products_snapshot') }}
),

scd_enriched AS (
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
        effective_start_date,
        effective_end_date,

        -- 🔹 Flag for current record (open-ended SCD record)
        CASE 
            WHEN effective_end_date IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current,

        -- 🔹 Compare price vs. previous record for the same product
        LAG(current_price) OVER (
            PARTITION BY product_id 
            ORDER BY effective_start_date
        ) AS prev_price,

        CASE 
            WHEN LAG(current_price) OVER (
                PARTITION BY product_id 
                ORDER BY effective_start_date
            ) IS NULL THEN NULL
            WHEN current_price > LAG(current_price) OVER (
                PARTITION BY product_id 
                ORDER BY effective_start_date
            ) THEN 'Increased'
            WHEN current_price < LAG(current_price) OVER (
                PARTITION BY product_id 
                ORDER BY effective_start_date
            ) THEN 'Decreased'
            ELSE 'No Change'
        END AS price_change_indicator
    FROM source
)

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
    effective_start_date,
    effective_end_date,
    is_current,
    prev_price,
    price_change_indicator
FROM scd_enriched