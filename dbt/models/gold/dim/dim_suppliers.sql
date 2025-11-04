WITH source AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

-- 🧠 Enrich supplier attributes
enriched AS (
    SELECT
        supplier_id,
        supplier_code,
        name,
        country_code,
        lead_time_days,
        preferred,

        -- 🏷️ Supplier Tier Classification
        CASE
            WHEN lead_time_days <= 5 THEN 'Tier 1 - Fast'
            WHEN lead_time_days BETWEEN 6 AND 10 THEN 'Tier 2 - Standard'
            WHEN lead_time_days BETWEEN 11 AND 20 THEN 'Tier 3 - Slow'
            ELSE 'Tier 4 - Delayed'
        END AS supplier_tier,

        -- 🌍 Map countries to regions (customize as needed)
        CASE
            WHEN country_code IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN country_code IN ('GB', 'FR', 'DE', 'ES', 'IT') THEN 'Europe'
            WHEN country_code IN ('CN', 'JP', 'KR', 'SG', 'PH', 'IN') THEN 'Asia Pacific'
            WHEN country_code IN ('BR', 'AR', 'CL') THEN 'South America'
            WHEN country_code IN ('ZA', 'NG', 'EG') THEN 'Africa'
            ELSE 'Other'
        END AS country_region
    FROM source
)

SELECT * FROM enriched
