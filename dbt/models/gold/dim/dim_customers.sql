{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        customer_id,
        natural_key,
        first_name,
        last_name,

        -- ✅ Mask email when gdpr_consent = false
        CASE 
            WHEN gdpr_consent THEN email 
            ELSE MD5(email) 
        END AS email,

        -- ✅ Mask phone when gdpr_consent = false
        CASE 
            WHEN gdpr_consent THEN phone
            ELSE CONCAT('XXXXXX', RIGHT(phone, 2))
        END AS phone,

        -- ✅ Generalize address to city level if gdpr_consent = false
        CASE 
            WHEN gdpr_consent THEN address_line1
            ELSE NULL 
        END AS address_line1,
        CASE 
            WHEN gdpr_consent THEN address_line2
            ELSE NULL 
        END AS address_line2,

        city,
        state_region,
        postcode,
        country_code,
        latitude,
        longitude,
        birth_date,
        join_ts,
        is_vip,
        gdpr_consent
    FROM {{ ref('stg_customers') }}
),

-- 🧮 Add derived attributes
derived AS (
    SELECT
        *,
        -- ✅ Age calculation (in years)
        DATE_DIFF('year', birth_date, CURRENT_DATE) AS customer_age,

        -- ✅ Customer lifetime in days
        DATE_DIFF('day', join_ts, CURRENT_DATE) AS customer_lifetime_days,

        -- ✅ Customer segment based on VIP and join date
        CASE
            WHEN is_vip THEN 'VIP'
            WHEN DATE_DIFF('year', join_ts, CURRENT_DATE) >= 5 THEN 'Loyal'
            WHEN DATE_DIFF('year', join_ts, CURRENT_DATE) BETWEEN 1 AND 4 THEN 'Active'
            ELSE 'New'
        END AS customer_segment
    FROM base
)

SELECT * FROM derived
