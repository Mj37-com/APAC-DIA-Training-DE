{{ config(
    materialized='table',
    schema='gold'
) }}

WITH base AS (
    SELECT 
        DATE '2020-01-01' + (i || ' days')::INTERVAL AS date_day
    FROM range(0, 3650) AS t(i)  -- 10 years of dates
),

enhanced AS (
    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS iso_week,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        strftime(date_day, '%A') AS day_name,
        strftime(date_day, '%B') AS month_name,

        CASE 
            WHEN strftime(date_day, '%w') IN ('0','6') THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- Fiscal Year (July to June)
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) >= 7 THEN EXTRACT(YEAR FROM date_day) + 1
            ELSE EXTRACT(YEAR FROM date_day)
        END AS fiscal_year,

        CASE 
            WHEN EXTRACT(MONTH FROM date_day) >= 7 THEN EXTRACT(MONTH FROM date_day) - 6
            ELSE EXTRACT(MONTH FROM date_day) + 6
        END AS fiscal_month,

        -- Quarter boundaries
        date_trunc('quarter', date_day) AS quarter_start,
        date_trunc('quarter', date_day) + INTERVAL 3 MONTH - INTERVAL 1 DAY AS quarter_end,

        -- Simplified holidays (AU/US/UK)
        CASE 
            WHEN strftime(date_day, '%m-%d') IN ('01-01', '12-25', '07-04', '12-26', '04-25') THEN TRUE
            ELSE FALSE
        END AS is_holiday
    FROM base
)

SELECT * FROM enhanced