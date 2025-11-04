WITH source AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

enriched AS (
    SELECT
        store_id,
        store_code,
        name,
        channel,
        region,
        state,
        latitude,
        longitude,
        CAST(open_dt AS DATE) AS open_dt,
        CAST(close_dt AS DATE) AS close_dt,

        date_diff(
            'day',
            COALESCE(CAST(close_dt AS DATE), CURRENT_DATE),
            CAST(open_dt AS DATE)
        ) AS store_age_days,

        CASE
            WHEN latitude IS NULL OR longitude IS NULL THEN 'Small'
            WHEN ABS(latitude) + ABS(longitude) < 100 THEN 'Medium'
            ELSE 'Large'
        END AS store_size_category,

        CASE
            WHEN close_dt IS NULL THEN 'Operational'
            ELSE 'Closed'
        END AS operational_status

    FROM source
)

SELECT * FROM enriched
