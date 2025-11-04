{{ config(
    materialized='table',
    schema='gold'
) }}

WITH base AS (
    SELECT
        sensor_ts,
        store_id,
        shelf_id,
        temperature_c,
        humidity_pct,
        battery_mv,
        month
    FROM {{ ref('stg_sensors') }}
),

-- 🕐 Round timestamps down to the nearest hour for aggregation
hourly AS (
    SELECT
        store_id,
        shelf_id,
        date_trunc('hour', sensor_ts) AS hour_ts,
        AVG(temperature_c) AS avg_temperature_c,
        AVG(humidity_pct) AS avg_humidity_pct,
        AVG(battery_mv) AS avg_battery_mv
    FROM base
    GROUP BY store_id, shelf_id, date_trunc('hour', sensor_ts)
),

-- ⚠️ Detect anomalies: out-of-range readings (based on realistic thresholds)
anomaly_flags AS (
    SELECT
        *,
        CASE 
            WHEN avg_temperature_c < 0 OR avg_temperature_c > 40 THEN TRUE
            ELSE FALSE
        END AS temperature_anomaly_flag,
        CASE 
            WHEN avg_humidity_pct < 10 OR avg_humidity_pct > 90 THEN TRUE
            ELSE FALSE
        END AS humidity_anomaly_flag,
        CASE 
            WHEN avg_battery_mv < 3000 THEN TRUE
            ELSE FALSE
        END AS low_battery_flag
    FROM hourly
),

-- 📈 Rolling statistics (3-hour moving average)
rolling_stats AS (
    SELECT
        *,
        AVG(avg_temperature_c) OVER (
            PARTITION BY store_id, shelf_id 
            ORDER BY hour_ts 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_temp_3h,

        AVG(avg_humidity_pct) OVER (
            PARTITION BY store_id, shelf_id 
            ORDER BY hour_ts 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_humidity_3h
    FROM anomaly_flags
)

SELECT
    store_id,
    shelf_id,
    hour_ts,
    avg_temperature_c,
    avg_humidity_pct,
    avg_battery_mv,
    temperature_anomaly_flag,
    humidity_anomaly_flag,
    low_battery_flag,
    rolling_avg_temp_3h,
    rolling_avg_humidity_3h
FROM rolling_stats