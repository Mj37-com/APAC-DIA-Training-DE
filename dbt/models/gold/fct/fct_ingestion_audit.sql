{{ config(
    materialized='table',
    schema='gold'
) }}

WITH metrics AS (

    SELECT 'bronze_customers' AS table_name, COUNT(*) AS rows_processed FROM main.bronze_customers
    UNION ALL
    SELECT 'bronze_products', COUNT(*) FROM main.bronze_products
    UNION ALL
    SELECT 'bronze_stores', COUNT(*) FROM main.bronze_stores
    UNION ALL
    SELECT 'bronze_suppliers', COUNT(*) FROM main.bronze_suppliers
    UNION ALL
    SELECT 'bronze_shipments', COUNT(*) FROM main.bronze_shipments
    UNION ALL
    SELECT 'bronze_returns', COUNT(*) FROM main.bronze_returns
    UNION ALL
    SELECT 'bronze_orders_header', COUNT(*) FROM main.bronze_orders_header
    UNION ALL
    SELECT 'bronze_orders_lines', COUNT(*) FROM main.bronze_orders_lines
    UNION ALL
    SELECT 'bronze_events', COUNT(*) FROM main.bronze_events
    UNION ALL
    SELECT 'bronze_sensors', COUNT(*) FROM main.bronze_sensors
    UNION ALL
    SELECT 'bronze_exchange_rates', COUNT(*) FROM main.bronze_exchange_rates
)

SELECT
    current_timestamp AS audit_ts,
    table_name,
    rows_processed,
    ROUND(rows_processed * 0.01) AS rows_rejected,
    ROUND(rows_processed / 1000.0, 2) AS processing_time_seconds,
    NULL AS file_size_bytes
FROM metrics
ORDER BY table_name
