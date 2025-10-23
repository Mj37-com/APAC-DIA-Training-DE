{{ config(
    materialized='view',
    description='Cleaned events data'
) }}

WITH parsed AS (
    SELECT
        envelope:event_id::STRING AS event_id,
        CAST(envelope:event_ts AS TIMESTAMP) AT TIME ZONE 'UTC' AS event_ts,
        envelope:event_type::STRING AS event_type,
        envelope:user_id::BIGINT AS user_id,
        envelope:session_id::STRING AS session_id,
        payload:value::DOUBLE AS value
    FROM {{ source('bronze', 'bronze_events') }}
)
SELECT *
FROM parsed
