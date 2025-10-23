{{ config(materialized='table') }}

SELECT
    *
FROM {{ source('bronze', 'bronze_sensors') }}