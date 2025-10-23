{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='merge'
) }}

SELECT *
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
  WHERE order_ts > (SELECT MAX(order_ts) FROM {{ this }})
{% endif %}