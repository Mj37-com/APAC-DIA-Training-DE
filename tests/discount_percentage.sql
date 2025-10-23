SELECT *
FROM {{ ref('stg_orders') }}
WHERE discount < 0 OR discount > 100
