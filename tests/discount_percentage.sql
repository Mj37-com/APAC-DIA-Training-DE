SELECT *
FROM {{ ref('stg_orders_lines') }}
WHERE discount < 0 OR discount > 100
