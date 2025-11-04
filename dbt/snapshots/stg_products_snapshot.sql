{% snapshot stg_products_snapshot %}
{{  
    config(
        target_schema='main_silver',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'name',
            'category',
            'subcategory',
            'current_price',
            'currency',
            'is_discontinued'
        ]
    )
}}

SELECT 
    product_id,
    sku,
    name,
    category,
    subcategory,
    current_price,
    currency,
    is_discontinued,
    introduced_dt,
    discontinued_dt
FROM {{ ref('stg_products') }}

{% endsnapshot %}
