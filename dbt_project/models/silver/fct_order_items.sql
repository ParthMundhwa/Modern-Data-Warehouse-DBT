{{ config(materialized='table') }}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

final AS (
    SELECT 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        (price + freight_value) AS total_item_value
    FROM order_items
)

SELECT * FROM final
