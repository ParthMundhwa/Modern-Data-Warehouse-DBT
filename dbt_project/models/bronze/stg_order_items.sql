{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist', 'raw_order_items') }}
),

renamed AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
        CAST(price AS FLOAT64) AS price,
        CAST(freight_value AS FLOAT64) AS freight_value
    FROM source
)

SELECT * FROM renamed
