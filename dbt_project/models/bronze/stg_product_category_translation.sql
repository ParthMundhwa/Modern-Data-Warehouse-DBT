{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist', 'raw_product_category_name_translation') }}
),

renamed AS (
    SELECT
        product_category_name AS product_category_name_pt,
        product_category_name_english AS product_category_name_en
    FROM source
)

SELECT * FROM renamed
