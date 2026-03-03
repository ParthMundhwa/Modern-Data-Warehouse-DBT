{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist', 'raw_order_payments') }}
),

renamed AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        CAST(payment_value AS FLOAT64) AS payment_value
    FROM source
)

SELECT * FROM renamed
