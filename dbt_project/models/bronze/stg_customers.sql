{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist', 'raw_customers') }}
),

renamed AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix AS zip_code_prefix,
        customer_city AS city,
        customer_state AS state
    FROM source
)

SELECT * FROM renamed
