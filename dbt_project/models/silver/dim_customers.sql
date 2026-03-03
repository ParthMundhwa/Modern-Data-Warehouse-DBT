{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

geolocation AS (
    SELECT * FROM {{ ref('stg_geolocation') }}
),

final AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        c.zip_code_prefix,
        c.city,
        c.state,
        g.lat,
        g.lng
    FROM customers c
    LEFT JOIN geolocation g
        ON c.zip_code_prefix = g.zip_code_prefix
)

SELECT * FROM final
