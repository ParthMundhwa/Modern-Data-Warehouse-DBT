{{ config(materialized='table') }}

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

geolocation AS (
    SELECT * FROM {{ ref('stg_geolocation') }}
),

final AS (
    SELECT
        s.seller_id,
        s.zip_code_prefix,
        s.city,
        s.state,
        g.lat,
        g.lng
    FROM sellers s
    LEFT JOIN geolocation g
        ON s.zip_code_prefix = g.zip_code_prefix
)

SELECT * FROM final
