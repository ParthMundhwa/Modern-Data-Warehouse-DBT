{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist', 'raw_geolocation') }}
),

renamed AS (
    SELECT
        geolocation_zip_code_prefix AS zip_code_prefix,
        CAST(geolocation_lat AS FLOAT64) AS lat,
        CAST(geolocation_lng AS FLOAT64) AS lng,
        geolocation_city AS city,
        geolocation_state AS state
    FROM source
),

-- There are multiple lat/lng for the same zip code in the dataset. 
-- We'll deduplicate by taking the average coordinates for a given zip code.
deduplicated AS (
    SELECT
        zip_code_prefix,
        MAX(city) AS city,
        MAX(state) AS state,
        AVG(lat) AS lat,
        AVG(lng) AS lng
    FROM renamed
    GROUP BY zip_code_prefix
)

SELECT * FROM deduplicated
