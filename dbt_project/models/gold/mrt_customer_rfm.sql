{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
    WHERE order_status = 'delivered'
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

-- Reference point for Recency calculation (max date in dataset)
reference_date AS (
    SELECT MAX(purchased_at) AS max_date 
    FROM orders
),

customer_metrics AS (
    SELECT 
        o.customer_id, -- Note: customer_id is order-specific in Olist, customer_unique_id is the actual person
        c.customer_unique_id,
        MAX(o.purchased_at) AS last_purchase_date,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(o.total_payment_value) AS monetary_value
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY 1, 2
),

-- Because customer_id in Olist is generated PER ORDER, we need to roll up to customer_unique_id
rfm_base AS (
    SELECT
        customer_unique_id,
        MAX(last_purchase_date) AS last_purchase_date,
        SUM(frequency) AS total_orders,
        SUM(monetary_value) AS total_spent
    FROM customer_metrics
    GROUP BY 1
),

rfm_calculated AS (
    SELECT 
        b.customer_unique_id,
        b.last_purchase_date,
        TIMESTAMP_DIFF((SELECT max_date FROM reference_date), b.last_purchase_date, DAY) AS recency_days,
        b.total_orders AS frequency,
        b.total_spent AS monetary
    FROM rfm_base b
),

-- Assigning deciles/quartiles for RFM score (using simplified hardcoded logic for demonstration, alternatively use dbt-utils/ntile)
final AS (
    SELECT 
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 90 THEN 4
            WHEN recency_days <= 180 THEN 3
            WHEN recency_days <= 365 THEN 2
            ELSE 1 
        END AS r_score,
        CASE 
            WHEN frequency >= 5 THEN 5
            WHEN frequency = 4 THEN 4
            WHEN frequency = 3 THEN 3
            WHEN frequency = 2 THEN 2
            ELSE 1 
        END AS f_score,
        CASE 
            WHEN monetary >= 500 THEN 5
            WHEN monetary >= 250 THEN 4
            WHEN monetary >= 100 THEN 3
            WHEN monetary >= 50 THEN 2
            ELSE 1 
        END AS m_score
    FROM rfm_calculated
)

SELECT 
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CAST(r_score AS STRING) || CAST(f_score AS STRING) || CAST(m_score AS STRING) AS rfm_segment
FROM final
