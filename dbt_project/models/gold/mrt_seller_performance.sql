{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('fct_order_items') }}
),

sellers AS (
    SELECT * FROM {{ ref('dim_sellers') }}
),

seller_metrics AS (
    SELECT 
        oi.seller_id,
        COUNT(DISTINCT o.order_id) AS total_orders_fulfilled,
        COUNT(DISTINCT o.customer_id) AS unique_customers_served,
        SUM(oi.price) AS total_revenue_generated,
        AVG(o.average_review_score) AS average_review_score,
        AVG(o.days_to_deliver) AS average_days_to_deliver
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY 1
),

final AS (
    SELECT 
        s.seller_id,
        s.city,
        s.state,
        m.total_orders_fulfilled,
        m.unique_customers_served,
        m.total_revenue_generated,
        ROUND(m.average_review_score, 2) AS average_review_score,
        ROUND(m.average_days_to_deliver, 2) AS average_days_to_deliver
    FROM sellers s
    LEFT JOIN seller_metrics m ON s.seller_id = m.seller_id
)

SELECT * FROM final
ORDER BY total_revenue_generated DESC
