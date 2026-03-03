{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_payments AS (
    SELECT 
        order_id,
        SUM(payment_value) AS total_payment_value,
        COUNT(payment_sequential) AS total_installments,
        STRING_AGG(DISTINCT payment_type, ', ') AS payment_types
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

order_reviews AS (
    SELECT 
        order_id,
        AVG(review_score) AS average_review_score,
        COUNT(review_id) AS total_reviews
    FROM {{ ref('stg_order_reviews') }}
    GROUP BY order_id
),

final AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_carrier_at,
        o.delivered_customer_at,
        o.estimated_delivery_at,
        p.total_payment_value,
        p.total_installments,
        p.payment_types,
        r.average_review_score,
        r.total_reviews,
        TIMESTAMP_DIFF(o.delivered_customer_at, o.purchased_at, DAY) AS days_to_deliver,
        TIMESTAMP_DIFF(o.delivered_customer_at, o.estimated_delivery_at, DAY) AS delay_days
    FROM orders o
    LEFT JOIN order_payments p
        ON o.order_id = p.order_id
    LEFT JOIN order_reviews r
        ON o.order_id = r.order_id
)

SELECT * FROM final
