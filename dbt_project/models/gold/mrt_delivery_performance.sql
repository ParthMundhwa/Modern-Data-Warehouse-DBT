{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
    WHERE order_status = 'delivered'
),

final AS (
    SELECT 
        DATE_TRUNC(purchased_at, MONTH) AS order_month,
        COUNT(order_id) AS total_delivered_orders,
        AVG(days_to_deliver) AS average_days_to_deliver,
        AVG(delay_days) AS average_estimated_delay_days,
        SUM(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END) AS late_deliveries,
        SUM(CASE WHEN delay_days <= 0 THEN 1 ELSE 0 END) AS on_time_deliveries,
        CAST(SUM(CASE WHEN delay_days <= 0 THEN 1 ELSE 0 END) AS FLOAT64) / COUNT(order_id) * 100 AS on_time_delivery_rate_percent
    FROM orders
    GROUP BY 1
)

SELECT * FROM final
ORDER BY order_month DESC
