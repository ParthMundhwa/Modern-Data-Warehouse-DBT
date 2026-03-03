{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
    WHERE order_status = 'delivered'
),

order_items AS (
    SELECT * FROM {{ ref('fct_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

final AS (
    SELECT 
        DATE_TRUNC(o.purchased_at, MONTH) AS sales_month,
        COALESCE(p.product_category_name_en, 'Unknown') AS product_category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(oi.order_item_id) AS total_items_sold,
        SUM(oi.price) AS total_revenue,
        SUM(oi.freight_value) AS total_freight_cost
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY 
        1, 2
)

SELECT * FROM final
ORDER BY sales_month DESC, total_revenue DESC
