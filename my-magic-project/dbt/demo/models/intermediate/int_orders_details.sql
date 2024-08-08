WITH combined AS (
    SELECT orders.*, m.effective_date, m.menu_name AS menu_name, m.price, m.cogs
    FROM {{ source('staging', 'stg_orders') }} AS orders
    LEFT JOIN {{ source('staging', 'stg_menu') }} AS m ON orders.menu_id = m.menu_id
),

filtered AS (
    SELECT *
    FROM combined
    WHERE sales_date >= effective_date
),

latest_prices AS (
    SELECT DISTINCT ON (order_id, menu_id) order_id, menu_id, menu_name, price, cogs
    FROM filtered
    ORDER BY order_id, menu_id, effective_date DESC
)

SELECT o.order_id, o.sales_date, o.menu_id, lp.menu_name, o.quantity, lp.price, lp.cogs
FROM {{ source('staging', 'stg_orders') }} AS o
LEFT JOIN latest_prices lp ON o.order_id = lp.order_id AND o.menu_id = lp.menu_id
