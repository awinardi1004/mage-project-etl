WITH aggregated_data AS (
    SELECT 
        order_id, 
        sales_date, 
        SUM(quantity * price) AS total_amount,
        SUM(quantity * cogs) AS total_cogs,
        AVG(disc_value) AS avg_disc_value,
        AVG(max_disc) AS avg_max_disc       
    FROM dm_resto dr
    GROUP BY order_id, sales_date
),
discounted_data AS (
    SELECT
        order_id,
        sales_date,
        total_amount,
        total_cogs,
        avg_disc_value AS disc_value,
        avg_max_disc AS max_disc,
        LEAST(total_amount * avg_disc_value, avg_max_disc) AS total_discount_applied
    FROM aggregated_data
)
SELECT
    order_id,
    sales_date,
    total_amount,
    total_cogs,
    disc_value,
    max_disc,
    total_discount_applied,
    total_amount - total_discount_applied - total_cogs AS net_profit
FROM discounted_data
ORDER BY order_id ASC;
