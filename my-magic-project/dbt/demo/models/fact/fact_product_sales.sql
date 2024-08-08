SELECT
    sales_date,
    order_id,
    menu_name,
    quantity,
    SUM(price * quantity) AS total_amount,
    SUM(quantity * cogs_per_item) AS total_cogs
FROM 
    {{ ref('int_ordrers_detail_disc') }}
GROUP BY
    sales_date,
    order_id,
    menu_name,
    quantity
