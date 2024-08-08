select 
    sales_date,
    order_id,
    menu_name,
    quantity,
    total_amount as revenue,
    total_cogs
from
    {{ ref('fact_product_sales')}}
