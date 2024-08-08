select 
    sales_date,
    order_id,
    total_amount,
    total_cogs,
    total_discount_applied as disc_applied,
    net_profit
from
    {{ ref('fact_sales')}}