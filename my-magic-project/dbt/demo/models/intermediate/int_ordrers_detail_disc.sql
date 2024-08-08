select 
    od.order_id, 
    od.sales_date, 
    od.menu_name, 
    od.quantity, 
    od.price,
    od.cogs as cogs_per_item,
    op.promo_id,
    op.disc_value,
    op.max_disc
from 
    {{ ref('int_orders_details')}} as od
left join 
    {{ ref('int_orders_promotions')}} as op
on 
    od.order_id=op.order_id