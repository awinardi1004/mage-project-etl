{{config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['order_id','menu_id']
)}}

with combined as (
    select 
    od.order_id, 
    od.sales_date, 
    od.menu_name, 
    od.menu_id,
    od.quantity, 
    od.price,
    od.cogs as cogs_per_item,
    op.promo_id,
    op.disc_value,
    op.max_disc,
    od.created_at,
    od.updated_at
from 
    {{ ref('int_orders_details')}} as od
left join 
    {{ ref('int_orders_promotions')}} as op
on 
    od.order_id=op.order_id
)

select * from combined
where 1=1
{{ incremental_filter('created_at') }}