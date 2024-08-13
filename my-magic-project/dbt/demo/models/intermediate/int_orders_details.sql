{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['order_id', 'menu_id']
) }}

with combined as (
    select orders.*, m.effective_date, m.menu_name as menu_name, m.price, m.cogs
    from {{ source('staging', 'stg_orders') }} as orders
    left join {{ source('staging', 'stg_menu') }} as m on orders.menu_id = m.menu_id
),

filtered as (
    select *
    from combined
    where sales_date >= effective_date
),

latest_prices as (
    select distinct on (order_id, menu_id) order_id, menu_id, menu_name, price, cogs
    from filtered
    order by order_id, menu_id, effective_date asc
)

select o.order_id, o.sales_date, o.menu_id, lp.menu_name, o.quantity, lp.price, lp.cogs, o.created_at, o.updated_at
from {{ source('staging', 'stg_orders') }} as o
left join latest_prices lp on o.order_id = lp.order_id and o.menu_id = lp.menu_id

where 1=1
{{ incremental_filter('created_at') }}
