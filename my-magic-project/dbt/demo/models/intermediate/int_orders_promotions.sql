{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='order_id'
) }}

with combined as (
    select 
        o.order_id,  
        o.sales_date,
        p.promo_id, 
        COALESCE(AVG(p.disc_value), 0) as disc_value, 
        COALESCE(AVG(p.max_disc), 0) as max_disc,
        o.created_at,
        o.updated_at
    from 
        {{ source('staging', 'stg_orders') }} as o
    left join 
        {{ source('staging', 'stg_promotions') }} as p
    on 
        o.sales_date = p.promo_act_date
    group by 
        o.order_id, 
        o.sales_date, 
        p.promo_id,
        o.created_at,
        o.updated_at
)

select *
from combined
where 1=1
{{ incremental_filter('created_at') }}
