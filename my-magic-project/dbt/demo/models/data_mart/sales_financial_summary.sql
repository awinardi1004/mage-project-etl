{{ config(
    materialized='incremental',
    unique_key='order_id'
)}}

select 
    sales_date,
    order_id,
    total_amount,
    total_cogs,
    total_discount_applied as disc_applied,
    net_profit,
    created_at,
    updated_at
from
    {{ ref('fact_sales')}}
where 1=1
    {{ incremental_filter('created_at')}}