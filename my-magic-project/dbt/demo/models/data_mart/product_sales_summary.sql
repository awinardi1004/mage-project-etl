{{ config(
    materialized='incremental',
    unique_key=['menu_name', 'order_id']
) }}

select 
    sales_date,
    order_id,
    menu_name,
    quantity,
    total_amount as revenue,
    total_cogs,
    created_at,
    updated_at
from
    {{ ref('fact_product_sales')}}
where 1=1
    {{ incremental_filter('created_at') }}

