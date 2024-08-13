{{ config(
    materialized='incremental',
    unique_key=['menu_name', 'order_id']
) }}

with agg as (
    select
        sales_date,
        order_id,
        menu_name,
        quantity,
        SUM(price * quantity) AS total_amount,
        SUM(quantity * cogs_per_item) AS total_cogs,
        created_at,
        updated_at
    from 
        {{ ref('int_ordrers_detail_disc') }} AS odc
    group by
        sales_date,
        order_id,
        menu_name,
        quantity,
        created_at,
        updated_at
)

select *
from agg
where 1=1
{{ incremental_filter('created_at') }}
