{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with agg_data as (
    select
        order_id,
        sales_date,
        promo_id,
        sum(quantity * price) as total_amount,
        sum(quantity * cogs_per_item) as total_cogs,
        avg(disc_value) as avg_disc_value,
        sum(max_disc) as avg_max_disc,
        max(created_at) as created_at,  -- gunakan max untuk memilih nilai terbaru dari kolom waktu
        max(updated_at) as updated_at   -- gunakan max untuk memilih nilai terbaru dari kolom waktu
    from
        {{ ref('int_ordrers_detail_disc') }} as odc
    group by
        order_id, sales_date, promo_id     
),
disc_data as (
    select
        order_id,
        sales_date,
        total_amount,
        total_cogs,
        promo_id,
        avg_disc_value as disc_value,
        avg_max_disc as max_disc,
        LEAST(total_amount * avg_disc_value, avg_max_disc) as total_discount_applied,
        created_at,
        updated_at
    from agg_data
)

select 
    order_id,
    sales_date,
    total_amount,
    total_cogs,
    promo_id,
    disc_value,
    max_disc,
    total_discount_applied,
    total_amount - total_discount_applied - total_cogs AS net_profit,
    created_at,
    updated_at
from
    disc_data
where
    1=1
    {{ incremental_filter('created_at') }}
order by 
    order_id asc
