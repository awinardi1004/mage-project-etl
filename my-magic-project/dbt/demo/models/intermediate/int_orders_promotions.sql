SELECT 
    o.order_id,  
    o.sales_date,
    p.promo_id, 
    COALESCE(AVG(p.disc_value), 0) AS disc_value, 
    COALESCE(AVG(p.max_disc), 0) AS max_disc
FROM 
    {{ source('staging', 'stg_orders') }} AS o
LEFT JOIN 
    {{ source('staging', 'stg_promotions') }} AS p
ON 
    o.sales_date = p.promo_act_date 
GROUP BY 
    o.order_id, o.sales_date, p.promo_id


