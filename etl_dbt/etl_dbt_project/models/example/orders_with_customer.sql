{{ config(materialized = 'table') }}

SELECT
    o.order_id,
    c.first_name,
    c.last_name,
    o.order_date,
    o.total_amount
FROM {{ source('YANSHEN_DIT', 'ORDERS') }} o
JOIN {{ source('YANSHEN_DIT', 'CUSTOMERS') }} c 
ON o.customer_id = c.customer_id
