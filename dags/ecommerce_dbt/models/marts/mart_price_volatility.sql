{{ config(materialized='table') }}

SELECT
    product_id,
    category,
    -- cuántas veces ha cambiado de precio este producto
    COUNT(*) as number_of_versions,
    
    -- rango de precios histórico
    MIN(current_price) as min_price,
    MAX(current_price) as max_price,
    
    -- qué tanto varía
    MAX(current_price) - MIN(current_price) as price_volatility,
    
    -- cuándo fue el último cambio
    MAX(dbt_valid_from) as last_change_at
    
FROM {{ ref('products_snapshot') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1 -- solo interesan los que han cambiado
ORDER BY price_volatility DESC