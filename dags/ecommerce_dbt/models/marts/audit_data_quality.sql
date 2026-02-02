{{ config(materialized='table') }}

SELECT
    DATE(ingestion_timestamp) as ingestion_date,
    COUNT(*) as total_events,
    -- cuántos eventos pasaron las pruebas y cuántos no
    SUM(CASE WHEN is_valid_event = true THEN 1 ELSE 0 END) as valid_events,
    SUM(CASE WHEN is_valid_event = false THEN 1 ELSE 0 END) as invalid_events,
    -- calculamos el porcentaje de error
    ROUND(
        (SUM(CASE WHEN is_valid_event = false THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 
    2) as error_rate_percentage
FROM {{ ref('stg_events') }}
GROUP BY 1
ORDER BY 1 DESC